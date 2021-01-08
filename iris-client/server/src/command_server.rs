use dashmap::DashMap;

use futures::prelude::*;

use hello_world::{greeter_server::Greeter, *};

use prost::bytes::Bytes;
use proto::hello_world;
use proto::n2n;
use pyo3::exceptions;
use pyo3::prelude::*;
use pyo3::{
    types::{IntoPyDict, PyBytes, PyDict, PyList, PyTuple, PyType},
    AsPyPointer, PyNativeType, PyTypeInfo,
};
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::Ordering;
use std::{
    collections::HashMap,
    convert::TryInto,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{command::after::After, utils::*};
use crate::{distributed, mem::LazyPyObject};
use tokio::task;
use tonic::{Request, Response, Status};
use tracing::{debug, event, info, instrument, log::warn, span, Level};
use tracing_futures::*;
use uuid;
#[derive(Clone)]
pub struct IrisServer {
    pub modules: Arc<DashMap<String, PyObject>>,
    pub objects: crate::mem::Mem,
    pub nodes: Arc<DashMap<String, distributed::DistributedClient>>,
    pub nodes_addr: Arc<DashMap<SocketAddr, String>>,
    pub pickle: PyObject,
    pub current_node: Arc<String>,
    pub metrics: crate::metrics::ExecutionMeter,
    pub clock: quanta::Clock,
    pub last_exception: Arc<Mutex<Option<PyObject>>>,
}

impl IrisServer {
    fn import_modules(
        &self,
        py: Python<'_>,
        modules: Vec<String>,
        paths: Vec<String>,
    ) -> PyResult<()> {
        let sys = py.import("sys")?;
        let path = sys.get("path")?.cast_as::<PyList>()?;

        for p in paths {
            path.append(p.as_str())?;
        }
        sys.add("path", path).unwrap();
        let path = sys.get("path")?.cast_as::<PyList>()?;
        info!("{:?}", path.repr());

        let state = &self.modules;
        for module_name in modules {
            info!("import.. {}", module_name);
            let py = py.import(module_name.as_str())?;
            state.insert(module_name, Py::from(py));
        }

        Ok(())
    }

    // TODO: Optimize?
    #[instrument(skip(self, fetch_list))]
    async fn fetch_remote(&self, fetch_list: &Vec<NodeObjectRef>) -> HashMap<u64, u64> {
        // let span = span!(Level::TRACE, "fetch");
        // let _g = span.enter();
        if fetch_list.len() == 0 {
            return Default::default();
        }
        event!(Level::DEBUG, fetch_list=?fetch_list);
        let mut bytes_c = 0;
        let start = std::time::Instant::now();
        let mut nodes = Vec::with_capacity(fetch_list.len());
        for n in fetch_list {
            let node: &distributed::DistributedClient =
                &self.nodes.get(&n.location).expect(&n.location);
            let node = node.clone();
            nodes.push((node, n));
        }
        let tasks = nodes.iter_mut().map(|(node, o)| {
            let mut hasher = DefaultHasher::new();
            let id = uuid::Uuid::new_v4();
            id.hash(&mut hasher);
            let id = hasher.finish();
            let obj = o.clone();
            node.get_object(tonic::Request::new(n2n::NodeObjectRef {
                id: o.id,
                attr: o.attr.clone(),
                location: o.location.clone(),
            }))
            .map(move |x| (id, x.unwrap().into_inner(), obj))
        });
        let result: Vec<(u64, n2n::Value, NodeObjectRef)> = futures::future::join_all(tasks).await;
        let mut ret = HashMap::new();
        for (id, b, r) in result {
            self.objects
                .insert(Some(id), LazyPyObject::new_serialized(Bytes::from(b.data)));
            ret.insert(r.id, id);
        }

        let end = std::time::Instant::now();
        info!("{:?}, bytes {}", end - start, bytes_c);
        return ret;
    }
}

#[tonic::async_trait]
impl Greeter for IrisServer {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let _request = request.into_inner();
        unimplemented!()
    }

    async fn connect_nodes(
        &self,
        request: Request<ConnectRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        // let addr = request.remote_addr().unwrap();
        let request = request.into_inner();
        for node in request.nodes {
            let mut client = distributed::connect(format!("http://{}", node.address))
                .await
                .unwrap();
            let reply = client
                .hello(n2n::HelloRequest {
                    name: self.current_node.as_str().to_owned(),
                })
                .await;
            info!("connected to {}", node.name);
            self.nodes.insert(node.name.clone(), client);
        }
        return Ok(Response::new(HelloReply { message: "".into() }));
    }

    async fn init(&self, request: Request<InitRequest>) -> Result<Response<NodeObject>, Status> {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let request = request.into_inner();
        let modules: Vec<String> = request.modules;
        let paths: Vec<String> = request.paths;
        let pickle = self.pickle.clone();
        let pickle = pickle.to_object(py);
        if let Err(err) = self.import_modules(py, modules, paths) {
            warn!("{:?}", err);
            return Ok(Response::new(NodeObject {
                exception: dumps(&pickle, py, err).unwrap(),
                location: self.current_node.as_str().to_owned(),
                ..Default::default()
            }));
        }
        return Ok(Response::new(NodeObject {
            r#type: "init".to_owned(),
            location: self.current_node.as_str().to_owned(),
            ..Default::default()
        }));
    }

    async fn call(&self, request: Request<CallRequest>) -> Result<Response<NodeObject>, Status> {
        let request: CallRequest = request.into_inner();
        info!("receive call request {}", request.object_id);
        let result =
            crate::command::ControlCommandTask::<crate::command::call::CallCommand>::new(request)
                .run(self)
                .await;
        match result {
            Ok(r) => Ok(Response::new(r)),
            Err(error) => {
                warn!("{:#?}", error);
                Err(tonic::Status::internal(format!("{:#?}", error)))
            }
        }
    }

    async fn create_object(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        // let clock = quanta::Clock::new();
        let start = self.clock.start();
        let request = request.into_inner();
        let result = crate::command::ControlCommandTask::<
            crate::command::create_object::CreateObjectCommand,
        >::new(request)
        .run(self)
        .await;

        match result {
            Ok(r) => Ok(Response::new(r)),
            Err(error) => {
                warn!("{:#?}", error);
                Err(tonic::Status::internal(format!("{:#?}", error)))
            }
        }
    }

    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let mut request = request.into_inner();
        let result =
            crate::command::ControlCommandTask::<crate::command::apply::ApplyCommand>::new(request)
                .run(self)
                .await;
        match result {
            Ok(r) => Ok(Response::new(r)),
            Err(error) => {
                warn!("{:#?}", error);
                Err(tonic::Status::internal(format!("{:#?}", error)))
            }
        }
    }

    async fn send(&self, request: Request<SendRequest>) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let mut request = request.into_inner();
        let result =
            crate::command::ControlCommandTask::<crate::command::send::SendCommand>::new(request)
                .run(self)
                .await;
        match result {
            Ok(r) => Ok(Response::new(r)),
            Err(error) => {
                warn!("{:#?}", error);
                Err(tonic::Status::internal(format!("{:#?}", error)))
            }
        }
    }

    async fn get_remote_object(
        &self,
        request: Request<GetRemoteObjectRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let request = request.into_inner();
        let result = crate::command::get_remote_object::GetRemoteRequest {
            request,
            mem: &self.objects,
            nodes: &self.nodes,
            current_node: self.current_node.as_ref(),
            pickle: &self.pickle,
        }.run().await;
        match result {
            Ok(r) => Ok(Response::new(r)),
            Err(error) => {
                warn!("{:#?}", error);
                Err(tonic::Status::internal(format!("{:#?}", error)))
            }
        }
    }

    async fn get_attr(
        &self,
        request: Request<GetAttrRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let request = request.into_inner();
        let result =
            crate::command::ControlCommandTask::<crate::command::get_attr::GetAttrCommand>::new(
                request,
            )
            .run(self)
            .await;
        match result {
            Ok(r) => Ok(Response::new(r)),
            Err(error) => {
                warn!("{:#?}", error);
                Err(tonic::Status::internal(format!("{:#?}", error)))
            }
        }
    }

    async fn del_object(
        &self,
        request: Request<DelRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let id = request.object_id;
        info!("receive del request {}", id);
        let maps = &self.objects;
        match request.options {
            Some(option) => {
                After {
                    objects: &option.after,
                    mem: &self.objects,
                    nodes: &self.nodes,
                    current_node: &self.current_node
                }.wait().await;
            }
            None => {}
        }
        if let Some(out_refs) = maps.del(request.object_id).await {
            for c in out_refs {
                if let Some(client) = self.nodes.get(&c) {
                    let mut c = client.value().clone();
                    tokio::spawn(async move {
                        c.del_object(n2n::ObjectId { id }).await.unwrap();
                    });
                }
            }
        }

        return Ok(Response::new(HelloReply {
            message: "123".to_string()
        }));
    }

    async fn get_value(&self, request: Request<NodeObjectRef>) -> Result<Response<Value>, Status> {
        let request = request.into_inner();
        let maps = self.objects.clone();
        let (obj, s) = maps.get(request.id).await;
        let obj = obj.unwrap();
        let pickle = self.pickle.clone();
        let data = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                // let maps = &self.objects; //.lock().unwrap();
                let mut obj = obj.get(&pickle, py).unwrap();
                for attr in request.attr {
                    obj = obj.getattr(py, attr).unwrap();
                }

                dumps(&pickle, py, obj).unwrap()
            }),
        )
        .await
        .unwrap()
        .unwrap();
        s.send(());
        return Ok(Response::new(Value { data }));
    }

    async fn sync_object(
        &self,
        request: Request<SyncRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        todo!()
    }
}
