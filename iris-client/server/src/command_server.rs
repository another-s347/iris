use common::IrisObjectId;
use dashmap::DashMap;
use dict_derive::{FromPyObject, IntoPyObject};
use futures::prelude::*;
use futures::stream::TryStreamExt;
use hello_world::{
    greeter_server::{Greeter, GreeterServer},
    *,
};

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
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};

use crate::distributed;
use crate::utils::*;
use tokio::{net::UnixListener, task};
use tonic::{
    transport::{Server, Uri},
    Request, Response, Status,
};
use uuid;
#[derive(Debug, Clone)]
pub struct IrisServer {
    pub modules: Arc<DashMap<String, Py<PyModule>>>,
    pub objects: Arc<DashMap<u64, Py<PyAny>>>,
    pub nodes: Arc<DashMap<String, distributed::DistributedClient>>,
    pub pickle: Py<PyModule>,
    pub profile: Arc<dashmap::DashMap<&'static str, (usize, std::time::Duration)>>,
    pub current_node: Arc<String>,
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
        println!("{:?}", path.repr());

        let mut state = &self.modules;
        for module_name in modules {
            println!("import.. {}", module_name);
            let py = py.import(module_name.as_str())?;
            state.insert(module_name, Py::from(py));
        }

        Ok(())
    }

    // TODO: Optimize?
    async fn fetch_remote(&self, fetch_list: &Vec<NodeObjectRef>) -> HashMap<u64, u64> {
        if fetch_list.len() == 0 {
            return Default::default();
        }
        let mut bytes_c = 0;
        let start = std::time::Instant::now();
        let result = if fetch_list.len() == 1 {
            let mut o = fetch_list.first().unwrap();
            let node: &distributed::DistributedClient = &self.nodes.get(&o.location).unwrap();
            let recv_start = std::time::Instant::now();
            let mut node = node.clone();
            let data = node
                .get_object(tonic::Request::new(n2n::NodeObjectRef {
                    id: o.id,
                    attr: o.attr.clone(),
                    location: o.location.clone(),
                }))
                .await
                .unwrap()
                .into_inner();
            let mid = std::time::Instant::now();
            println!("recv {:?}", mid - recv_start);
            let o = o.clone();
            let map = self.objects.clone();
            let pickle = self.pickle.clone();
            bytes_c += data.data.len();
            tokio::task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                let mut hasher = DefaultHasher::new();
                let id = uuid::Uuid::new_v4();
                id.hash(&mut hasher);
                let id = hasher.finish();
                let obj = loads(&pickle, py, data.data.as_ref()).unwrap();
                // println!("fetch object id#{}, {:?}", id, obj.as_ref(py).repr());
                map.insert(id, Py::from(obj.as_ref(py)));
                let mut result = HashMap::new();
                result.insert(o.id, id);
                result
            })
            .await
            .unwrap()
        } else {
            let mut nodes = Vec::with_capacity(fetch_list.len());
            for n in fetch_list {
                let node: &distributed::DistributedClient =
                    &self.nodes.get(&n.location).expect(&n.location);
                let node = node.clone();
                nodes.push((node, n));
            }
            let tasks = nodes.iter_mut().map(|(node, o)| {
                let obj = o.clone();
                node.get_object(tonic::Request::new(n2n::NodeObjectRef {
                    id: o.id,
                    attr: o.attr.clone(),
                    location: o.location.clone(),
                }))
                .map(move |x| (x.unwrap().into_inner(), obj))
            });
            let result: Vec<(n2n::Value, NodeObjectRef)> = futures::future::join_all(tasks).await;
            let map = self.objects.clone();
            let pickle = self.pickle.clone();
            tokio::task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                let mut ret = HashMap::new();
                for (b, r) in result {
                    let obj = loads(&pickle, py, b.data.as_ref()).unwrap();
                    let mut hasher = DefaultHasher::new();
                    let id = uuid::Uuid::new_v4();
                    id.hash(&mut hasher);
                    let id = hasher.finish();
                    map.insert(id, Py::from(obj.as_ref(py)));
                    ret.insert(r.id, id);
                }
                ret
            })
            .await
            .unwrap()
        };

        let end = std::time::Instant::now();
        println!("{:?}, bytes {}", end - start, bytes_c);
        return result;
    }
}

fn _call(
    object_map: Arc<DashMap<u64, Py<PyAny>>>,
    py: Python<'_>,
    request: CallRequest,
    pickle: &PyObject,
    current_node: &str,
    fetch_list: &HashMap<u64, u64>,
) -> PyResult<NodeObject> {
    let mut maps = object_map;
    let (args, kwargs) = if let Some(arg) = request.arg {
        (
            map_args_to_local(&maps, py, arg.args, &pickle, fetch_list),
            map_kwargs_to_local(&maps, py, arg.kwargs, &pickle, fetch_list),
        )
    } else {
        (PyTuple::empty(py).to_object(py), None)
    };
    let o = maps.get(&request.object_id).unwrap().value().clone();
    let mut o = o.to_object(py);
    for attr in request.attr {
        o = dbg_py(py, o.getattr(py, &attr)).unwrap();
    }
    let ret = if let Some(k) = kwargs {
        o.call(py, args.cast_as(py)?, Some(k.cast_as(py)?))?
    } else {
        o.call(py, args.cast_as(py)?, None)?
    };

    let mut hasher = DefaultHasher::new();
    let id = uuid::Uuid::new_v4();
    id.hash(&mut hasher);
    let id = hasher.finish();

    maps.insert(id, Py::from(ret.as_ref(py)));

    let mut nodeobj = NodeObject {
        id,
        r#type: ret.as_ref(py).get_type().name().to_string(),
        location: current_node.to_owned(),
        ..Default::default()
    };

    try_extract_native_value(ret.as_ref(py), py, &mut nodeobj, &maps, current_node)?;

    return Ok(nodeobj);
}

fn try_extract_native_value(
    obj: &PyAny,
    py: Python<'_>,
    ret: &mut NodeObject,
    maps: &DashMap<u64, Py<PyAny>>,
    current_node: &str,
) -> PyResult<()> {
    if py.is_instance::<pyo3::types::PyBool, _>(obj)? {
        let value: bool = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::Boolean(value)),
        });
    } else if py.is_instance::<pyo3::types::PyInt, _>(obj)? {
        let value = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::I64(value)),
        });
    } else if py.is_instance::<pyo3::types::PyFloat, _>(obj)? {
        let value = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::F32(value)),
        });
    } else if py.is_instance::<pyo3::types::PyString, _>(obj)? {
        let value = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::Str(value)),
        })
    } else if py.is_instance::<pyo3::types::PyTuple, _>(obj)? {
        let value: &PyTuple = obj.cast_as()?;
        let mut vec = vec![];
        for a in value {
            if let Ok(x) = a.extract() {
                vec.push(proto_py_any::Data::Boolean(x));
            } else if let Ok(true) = py.is_instance::<pyo3::types::PyFloat, _>(a) {
                vec.push(proto_py_any::Data::F32(a.extract().unwrap()));
            } else if let Ok(true) = py.is_instance::<pyo3::types::PyInt, _>(a) {
                vec.push(proto_py_any::Data::I64(a.extract().unwrap()));
            } else if let Ok(x) = a.extract() {
                vec.push(proto_py_any::Data::Str(x));
            } else {
                let mut hasher = DefaultHasher::new();
                let id = uuid::Uuid::new_v4();
                id.hash(&mut hasher);
                let id = hasher.finish();
                let obj = Py::from(a);
                maps.insert(id, obj);
                vec.push(proto_py_any::Data::ObjectId(NodeObjectRef {
                    id,
                    location: current_node.to_owned(),
                    attr: Default::default(),
                }));
            }
        }
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::Tuple(ProtoPyTuple {
                items: vec
                    .drain(0..)
                    .map(|x| ProtoPyAny { data: Some(x) })
                    .collect(),
            })),
        })
    }
    Ok(())
}

fn create_object(
    object_map: Arc<DashMap<u64, Py<PyAny>>>,
    modules: Arc<DashMap<String, Py<PyModule>>>,
    py: Python<'_>,
    request: CreateRequest,
    pickle: &PyObject,
    current_node: &str,
    fetch_list: &HashMap<u64, u64>,
) -> PyResult<NodeObject> {
    let module = modules;
    if let Some(m) = module.get(&request.module) {
        let m = m.to_object(py);
        let m = m.getattr(py, request.qualname)?;
        let mut maps = object_map;
        let (args, kwargs) = if let Some(arg) = request.arg {
            let pickle = pickle.to_object(py);
            (
                map_args_to_local(&maps, py, arg.args, &pickle, fetch_list),
                map_kwargs_to_local(&maps, py, arg.kwargs, &pickle, fetch_list),
            )
        } else {
            (PyTuple::empty(py).to_object(py), None)
        };

        let ret = m.call(
            py,
            args.cast_as(py)?,
            kwargs.as_ref().map(|x| x.cast_as(py).unwrap()),
        )?;
        let ret = ret.as_ref(py);
        let new_object = Py::from(ret);
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        maps.insert(id, new_object);

        let mut nodeobj = NodeObject {
            id,
            r#type: ret.get_type().name().to_string(),
            location: current_node.to_owned(),
            ..Default::default()
        };

        try_extract_native_value(ret, py, &mut nodeobj, &maps, current_node)?;

        return Ok(nodeobj);
    } else {
        let err =
            PyErr::new::<exceptions::KeyError, _>(format!("Module {} not found.", request.module));
        return Err(err);
    }
}

fn apply(
    object_map: Arc<DashMap<u64, Py<PyAny>>>,
    py: Python<'_>,
    request: ApplyRequest,
    pickle: &PyObject,
    current_node: &str,
    fetch_list: &HashMap<u64, u64>,
) -> PyResult<NodeObject> {
    let mut maps = object_map;
    let (args, kwargs) = if let Some(arg) = request.arg {
        (
            map_args_to_local(&maps, py, arg.args, &pickle, fetch_list),
            map_kwargs_to_local(&maps, py, arg.kwargs, &pickle, fetch_list),
        )
    } else {
        (PyTuple::empty(py).to_object(py), None)
    };

    let func = loads(&pickle, py, request.func.as_ref()).unwrap();

    let func = func.as_ref(py);
    let ret = func
        .call(
            args.cast_as(py).unwrap(),
            kwargs.as_ref().map(|x| x.cast_as(py).unwrap()),
        )
        .unwrap();

    let mut hasher = DefaultHasher::new();
    let id = uuid::Uuid::new_v4();
    id.hash(&mut hasher);
    let id = hasher.finish();

    maps.insert(id, Py::from(ret));

    let mut nodeobj = NodeObject {
        id,
        r#type: ret.get_type().name().to_string(),
        location: current_node.to_owned(),
        ..Default::default()
    };

    try_extract_native_value(ret, py, &mut nodeobj, &maps, current_node)?;

    return Ok(nodeobj);
}

#[tonic::async_trait]
impl Greeter for IrisServer {
    async fn say_hello(
        &self,
        request: Request<HelloRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let request = request.into_inner();
        unimplemented!()
    }

    async fn connect_nodes(
        &self,
        request: Request<ConnectRequest>,
    ) -> Result<Response<HelloReply>, Status> {
        let request = request.into_inner();
        for node in request.nodes {
            let client = distributed::connect(node.address).await.unwrap();
            println!("connected to {}", node.name);
            self.nodes.insert(node.name, client);
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
        let start = std::time::Instant::now();
        let mut request: CallRequest = request.into_inner();
        let fetch_list = if let Some(arg) = &request.arg {
            if arg
                .fetch_lists
                .iter()
                .find(|x| x.id == request.object_id)
                .is_some()
            {
                unimplemented!()
            }
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.fetch_remote(&arg.fetch_lists),
            )
            .await
            .unwrap()
        } else {
            Default::default()
        };
        if let Some(id) = fetch_list.get(&request.object_id) {
            unimplemented!()
            // request.object_id = id;
            // request.attr.clear();
        }
        let object_map = self.objects.clone();
        let pickle = self.pickle.clone();
        let name = self.current_node.clone();
        let call_task = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                let result = _call(object_map, py, request, &pickle, name.as_str(), &fetch_list);
                map_result(&pickle, py, result, name.as_str())
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let end = std::time::Instant::now();
        let d: std::time::Duration = end - start;
        self.profile
            .update("call", move |key, (count, value)| (count + 1, *value + d));
        return Ok(Response::new(call_task));
    }

    async fn create_object(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let fetch_list = if let Some(arg) = &request.arg {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.fetch_remote(&arg.fetch_lists),
            )
            .await
            .unwrap()
        } else {
            Default::default()
        };
        let object_map = self.objects.clone();
        let pickle = self.pickle.clone();
        let modules = self.modules.clone();
        let name = self.current_node.clone();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                let result = create_object(
                    object_map,
                    modules,
                    py,
                    request,
                    &pickle,
                    name.as_str(),
                    &fetch_list,
                );
                map_result(&pickle, py, result, name.as_str())
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let end = std::time::Instant::now();
        let d: std::time::Duration = end - start;
        self.profile
            .update("create", move |key, (count, value)| (count + 1, *value + d));
        return Ok(Response::new(result));
    }

    async fn apply(&self, request: Request<ApplyRequest>) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let fetch_list = if let Some(arg) = &request.arg {
            tokio::time::timeout(
                std::time::Duration::from_secs(2),
                self.fetch_remote(&arg.fetch_lists),
            )
            .await
            .unwrap()
        } else {
            Default::default()
        };
        let object_map = self.objects.clone();
        let pickle = self.pickle.clone();
        let modules = self.modules.clone();
        let name = self.current_node.clone();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                let result = apply(object_map, py, request, &pickle, name.as_str(), &fetch_list);
                map_result(&pickle, py, result, name.as_str())
            }),
        )
        .await
        .unwrap()
        .unwrap();
        let end = std::time::Instant::now();
        let d: std::time::Duration = end - start;
        self.profile
            .update("apply", move |key, (count, value)| (count + 1, *value + d));
        return Ok(Response::new(result));
    }

    async fn get_remote_object(
        &self,
        request: Request<NodeObjectRef>,
    ) -> Result<Response<NodeObject>, Status> {
        let request = request.into_inner();
        let id = request.id;
        let request = vec![request];
        let result: HashMap<u64, u64> = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.fetch_remote(&request),
        )
        .await
        .unwrap();
        let id = *result.get(&id).unwrap();
        return Ok(Response::new(NodeObject {
            id,
            location: self.current_node.as_str().to_string(),
            ..Default::default()
        }));
    }

    async fn get_attr(
        &self,
        request: Request<GetAttrRequest>,
    ) -> Result<Response<NodeObject>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let maps = self.objects.clone();
        let id = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();

                let mut hasher = DefaultHasher::new();
                let id = uuid::Uuid::new_v4();
                id.hash(&mut hasher);
                let id = hasher.finish();

                // let mut maps = &self.objects; //.lock().unwrap();
                let obj = { maps.get(&request.object_id).unwrap().value().clone() };
                let obj = obj.to_object(py);
                let mut obj = obj.as_ref(py);
                for attr in request.attr {
                    obj = dbg_py(py, obj.getattr(attr.as_str())).expect(attr.as_str());
                }
                maps.insert(id, Py::from(obj));
                id
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let end = std::time::Instant::now();
        let d: std::time::Duration = end - start;
        self.profile.update("getattr", move |key, (count, value)| {
            (count + 1, *value + d)
        });
        return Ok(Response::new(NodeObject {
            id,
            location: self.current_node.as_str().to_owned(),
            ..Default::default()
        }));
    }

    async fn del_object(
        &self,
        request: Request<NodeObjectRef>,
    ) -> Result<Response<NodeObjectRef>, Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();

        let mut maps = &self.objects;
        maps.remove(&request.id);

        let end = std::time::Instant::now();
        let d: std::time::Duration = end - start;
        self.profile
            .update("del", move |key, (count, value)| (count + 1, *value + d));
        return Ok(Response::new(request));
    }

    async fn get_value(&self, request: Request<NodeObjectRef>) -> Result<Response<Value>, Status> {
        let request = request.into_inner();
        let maps = self.objects.clone();
        let pickle = self.pickle.clone();
        let data = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            tokio::task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let pickle = pickle.to_object(py);
                // let maps = &self.objects; //.lock().unwrap();
                let mut obj = { maps.get(&request.id).unwrap().value().clone() }.to_object(py);
                for attr in request.attr {
                    obj = obj.getattr(py, attr).unwrap();
                }

                dumps(&pickle, py, obj).unwrap()
            }),
        )
        .await
        .unwrap()
        .unwrap();
        return Ok(Response::new(Value { data }));
    }
}
