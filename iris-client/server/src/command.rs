use std::{collections::{hash_map::DefaultHasher, HashMap}, hash::{Hash, Hasher}, sync::Arc, fmt::Debug, time::Duration};

use crate::{Opt, command_server::IrisServer, metrics::SingleCommand, distributed, hello_world::{greeter_server::Greeter, *}, mem::LazyPyObject, utils::LocalObject};
use after::After;
use anyhow::{Context, anyhow};
use args::PrepareArgsResult;
use dashmap::DashMap;
use futures::FutureExt;
use prost::bytes::Bytes;
use proto::n2n;
use pyo3::{
    types::{PyModule, PyTuple},
    Py, PyAny, PyObject, PyResult, Python,
};
use tokio::{task::JoinHandle};
use tonic::{Request, Response};
use tracing::{debug, info, span, warn, Instrument};

use self::args::PrepareArgs;

pub mod after;
pub mod apply;
pub mod args;
pub mod call;
pub mod create_object;
pub mod get_attr;
pub mod get_remote_object;
pub mod send;

pub enum CommandTarget {
    Object(u64),
    Module(String),
    None,
}

pub trait ControlCommandRequest: Send + 'static + Debug {
    fn get_target_object(&self) -> CommandTarget;
    fn get_option(&self) -> Option<&RequestOption>;
    fn get_args(&self) -> Option<CallArgs>;
}

pub trait RequestExt {
    fn get_async(&self) -> bool;
}

impl<T> RequestExt for T
where
    T: ControlCommandRequest,
{
    fn get_async(&self) -> bool {
        self.get_option().map(|x| x.r#async).unwrap_or(false)
    }
}

pub trait ControlCommand {
    type Request: ControlCommandRequest;
    const NAME: &'static str;

    fn new(
        request: Self::Request,
        args: Option<PrepareArgsResult>,
        id: u64,
        object: Option<PyObject>,
    ) -> Self;

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject>;
}

pub struct ControlCommandTask<T: ControlCommand> {
    id: u64,
    request: T::Request,
    go_async: bool,
}

impl<T: ControlCommand + 'static> ControlCommandTask<T> {
    pub fn new(request: T::Request) -> Self {
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        let go_async = request.get_async();
        ControlCommandTask {
            id,
            request,
            go_async,
        }
    }

    pub async fn run(self, server: &IrisServer) -> crate::error::Result<NodeObject>
    where
        <T as ControlCommand>::Request: Sync,
    {
        let id = self.id;
        server.objects.reg(id);
        if self.go_async {
            self.run_async(
                &server.objects,
                server.nodes.clone(),
                server.pickle.clone(),
                &server.current_node,
                server.modules.clone(),
                &server.metrics
            )
            .await
        } else {
            self.run_sync(
                &server.objects,
                &server.nodes,
                server.pickle.clone(),
                &server.current_node,
                &server.modules,
                &server.metrics
            )
            .instrument(span!(
                tracing::Level::INFO,
                "SyncCommand",
                cmd = T::NAME,
                id = id
            ))
            .await
        }
    }

    pub async fn run_async(
        self,
        mem: &crate::mem::Mem,
        nodes: Arc<DashMap<String, distributed::DistributedClient>>,
        pickle: PyObject,
        current_node: &str,
        modules: Arc<DashMap<String, PyObject>>,
        metrics: &crate::metrics::ExecutionMeter
    ) -> crate::error::Result<NodeObject>
    where
        <T as ControlCommand>::Request: Sync,
    {
        let request = self.request;
        let mem = mem.clone();
        // let moudles = modules.clone();
        let nodes = nodes.clone();
        let current_node = current_node.to_owned();
        let metrics = metrics.clone();
        let id = self.id;

        let ret = NodeObject {
            id: self.id,
            r#type: "unknown".to_owned(),
            location: current_node.to_owned(),
            r#async: true,
            ..Default::default()
        };

        let task: JoinHandle<crate::error::Result<Vec<_>>> = tokio::spawn(async move {
            let mut record = SingleCommand {
                cmd: T::NAME,
                duration_all: None,
                duration_execution: None,
                duration_get_target_object: None,
                duration_after: None,
                duration_prepare: None
            };
            let start = std::time::Instant::now();
            let mut current = start.clone();
            // mem.get have to run not after 'after list' or fetch to avoid they took long time and the object got deleted.
            let mut maybe_s = None;
            let o = match request.get_target_object() {
                CommandTarget::Object(tid) => {
                    let (w, s) = tokio::time::timeout(Duration::from_secs(30), mem.get(tid)).await.with_context(||format!("get target object {}", tid))?;
                    let end_of_get_target_object = std::time::Instant::now();
                    record.duration_get_target_object = Some(end_of_get_target_object - current);
                    current = end_of_get_target_object;
                    maybe_s = Some(s);
                    Some(w.ok_or(anyhow::anyhow!(format!(
                        "command target object {} not found",
                        tid
                    )))?)
                }
                CommandTarget::Module(m) => Some(LazyPyObject::new_object(
                    modules
                        .get(&m)
                        .ok_or(anyhow::anyhow!(format!(
                            "command target module {} not found",
                            m
                        )))?
                        .value()
                        .clone(),
                )),
                CommandTarget::None => None,
            };

            if let Some(after_list) = request.get_option().as_ref().map(|x|&x.after) {
                After {
                    objects: after_list,
                    mem: &mem,
                    nodes: nodes.as_ref(),
                    current_node: current_node.as_ref(),
                }
                .wait().await?;
                let end_of_after = std::time::Instant::now();
                record.duration_after = Some(end_of_after - current);
                current = end_of_after;
            }

            let mut result = if let Some(args) = request.get_args() {
                let r = PrepareArgs {
                    args,
                    mem: &mem,
                    nodes: nodes.as_ref(),
                }
                .prepare().await?;
                let end_of_prepareargs = std::time::Instant::now();
                record.duration_prepare = Some(end_of_prepareargs - current);
                current = end_of_prepareargs;
                Some(r)
            } else {
                None
            };

            let mut guards = if let Some(x) = result.as_mut() {
                std::mem::take(&mut x.guards)
            } else {
                vec![]
            };

            if let Some(s) = maybe_s {
                guards.push(s);
            }

            let request = request;

            let mem_c = mem.clone();
            let blocking_task: crate::error::Result<_> = tokio::task::spawn_blocking(move || {
                // std::thread::spawn(move||{
                    Python::with_gil(|py|{
                        let o = match o.map(|x| x.get(&pickle, py)) {
                            Some(Ok(obj)) => Some(obj),
                            Some(Err(err)) => {
                                return Err(err.into());
                            }
                            None => None,
                        };
        
                        let result = T::new(request, result, id, o).run(py, &pickle);
        
                        match result {
                            Ok(obj) => {
                                mem_c.insert(Some(id), LazyPyObject::new_object(obj));
                            }
                            Err(error) => {
                                return Err(error);
                            }
                        };
    
                        Ok(())
                    })
                // }).join().unwrap()
                // let gil = Python::acquire_gil();
                // let py = gil.python();
                

                // Ok(())
            })
            .await?;
            let end_of_execution = std::time::Instant::now();
            record.duration_execution = Some(end_of_execution - current);
            record.duration_all = Some(end_of_execution - start);
            metrics.set_record(record);
            blocking_task?;

            Ok(guards)
        });

        tokio::spawn(async move {
            match task
                .instrument(span!(
                    tracing::Level::INFO,
                    "AsyncCommand",
                    cmd = T::NAME,
                    id = id
                ))
                .await
            {
                Ok(Ok(x)) => {
                    for mut s in x {
                        s.send(());
                    }
                }
                Ok(Err(err)) => match err {
                    crate::error::Error::UserPyError { source, backtrace } => {
                        Python::with_gil(|py|{
                            let pytraceback = source.ptraceback(py).and_then(|x|x.repr().ok());
                            warn!(target=id, "{:#?} {:#?} {:#?}", source, pytraceback, backtrace);
                        });
                    }
                    _ => {
                        warn!(target = id, "{:#?}", err);
                    }
                },
                Err(err) => {
                    warn!(target = id, "JoinError {:#?}", err);
                }
            };
        });

        Ok(ret)
    }

    pub async fn run_sync(
        self,
        mem: &crate::mem::Mem,
        nodes: &DashMap<String, distributed::DistributedClient>,
        pickle: PyObject,
        current_node: &str,
        modules: &Arc<DashMap<String, PyObject>>,
        metrics: &crate::metrics::ExecutionMeter
    ) -> crate::error::Result<NodeObject> {
        let mut record = SingleCommand {
            cmd: T::NAME,
            duration_all: None,
            duration_execution: None,
            duration_get_target_object: None,
            duration_after: None,
            duration_prepare: None
        };
        let start = std::time::Instant::now();
        let mut current = start.clone();

        match self.request.get_option().map(|x| &x.after) {
            Some(after) => {
                After {
                    objects: after,
                    mem: &mem,
                    nodes: &nodes,
                    current_node: current_node.as_ref(),
                }
                .wait().await?;
                let end_of_after = std::time::Instant::now();
                record.duration_after = Some(end_of_after - current);
                current = end_of_after;
            }
            _ => {}
        };

        let mut result = if let Some(args) = self.request.get_args() {
            let r = PrepareArgs { args, mem, nodes }.prepare().await?;
            let end_of_prepare = std::time::Instant::now();
            record.duration_prepare = Some(end_of_prepare - current);
            current = end_of_prepare;
            Some(r)
        } else {
            None
        };

        let mut guards = if let Some(x) = result.as_mut() {
            std::mem::take(&mut x.guards)
        } else {
            vec![]
        };

        let o = match self.request.get_target_object() {
            CommandTarget::Object(id) => {
                let (obj, s) = mem.get(id).await;
                let end_of_get_target_object = std::time::Instant::now();
                record.duration_get_target_object = Some(end_of_get_target_object - current);
                current = end_of_get_target_object;
                guards.push(s);
                Some(obj.ok_or(anyhow::anyhow!(format!(
                    "command target object {} not found",
                    id
                )))?)
            }
            CommandTarget::Module(m) => Some(LazyPyObject::new_object(
                modules
                    .get(&m)
                    .ok_or(anyhow::anyhow!(format!(
                        "command target module {} not found",
                        m
                    )))?
                    .value()
                    .clone(),
            )),
            CommandTarget::None => None,
        };
        let request = self.request;
        let current_node = current_node.to_owned();
        let id = self.id;

        let mem_c = mem.clone();
        let r = tokio::task::spawn_blocking(move || {
            // std::thread::spawn(move||{
                Python::with_gil(|py|{
                    let o = match o.map(|x| x.get(&pickle, py)) {
                        Some(Ok(obj)) => Some(obj),
                        Some(Err(err)) => {
                            return Err(err.into());
                        }
                        None => None,
                    };
        
                    let result = T::new(request, result, id, o).run(py, &pickle);
        
                    match result {
                        Ok(obj) => {
                            let mut ret = NodeObject {
                                id,
                                r#type: obj
                                    .as_ref(py)
                                    .get_type()
                                    .name()
                                    .map_err(|e| anyhow!("pyo3 get type name fail: {:#?}", e))?
                                    .to_string(),
                                location: current_node.to_owned(),
                                r#async: false,
                                ..Default::default()
                            };
        
                            try_extract_native_value(obj.as_ref(py), &mut ret, &mem_c, &current_node)
                                .map_err(|e| {
                                    anyhow!("try_extract_native_value should not failed:{:#?}", e)
                                })?;
                            mem_c.insert(Some(id), LazyPyObject::new_object(obj));
        
                            Ok(ret)
                        }
                        Err(crate::error::Error::UserPyError { source, backtrace }) => {
                            let err = crate::utils::dumps(&pickle, py, source)
                                .map_err(|e| anyhow!("dump failed:{:#?}", e))?;
                            Ok(NodeObject {
                                exception: err.into(),
                                location: current_node.to_owned(),
                                ..Default::default()
                            })
                        }
                        Err(error) => Err(error),
                    }
                })
            // }).join().unwrap()
            // let gil = Python::acquire_gil();
            // let py = gil.python();
            
        })
        .await?;

        let end_of_execution = std::time::Instant::now();
        record.duration_execution = Some(end_of_execution - current);
        record.duration_all = Some(end_of_execution - start);
        metrics.set_record(record);

        for mut s in guards {
            s.send(());
        }

        r
    }
}

fn try_extract_native_value(
    obj: &PyAny,
    ret: &mut NodeObject,
    maps: &crate::mem::Mem,
    current_node: &str,
) -> PyResult<()> {
    if obj.is_instance::<pyo3::types::PyBool>()? {
        let value: bool = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::Boolean(value)),
        });
    } else if obj.is_instance::<pyo3::types::PyInt>()? {
        let value = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::I64(value)),
        });
    } else if obj.is_instance::<pyo3::types::PyFloat>()? {
        let value = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::F32(value)),
        });
    } else if obj.is_instance::<pyo3::types::PyString>()? {
        let value = obj.extract()?;
        ret.value = Some(ProtoPyAny {
            data: Some(proto_py_any::Data::Str(value)),
        })
    } else if obj.is_instance::<pyo3::types::PyTuple>()? {
        let value: &PyTuple = obj.cast_as()?;
        let mut vec = vec![];
        for a in value {
            if let Ok(x) = a.extract() {
                vec.push(proto_py_any::Data::Boolean(x));
            } else if let Ok(true) = obj.is_instance::<pyo3::types::PyFloat>() {
                vec.push(proto_py_any::Data::F32(a.extract()?));
            } else if let Ok(true) = obj.is_instance::<pyo3::types::PyInt>() {
                vec.push(proto_py_any::Data::I64(a.extract()?));
            } else if let Ok(x) = a.extract() {
                vec.push(proto_py_any::Data::Str(x));
            } else {
                let obj = a.into();
                let id = maps.insert(None, LazyPyObject::new_object(obj));
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
