use std::{collections::{HashMap, hash_map::DefaultHasher}, hash::{Hash, Hasher}, sync::Arc};

use anyhow::anyhow;
use args::PrepareArgsResult;
use dashmap::DashMap;
use prost::bytes::Bytes;
use proto::n2n;
use pyo3::{Py, PyAny, PyObject, PyResult, Python, types::{PyModule, PyTuple}};
use tokio::task::JoinHandle;
use tonic::{Request,Response};
use tracing::{debug, info, span, warn};
use crate::{Opt, command_server::IrisServer, distributed, hello_world::{greeter_server::Greeter, *}, mem::LazyPyObject, utils::LocalObject};
use futures::FutureExt;

use self::args::PrepareArgs;

pub mod call;
pub mod create_object;
pub mod get_attr;
pub mod args;
pub mod apply;
pub enum CommandTarget {
    Object(u64),
    Module(String),
    None
}

pub trait ControlCommandRequest:Send + 'static {
    fn get_target_object(&self) -> CommandTarget;
    fn get_async(&self) -> bool;
    fn get_args(&self) -> Option<CallArgs>;
}

pub trait ControlCommand {
    type Request: ControlCommandRequest;
    const NAME:&'static str;

    fn new(request: Self::Request, args: Option<PrepareArgsResult>, id: u64, object: Option<PyObject>) -> Self;

    fn run(self, py: Python<'_>, pickle: &PyObject) -> crate::error::Result<PyObject>;
}

pub struct ControlCommandTask<T:ControlCommand> {
    id: u64,
    request: T::Request,
    go_async: bool
}

impl<T: ControlCommand + 'static> ControlCommandTask<T> {
    pub fn new(request: T::Request) -> Self {
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();

        let go_async = request.get_async();
        debug!("create task {} for cmd {}", id, T::NAME);
        ControlCommandTask {
            id,
            request,
            go_async
        }
    }

    pub async fn run(self, server: &IrisServer) -> crate::error::Result<NodeObject> 
    where <T as ControlCommand>::Request: Sync {
        if self.go_async {
            let span = span!(tracing::Level::INFO, "AsyncCommand", cmd=T::NAME, id=self.id);
            let _g = span.enter();
            self.run_async(&server.objects, server.nodes.clone(), server.pickle.clone(), &server.current_node, server.modules.clone()).await
        }
        else {
            let span = span!(tracing::Level::INFO, "SyncCommand", cmd=T::NAME, id=self.id);
            let _g = span.enter();
            self.run_sync(&server.objects, &server.nodes, server.pickle.clone(), &server.current_node, &server.modules).await
        }
    }

    pub async fn run_async(self, mem:&crate::mem::Mem, nodes:Arc<DashMap<String, distributed::DistributedClient>>, pickle: PyObject, current_node: &str, modules: Arc<DashMap<String, PyObject>>) -> crate::error::Result<NodeObject> 
    where <T as ControlCommand>::Request: Sync
    {
        let request = self.request;
        let mem = mem.clone();
        let moudles = modules.clone();
        let nodes = nodes.clone();
        let current_node = current_node.to_owned();
        let id = self.id;

        let ret = NodeObject {
            id: self.id,
            r#type: "unknown".to_owned(),
            location: current_node.to_owned(),
            r#async: true,
            ..Default::default()
        };

        let task:JoinHandle<crate::error::Result<()>> = tokio::spawn(async move {
            let result = if let Some(args) = request.get_args() {
                Some(PrepareArgs {
                    args,
                    mem: &mem,
                    nodes: nodes.as_ref()
                }.prepare().await?)
            } else {
                None
            };

            let o= match request.get_target_object() {
                CommandTarget::Object(id) => {
                    Some(mem.get(id).await.ok_or(anyhow::anyhow!(format!("command target object {} not found", id)))?)
                }
                CommandTarget::Module(m) => {
                    Some(LazyPyObject::new_object(modules.get(&m).ok_or(anyhow::anyhow!(format!("command target module {} not found", id)))?.value().clone()))
                }
                CommandTarget::None => {
                    None
                }
            };
    
            let request = request;
            
            let mem_c = mem.clone();
            let blocking_task:crate::error::Result<_> = tokio::task::spawn_blocking(move || {
                let gil = Python::acquire_gil();
                let py = gil.python();
                let o = match o.map(|x|x.get(&pickle, py)) {
                    Some(Ok(obj)) => {
                        Some(obj)
                    }
                    Some(Err(err)) => {
                        return Err(err.into());
                    }
                    None => {
                        None
                    }
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
            }).await?;
            blocking_task?;

            Ok(())
        });

        tokio::spawn(async move {
            match task.await {
                Ok(Ok(_)) => {}
                Ok(Err(err)) => {
                    warn!(target=id, "{:#?}", err);
                }
                Err(err) => {
                    warn!(target=id, "JoinError {:#?}", err);
                }
            }
        });

        Ok(ret)
    }
    
    pub async fn run_sync(self, mem:&crate::mem::Mem, nodes:&DashMap<String, distributed::DistributedClient>, pickle: PyObject, current_node: &str, modules: &Arc<DashMap<String, PyObject>>) -> crate::error::Result<NodeObject> {
        let result = if let Some(args) = self.request.get_args() {
            Some(PrepareArgs {
                args,
                mem,
                nodes
            }.prepare().await?)
        } else {
            None
        };

        let o= match self.request.get_target_object() {
            CommandTarget::Object(id) => {
                Some(mem.get(id).await.ok_or(anyhow::anyhow!(format!("command target object {} not found", id)))?)
            }
            CommandTarget::Module(m) => {
                Some(LazyPyObject::new_object(modules.get(&m).ok_or(anyhow::anyhow!(format!("command target module {} not found", m)))?.value().clone()))
            }
            CommandTarget::None => {
                None
            }
        };
        let request = self.request;
        let current_node = current_node.to_owned();
        let id = self.id;
        
        let mem_c = mem.clone();
        tokio::task::spawn_blocking(move || {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let o = match o.map(|x|x.get(&pickle, py)) {
                Some(Ok(obj)) => {
                    Some(obj)
                }
                Some(Err(err)) => {
                    return Err(err.into());
                }
                None => {
                    None
                }
            };

            let result = T::new(request, result, id, o).run(py, &pickle);
    
            match result {
                Ok(obj) => {
                    let mut ret = NodeObject {
                        id,
                        r#type: obj.as_ref(py).get_type().name().map_err(|e|anyhow!("pyo3 get type name fail: {:#?}", e))?.to_string(),
                        location: current_node.to_owned(),
                        r#async: false,
                        ..Default::default()
                    };

                    try_extract_native_value(obj.as_ref(py), &mut ret, &mem_c, &current_node).map_err(|e|anyhow!("try_extract_native_value should not failed:{:#?}",e))?;
                    mem_c.insert(Some(id), LazyPyObject::new_object(obj));

                    Ok(ret)
                }
                Err(crate::error::Error::UserPyError {
                    source,
                    backtrace
                }) => {
                    let err = crate::utils::dumps(&pickle, py, source).map_err(|e|anyhow!("dump failed:{:#?}",e))?;
                    Ok(NodeObject {
                        exception: err,
                        location: current_node.to_owned(),
                        ..Default::default()
                    })
                }
                Err(error) => {
                    Err(error)
                }
            }
        }).await?
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
                let id = maps.insert(None,LazyPyObject::new_object(obj));
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