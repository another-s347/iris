use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use super::{CommandTarget, after::After, args::PrepareArgsResult, try_extract_native_value};
use crate::utils::{dbg_py, loads};
use crate::{
    distributed,
    hello_world::{greeter_server::Greeter, *},
    mem::LazyPyObject,
    utils::LocalObject,
    Opt,
};
use anyhow::Context;
use dashmap::DashMap;
use proto::n2n;
use pyo3::{PyObject, PyResult, Python};
use tracing::info;

pub struct GetRemoteRequest<'a> {
    pub request: GetRemoteObjectRequest,
    pub mem: &'a crate::mem::Mem,
    pub nodes: &'a Arc<DashMap<String, distributed::DistributedClient>>,
    pub current_node: &'a str,
    pub pickle: &'a PyObject,
}

impl<'a> GetRemoteRequest<'a> {
    pub async fn run(self) -> crate::error::Result<NodeObject> {
        let mut hasher = DefaultHasher::new();
        let id = uuid::Uuid::new_v4();
        id.hash(&mut hasher);
        let id = hasher.finish();
        self.mem.reg(id);
        if self
            .request
            .options
            .as_ref()
            .map(|x| x.r#async)
            .unwrap_or(false)
        {
            let after: Option<Vec<NodeObjectRef>> = self.request.options.map(|x| x.after);
            let object = self.request.object.unwrap();
            let task = run_async(
                id,
                after,
                self.current_node.to_string(),
                self.mem.clone(),
                self.nodes.clone(),
                object,
                self.pickle.clone(),
            );
            tokio::spawn(async move {
                match task.await {
                    Ok(_) => {}
                    Err(err) => {
                        tracing::error!(target=id, "Get Remote Object error {:#?}", err);
                    }
                }
            });
            Ok(NodeObject {
                id: id,
                r#type: "unknown".to_owned(),
                location: self.current_node.to_owned(),
                r#async: true,
                ..Default::default()
            })
        } else {
            let r = self.run_sync(id).await?;
            Ok(r)
        }
    }

    pub async fn run_sync(&self, id: u64) -> crate::error::Result<NodeObject> {
        let target = self.request.object.as_ref().unwrap();
    
        let mut node = self.nodes.get(&target.location).unwrap().value().clone();
    
        let result = tokio::time::timeout(
            Duration::from_secs(20),
            node.get_object(tonic::Request::new(n2n::NodeObjectRef {
                id: target.id,
                attr: target.attr.clone(),
                location: target.location.clone(),
            })),
        )
        .await??
        .into_inner();
    
        let pickle = self.pickle.clone();
        let mem_c = self.mem.clone();
        let current_node = self.current_node.to_string();
        let result: crate::error::Result<NodeObject> = tokio::task::spawn_blocking(move|| {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let result = loads(&pickle, py, result.data.as_ref());

            let ret:crate::error::Result<_> = match result {
                Ok(obj) => {
                    let mut ret = NodeObject {
                        id,
                        r#type: obj
                            .as_ref(py)
                            .get_type()
                            .name()
                            .map_err(|e| anyhow::anyhow!("pyo3 get type name fail: {:#?}", e))?
                            .to_string(),
                        location: current_node.to_owned(),
                        r#async: false,
                        ..Default::default()
                    };

                    try_extract_native_value(obj.as_ref(py), &mut ret, &mem_c, &current_node)
                        .map_err(|e| {
                            anyhow::anyhow!("try_extract_native_value should not failed:{:#?}", e)
                        })?;
                    mem_c.insert(Some(id), LazyPyObject::new_object(obj));

                    Ok(ret)
                }
                Err(err) => {
                    let err = crate::utils::dumps(&pickle, py, err)
                        .map_err(|e| anyhow::anyhow!("dump failed:{:#?}", e))?;
                    Ok(NodeObject {
                        exception: err,
                        location: current_node.to_owned(),
                        ..Default::default()
                    })
                }
            };
    
            Ok(ret?)
        })
        .await?;
    
        Ok(result?)
    }
}

async fn run_async(
    id: u64,
    after: Option<Vec<NodeObjectRef>>,
    current_node: String,
    mem: crate::mem::Mem,
    nodes: Arc<DashMap<String, distributed::DistributedClient>>,
    target: NodeObjectRef,
    pickle: PyObject,
) -> crate::error::Result<()> {
    match after {
        Some(after) => {
            After {
                objects: &after,
                mem: &mem,
                nodes: &nodes,
                current_node: &current_node,
            }
            .wait()
            .await?;
        }
        None => {}
    }

    let mut node = nodes.get(&target.location).unwrap().value().clone();

    let result = tokio::time::timeout(
        Duration::from_secs(20),
        node.get_object(tonic::Request::new(n2n::NodeObjectRef {
            id: target.id,
            attr: target.attr.clone(),
            location: target.location.clone(),
        })),
    )
    .await??
    .into_inner();

    let result: crate::error::Result<PyObject> = tokio::task::spawn_blocking(move || {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let result = loads(&pickle, py, result.data.as_ref())?;

        Ok(result)
    })
    .await?;

    mem.insert(Some(id), LazyPyObject::new_object(result?));

    Ok(())
}