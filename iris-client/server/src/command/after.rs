use anyhow::anyhow;
use dashmap::DashMap;
use proto::{hello_world::NodeObjectRef, n2n};
use tracing::info;

use crate::distributed;

pub struct After<'a> {
    pub objects: &'a Vec<NodeObjectRef>,
    pub mem: &'a crate::mem::Mem,
    pub nodes: &'a DashMap<String, distributed::DistributedClient>,
    pub current_node: &'a str
}

impl<'a> After<'a> {
    pub async fn wait(self) -> crate::error::Result<()> {
        let mut local_tasks = Vec::new();
        let mut remote_tasks = Vec::new();

        info!("wait for object {:?}", self.objects);

        for o in self.objects {
            if o.location != self.current_node {
                // todo: Logically, the request should be passed to unconnected nodes.
                let client = self.nodes.get(&o.location).ok_or(anyhow!("node {} not connected", o.location))?;
                let client = client.value().clone();
                remote_tasks.push((client, o));
            }
            else {
                local_tasks.push(self.mem.get(o.id));
            }
        }

        let mut r = Vec::new();

        for (client, o) in remote_tasks.iter_mut() {
            r.push(client.wait_object(tonic::Request::new(n2n::NodeObjectRef {
                id: o.id,
                attr: o.attr.clone(),
                location: o.location.clone(),
            })))
        }

        futures::future::join_all(r).await;
        futures::future::join_all(local_tasks).await;

        Ok(())
    }
}