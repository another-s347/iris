use std::time::Duration;

use anyhow::{anyhow, Context};
use dashmap::DashMap;
use futures::FutureExt;
use proto::{hello_world::NodeObjectRef, n2n};
use tracing::info;

use crate::distributed;

pub struct After<'a> {
    pub objects: &'a Vec<NodeObjectRef>,
    pub mem: &'a crate::mem::Mem,
    pub nodes: &'a DashMap<String, distributed::DistributedClient>,
    pub current_node: &'a str,
}

impl<'a> After<'a> {
    pub async fn wait(self) -> crate::error::Result<()> {
        let mut local_tasks = Vec::new();
        let mut remote_tasks = Vec::new();

        for o in self.objects {
            if o.location != self.current_node {
                // todo: Logically, the request should be passed to unconnected nodes.
                let client = self
                    .nodes
                    .get(&o.location)
                    .ok_or(anyhow!("after {:#?} at {}:node {} not connected", o, self.current_node, o.location))?;
                let client = client.value().clone();
                remote_tasks.push((client, o));
            } else {
                let id = o.id;
                local_tasks.push(
                    tokio::time::timeout(Duration::from_secs(10), self.mem.after(o.id)).map(
                        move |x| {
                            x.map_err(move |x| {
                                anyhow!("timeout when waiting after local task {}", id)
                            })
                        },
                    ),
                );
            }
        }

        let mut r = Vec::new();

        for (client, o) in remote_tasks.iter_mut() {
            let o = o.clone();
            r.push(
                tokio::time::timeout(
                    Duration::from_secs(10),
                    client.wait_object(tonic::Request::new(n2n::NodeObjectRef {
                        id: o.id,
                        attr: o.attr.clone(),
                        location: o.location.clone(),
                    })),
                )
                .map(move |x| {
                    x.map_err(move |_| anyhow!("timeout when waiting after remote task {:#?}", o))
                }),
            )
        }

        for x in futures::future::join_all(r).await {
            x??;
        }
        for x in futures::future::join_all(local_tasks).await {
            x?;
        }

        Ok(())
    }
}
