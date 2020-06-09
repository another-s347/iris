use dashmap::DashMap;
use n2n::{
    n2n_client::N2nClient,
    n2n_server::{N2n},
};
use proto::n2n;
use pyo3::prelude::*;
use pyo3::{prelude::PyModule, Py, PyAny, Python};
use std::{sync::Arc, net::SocketAddr};
use tonic::{Request, Response};

pub type DistributedClient = N2nClient<tonic::transport::channel::Channel>;

pub async fn connect(address: String) -> Result<DistributedClient, tonic::transport::Error> {
    let client = DistributedClient::connect(address).await?;
    Ok(client)
}

pub struct NodeServer {
    pub objects: crate::mem::Mem,
    pub pickle: Py<PyModule>,
    pub current_node: String,
    pub node_addr: Arc<DashMap<SocketAddr, String>>
    // pub nodes: Arc<DashMap<String, DistributedClient>>
}

#[tonic::async_trait]
impl N2n for NodeServer {
    async fn hello(&self, request: Request<n2n::HelloRequest>) -> Result<Response<n2n::HelloReply>, tonic::Status> {
        let addr = request.remote_addr().unwrap();
        let request = request.into_inner();
        self.node_addr.insert(addr, request.name);
        return Ok(Response::new(n2n::HelloReply {
            message: "Ok".to_owned()
        }));
    }

    async fn del_object(
        &self,
        request: Request<n2n::ObjectId>
    ) -> Result<Response<n2n::ObjectId>, tonic::Status> {
        let request = request.into_inner();
        self.objects.del_remote(request.id);
        return Ok(Response::new(n2n::ObjectId {
            id: request.id
        }))
    }

    async fn get_object(
        &self,
        request: Request<n2n::NodeObjectRef>,
    ) -> Result<Response<n2n::Value>, tonic::Status> {
        let start = std::time::Instant::now();
        let addr = request.remote_addr().unwrap();
        let request = request.into_inner();
        if request.location != self.current_node {
            unimplemented!()
        }// let peer_addr = request.remote_addr().unwrap();
        // // let node_id = 
        let pickle = self.pickle.clone();
        let objects = self.objects.clone();
        println!("{:?}, {:?}", self.node_addr, addr);
        let node = self.node_addr.get(&addr).unwrap().value().clone();
        let object = tokio::task::spawn_blocking(move || {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let pickle = pickle.to_object(py);
            objects.insert_out_ref(request.id, node);
            let object = objects.get(&request.id).unwrap();
            let mut object = object.to_object(py);
            for attr in request.attr {
                object = object.getattr(py, attr).unwrap();
            }
            crate::utils::dbg_py(py,crate::utils::dumps(&pickle, py, object)).unwrap()
        })
        .await.unwrap();
        let end = std::time::Instant::now();
        println!("get object {:?}, bytes: {}", end-start, object.len());
        return Ok(Response::new(n2n::Value { data: object }));
    }
}
