use dashmap::DashMap;
use n2n::{
    n2n_client::N2nClient,
    n2n_server::{N2n, N2nServer},
};
use proto::n2n;
use pyo3::prelude::*;
use pyo3::{prelude::PyModule, Py, PyAny, Python};
use std::{convert::TryFrom, sync::Arc};
use tonic::{transport::Endpoint, Request, Response};

pub type DistributedClient = N2nClient<tonic::transport::channel::Channel>;

pub async fn connect(address: String) -> Result<DistributedClient, tonic::transport::Error> {
    let client = DistributedClient::connect(address).await?;
    Ok(client)
}

pub struct NodeServer {
    pub objects: Arc<DashMap<u64, Py<PyAny>>>,
    pub pickle: Py<PyModule>,
}

#[tonic::async_trait]
impl N2n for NodeServer {
    async fn get_object(
        &self,
        request: Request<n2n::ObjectId>,
    ) -> Result<Response<n2n::Value>, tonic::Status> {
        let start = std::time::Instant::now();
        let request = request.into_inner();
        let pickle = self.pickle.clone();
        let objects = self.objects.clone();
        let object = tokio::task::spawn_blocking(move || {
            let gil = Python::acquire_gil();
            let py = gil.python();
            let pickle = pickle.to_object(py);
            let object = objects.get(&request.id).unwrap();
            let object: &Py<PyAny> = &object;
            crate::utils::dumps(&pickle, py, object).unwrap()
        })
        .await.unwrap();
        let end = std::time::Instant::now();
        println!("get object {:?}, bytes: {}", end-start, object.len());
        return Ok(Response::new(n2n::Value { data: object }));
    }
}
