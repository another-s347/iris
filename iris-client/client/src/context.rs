use crate::hello_world::greeter_client::GreeterClient;
use crate::hello_world::*;
use futures::prelude::*;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use rayon::prelude::*;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::fs;
use std::{task::{Poll, Context}, path::PathBuf, pin::Pin};
use tokio::net::UnixStream;
use tokio::prelude::*;
use tokio::runtime;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic;
use tonic::transport::{Endpoint, Uri};
use tower::service_fn;
use std::path::Path;

type RpcClient = GreeterClient<tonic::transport::channel::Channel>;
type tonicResponseResult<T> = Result<tonic::Response<T>, tonic::Status>;

async fn _connect(address: String) -> Result<RpcClient, tonic::transport::Error> {
    // panic!();
    let channel = Endpoint::try_from(format!("http://[::]:50051{}", address))
        .unwrap()
        .connect_with_connector(service_fn(|uri: Uri| {
            UnixStream::connect(uri.path().to_owned())
        }))
        .await?;
    let client = GreeterClient::new(channel);
    Ok(client)
}

#[pyclass(module = "client")]
pub struct IrisContextInternal {
    pub runtime: tokio::runtime::Runtime,
    pub nodes: HashMap<i32, i32>,
}

#[pymethods]
impl IrisContextInternal {
    #[new]
    fn new() -> Self {
        let basic_rt = runtime::Builder::new()
            .threaded_scheduler()
            .enable_all()
            .build()
            .unwrap();

        IrisContextInternal {
            runtime: basic_rt,
            nodes: HashMap::new(),
        }
    }

    fn connect(&mut self, py:Python<'_>, address: String) -> PyResult<crate::IrisClientInternal> {
        let handle = self.runtime
            .spawn(_connect(address));
        let client = py.allow_threads(|| {
            self.runtime.block_on(handle)
        }).unwrap();
        Ok(crate::IrisClientInternal {
            runtime_handle: self.runtime.handle().clone(),
            client: client.unwrap(),
            async_tasks: Default::default()
        })
    }

}
