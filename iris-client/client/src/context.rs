use crate::hello_world::greeter_client::GreeterClient;

use futures::prelude::*;
use pyo3::prelude::*;
use tracing::info;
use tracing_subscriber::{Registry, fmt, prelude::__tracing_subscriber_SubscriberExt};


use std::collections::HashMap;
use std::convert::TryFrom;


use tokio::net::UnixStream;

use tokio::runtime;


use tonic;
use tonic::transport::{Endpoint, Uri};
use tower::util::service_fn;


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
    fn new(py:Python<'_>, config: &PyAny) -> PyResult<Self> {
        let d:bool = config.getattr("debug")?.extract()?;
        let log_color:bool = config.getattr("log_color")?.extract()?;
        setup_global_subscriber(d, log_color);
        info!("Debug: {:#?}, Log with color: {}", d, log_color);

        let basic_rt = runtime::Builder::new()
            .threaded_scheduler()
            .enable_all()
            .build()
            .unwrap();

        Ok(IrisContextInternal {
            runtime: basic_rt,
            nodes: HashMap::new(),
        })
    }

    fn connect(&mut self, py:Python<'_>, address: String, node:String) -> PyResult<crate::IrisClientInternal> {
        let handle = self.runtime
            .spawn(_connect(address));
        let client = py.allow_threads(|| {
            self.runtime.block_on(handle)
        }).unwrap();
        Ok(crate::IrisClientInternal {
            runtime_handle: self.runtime.handle().clone(),
            client: client.unwrap(),
            async_tasks: Default::default(),
            node,
            mem: crate::ClientMem {}
        })
    }
}

fn setup_global_subscriber(debug:bool, log_color: bool) {

    let filter = if debug { tracing_subscriber::filter::EnvFilter::new("client=trace,mio=info,hyper=info") } else { tracing_subscriber::filter::EnvFilter::new("client=info,mio=info,hyper=info") };
    let fmt_layer = fmt::Layer::default()
        .with_ansi(log_color)
        .with_timer(tracing_subscriber::fmt::time::SystemTime);

    let subscriber = Registry::default()
        .with(filter)
        .with(fmt_layer);

    tracing::subscriber::set_global_default(subscriber).expect("Could not set global default");
}
