use dashmap::DashMap;

use futures::prelude::*;
use futures::stream::TryStreamExt;
use hello_world::{
    greeter_server::{Greeter, GreeterServer},
    *,
};

use proto::hello_world;

use pyo3::prelude::*;

use std::{
    collections::HashMap,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex}, pin::Pin, task::{Poll, Context},
};
use structopt::StructOpt;
use tokio::{io::{AsyncWrite, AsyncRead}, net::UnixListener};

use tonic::{
    transport::{Server, Uri},
    Request, Response, Status,
};
use tracing::{debug, event, info, span, Level};
use tracing_futures::*;
use tracing_subscriber;
use tracing_subscriber::{fmt, prelude::*, registry::Registry};
use tracing_timing::{Builder, Histogram};
use opentelemetry::{api::Provider, sdk};
use metrics::CounterTcpStream;

pub mod command_server;
pub mod distributed;
pub mod mem;
pub mod utils;
pub mod metrics;

#[derive(Debug, StructOpt)]
#[structopt(name = "example", about = "An example of StructOpt usage.")]
struct Opt {
    /// Activate debug mode
    // short and long flags (-d, --debug) will be deduced from the field's name
    #[structopt(short, long)]
    pub debug: bool,

    /// Set speed
    // we don't want to name it "speed", need to look smart
    #[structopt(short = "l", long = "listen", default_value = "127.0.0.1")]
    pub address: String,

    #[structopt(short = "p", long = "port", default_value = "12345")]
    pub port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt: Opt = Opt::from_args();
    setup_global_subscriber();

    let pickle = {
        let gil = Python::acquire_gil();
        let py = gil.python();
        let pickle = py.import("dill").unwrap();
        Py::from(pickle)
    };

    let path = format!("/tmp/iris-tmp-node-{}-{}.sock", opt.address, opt.port);

    tokio::fs::create_dir_all(Path::new(&path).parent().unwrap()).await?;

    let mut uds = UnixListener::bind(&path)?;
    let mut tcp = tokio::net::TcpListener::bind(SocketAddr::new(opt.address.parse().unwrap(), opt.port)).await?;
    let traffic = metrics::DistributedTraffic::new();

    let objects = crate::mem::Mem::default();
    let addrs = Arc::new(DashMap::new());
    let metrics = metrics::ExecutionMeter::default();

    let distributed_server = distributed::NodeServer {
        pickle: pickle.clone(),
        objects: objects.clone(),
        current_node: format!("node{}:{}", opt.address, opt.port),
        node_addr: addrs.clone(),
        metrics: metrics.clone(),
        clock: quanta::Clock::new(),
    };

    let greeter = command_server::IrisServer {
        modules: Arc::new(DashMap::new()),
        objects: objects,
        nodes: Arc::new(DashMap::new()),
        pickle,
        current_node: Arc::new(format!("node{}:{}", opt.address, opt.port)),
        nodes_addr: addrs,
        metrics: metrics.clone(),
        clock: quanta::Clock::new(),
    };

    let t2 = traffic.clone();
    let (tx, mut rx) = tokio::sync::broadcast::channel(1);
    let mut rx2 = tx.subscribe();
    ctrlc::set_handler(move || {
        tx.send(()).unwrap();
    })
    .expect("Error setting Ctrl-C handler");

    // tokio::spawn(async move {
    //     // let span = span!(Level::TRACE, "profile");
    //     // let _g = span.enter();
    //     let mut t = tokio::time::interval(std::time::Duration::from_secs(1));
    //     loop {
    //         t.tick().await;
    //         event!(Level::DEBUG,"{:?}", metrics);
    //         event!(Level::DEBUG,"{:?}", t2);
    //     }
    // }.instrument(tracing::info_span!("profile")));

    let server_iris = Server::builder()
        .add_service(GreeterServer::new(greeter))
        // .serve_with_shutdown("127.0.0.1:12345".parse().unwrap(), rx.recv().map(|_|()))
        .serve_with_incoming_shutdown(
            uds.incoming().map_ok(unix::UnixStream),
            rx.recv().map(|_| ()),
        );

    let t2 = tokio::spawn(async move {
        let server_n2n = Server::builder()
            .concurrency_limit_per_connection(4096)
            .add_service(proto::n2n::n2n_server::N2nServer::new(distributed_server))
            .serve_with_incoming_shutdown(
                tcp.incoming().map_ok(|x|{
                    let counter = metrics::TrafficCounter::default();
                    traffic.nodes.insert(x.peer_addr().unwrap(), counter.clone());
                    x.set_nodelay(true).unwrap();
                    CounterTcpStream(x, counter)
                }),
                rx2.recv().map(|_| ()),
            );
        server_n2n.await
    });

    let (_r1, _r2) = tokio::join!(server_iris, t2);

    tokio::fs::remove_file(Path::new(&path)).await?;

    Ok(())
}

fn setup_global_subscriber() {
    let exporter = opentelemetry_jaeger::Exporter::builder()
        .with_agent_endpoint("127.0.0.1:6831".parse().unwrap())
        .with_process(opentelemetry_jaeger::Process {
            service_name: "report_example".to_string(),
            tags: Vec::new(),
        })
        .init().unwrap();
    let provider = sdk::Provider::builder()
        .with_simple_exporter(exporter)
        .with_config(sdk::Config {
            default_sampler: Box::new(sdk::Sampler::Always),
            ..Default::default()
        })
        .build();
    let tracer = provider.get_tracer("tracing");
    let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    let filter = tracing_subscriber::filter::EnvFilter::new("server=trace,mio=info,hyper=info");
    let fmt_layer = fmt::Layer::default();

    let subscriber = Registry::default()
    .with(filter)
    .with(fmt_layer)
    .with(telemetry)
    ;

    tracing::subscriber::set_global_default(subscriber).expect("Could not set global default");
}



#[cfg(unix)]
mod unix {
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    use tokio::io::{AsyncRead, AsyncWrite};
    use tonic::transport::server::Connected;

    #[derive(Debug)]
    pub struct UnixStream(pub tokio::net::UnixStream);

    impl Connected for UnixStream {}

    impl AsyncRead for UnixStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for UnixStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.0).poll_shutdown(cx)
        }
    }
}
