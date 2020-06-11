use std::{net::SocketAddr, sync::{atomic::Ordering, Arc}, pin::Pin, task::{Poll, Context}};
use dashmap::DashMap;
use tokio::io::{AsyncWrite, AsyncRead};
use std::sync::atomic::{AtomicUsize, AtomicU64};

#[derive(Debug, Clone)]
pub struct DistributedTraffic {
    pub nodes: Arc<DashMap<SocketAddr, TrafficCounter>>
}

impl DistributedTraffic {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new())
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TrafficCounter {
    pub in_pkts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub out_pkts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub in_bytes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub out_bytes: std::sync::Arc<std::sync::atomic::AtomicUsize>
}

pub struct CounterTcpStream(pub tokio::net::TcpStream, pub TrafficCounter);

impl tonic::transport::server::Connected for CounterTcpStream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.0.remote_addr()
    }
}

impl AsyncRead for CounterTcpStream {
    fn poll_read(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>, buf: &mut [u8])
            -> std::task::Poll<std::io::Result<usize>> {
        let len = buf.len();
        let counter = &self.1;
        counter.in_bytes.fetch_add(len, Ordering::SeqCst);
        counter.in_pkts.fetch_add(1, Ordering::SeqCst);
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for CounterTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let len = buf.len();
        let counter = &self.1;
        if counter.out_bytes.fetch_add(len, Ordering::SeqCst) > usize::MAX {
            unimplemented!()
        }
        counter.out_pkts.fetch_add(1, Ordering::SeqCst);
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

#[derive(Debug, Default, Clone)]
pub struct ExecutionMeter {
    pub n2n_getobject_hitcount: Arc<AtomicU64>,
    pub n2n_getobject_throughput: Arc<AtomicU64>,
    pub n2n_getobject_responsetime: Arc<AtomicU64>,

    pub call_hitcount: Arc<AtomicU64>,
    // pub call_throughput: Arc<AtomicU64>,
    pub call_responsetime: Arc<AtomicU64>,
    pub createobject_hitcount: Arc<AtomicU64>,
    // pub call_throughput: Arc<AtomicU64>,
    pub createobject_responsetime: Arc<AtomicU64>,
    pub getattr_hitcount: Arc<AtomicU64>,
    pub getattr_throughput: Arc<AtomicU64>,
    pub getattr_responsetime: Arc<AtomicU64>,
}