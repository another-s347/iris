use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::{
    net::SocketAddr,
    pin::Pin,
    sync::{atomic::Ordering, Arc},
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Clone)]
pub struct DistributedTraffic {
    pub nodes: Arc<DashMap<SocketAddr, TrafficCounter>>,
}

impl std::fmt::Debug for DistributedTraffic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for r in self.nodes.iter() {
            let key = r.key();
            let value = r.value();
            writeln!(f, "{}: {:#?}", key, value)?;
        }

        Ok(())
    }
}

impl DistributedTraffic {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Clone, Default)]
pub struct TrafficCounter {
    pub in_pkts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub out_pkts: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub in_bytes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    pub out_bytes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl std::fmt::Debug for TrafficCounter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OUT {} pkts {} bytes, IN {} pkts {} bytes",
            self.out_pkts.load(Ordering::SeqCst),
            self.out_bytes.load(Ordering::SeqCst),
            self.in_pkts.load(Ordering::SeqCst),
            self.in_bytes.load(Ordering::SeqCst)
        )?;

        Ok(())
    }
}

pub struct CounterTcpStream(pub tokio::net::TcpStream, pub TrafficCounter);

impl tonic::transport::server::Connected for CounterTcpStream {
    fn remote_addr(&self) -> Option<SocketAddr> {
        self.0.remote_addr()
    }
}

impl AsyncRead for CounterTcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let size_before = buf.filled().len();
        match Pin::new(&mut self.0).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let size = buf.filled().len() - size_before;
                let counter = &self.1;
                counter.in_bytes.fetch_add(size, Ordering::SeqCst);
                counter.in_pkts.fetch_add(1, Ordering::SeqCst);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl AsyncWrite for CounterTcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let r = Pin::new(&mut self.0).poll_write(cx, buf);
        if let Poll::Ready(Ok(len)) = r {
            let counter = &self.1;
            if counter.out_bytes.fetch_add(len, Ordering::SeqCst) > usize::MAX - len {
                println!("{}, {}", counter.out_bytes.load(Ordering::SeqCst), len);
                unimplemented!()
            }
            counter.out_pkts.fetch_add(1, Ordering::SeqCst);
        }
        r
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

#[derive(Debug, Default, Clone)]
pub struct ExecutionMeter {
    pub call: Record,
    pub send: Record,
    pub get_attr: Record,
    pub get_remote_object: Record,
    pub create_object: Record,
    pub apply: Record,
    pub total: Record,
}

impl ExecutionMeter {
    pub fn set_record(&self, r: SingleCommand) {
        self.total.add(&r);
        match r.cmd {
            "Call" => {
                self.call.add(&r);
            }
            "Send" => {
                self.send.add(&r);
            }
            "GetAttr" => {
                self.get_attr.add(&r);
            }
            "CreateObject" => {
                self.create_object.add(&r);
            }
            "Apply" => {
                self.apply.add(&r);
            }
            "GetRemoteObject" => {
                self.get_remote_object.add(&r);
            }
            o => {
                panic!("unknown cmd {}", o);
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct Record {
    pub duration_all: Arc<AtomicU64>,
    pub duration_execution: Arc<AtomicU64>,
    pub duration_get_target_object: Arc<AtomicU64>,
    pub duration_after: Arc<AtomicU64>,
    pub duration_prepare: Arc<AtomicU64>,
    pub count_all: Arc<AtomicU64>,
    pub count_execution: Arc<AtomicU64>,
    pub count_get_target_object: Arc<AtomicU64>,
    pub count_after: Arc<AtomicU64>,
    pub count_prepare: Arc<AtomicU64>,
}

#[macro_use]
macro_rules! write_field {
    ($f:ident, $self:ident, $i:ident) => {
        paste::paste! {
            let sum = $self.[<duration_$i>].load(Ordering::SeqCst);
            let count = $self.[<count_$i>].load(Ordering::SeqCst);
        }
        if count == 0 {
            (&mut $f).field(
                stringify!($i),
                &format!(
                    "total {:.2}s, avg {:.2}ms, count {}",
                    sum as f64 / 1_000f64,
                    0,
                    0
                ),
            );
        } else {
            (&mut $f).field(
                stringify!($i),
                &format!(
                    "total {:.2}s, avg {:.2}ms, count {}",
                    sum as f64 / 1_000f64,
                    sum as f64 / count as f64,
                    count
                ),
            );
        }
    };
}

impl std::fmt::Debug for Record {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("Record");
        write_field!(d, self, all);
        write_field!(d, self, execution);
        write_field!(d, self, get_target_object);
        write_field!(d, self, after);
        write_field!(d, self, prepare);
        d.finish()?;
        Ok(())
    }
}

#[macro_use]
macro_rules! add {
    ($self:ident, $r:ident, $i:ident) => {
        paste::paste! {
            if let Some(d) = $r.[<duration_$i>] {
                $self.[<count_$i>].fetch_add(1, Ordering::SeqCst);
                $self.[<duration_$i>].fetch_add(d.as_millis() as u64, Ordering::SeqCst);
            }
        }
    };
}

impl Record {
    pub fn add(&self, r: &SingleCommand) {
        add!(self, r, all);
        add!(self, r, execution);
        add!(self, r, get_target_object);
        add!(self, r, after);
        add!(self, r, prepare);
    }
}

#[derive(Copy, Clone)]
pub struct SingleCommand {
    pub cmd: &'static str,
    pub duration_all: Option<Duration>,
    pub duration_execution: Option<Duration>,
    pub duration_get_target_object: Option<Duration>,
    pub duration_after: Option<Duration>,
    pub duration_prepare: Option<Duration>,
}
