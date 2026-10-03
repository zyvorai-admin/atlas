// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Networked data plane. A [`DataNodeServer`] serves one local [`FileDevice`] over TCP and a
//! [`RemoteDevice`] is the engine-side client, so replicas land on other hosts' disks.
//!
//! Wire format, both directions: a 4-byte big-endian header length, a JSON header, then the raw
//! payload the header announces (write data in requests, read data in responses). Without TLS
//! there is no authentication or encryption; bind to a private storage network only. With a
//! [`TlsIdentity`] connections are mutual TLS against the cluster CA: clients must present a
//! CA-signed certificate, and verify the node's certificate against its node id.
//!
//! Every write carries a fence (the writer's Raft term). The node durably records the highest
//! fence it has accepted and rejects lower ones, so a deposed leader that has not noticed yet
//! cannot overwrite a range the new leader has reallocated.

use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use rustls::{pki_types::ServerName, ClientConfig, ServerConfig};
use serde::{Deserialize, Serialize};

use crate::{
    device::{BlockStore, FileDevice},
    durable,
    engine::NativeError,
    metrics::PromText,
    tls::{self, Conn, TlsIdentity},
};

const MAX_HEADER: usize = 64 << 10;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
/// Largest payload accepted in either direction.
pub const MAX_PAYLOAD: u64 = 256 << 20;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request {
    Append { fence: u64, len: u64 },
    WriteAt { fence: u64, offset: u64, len: u64 },
    Read { offset: u64, len: u64 },
    Len,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Response {
    Appended { offset: u64 },
    Written,
    Data { len: u64 },
    Len { len: u64 },
    Fenced { current: u64 },
    Error { message: String },
}

#[derive(Debug, Default)]
struct Stats {
    appends: AtomicU64,
    writes: AtomicU64,
    reads: AtomicU64,
    fenced: AtomicU64,
    errors: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    tls_handshake_failures: AtomicU64,
}

struct Shared {
    device: FileDevice,
    fence: Mutex<u64>,
    fence_path: PathBuf,
    tls: Option<Arc<ServerConfig>>,
    stop: AtomicBool,
    stats: Stats,
    conns: Mutex<BTreeMap<u64, TcpStream>>,
    next_conn: AtomicU64,
    handlers: Mutex<Vec<JoinHandle<()>>>,
}

impl Shared {
    /// Runs `write` only if `fence` is not below the highest accepted fence. Holding the fence
    /// lock across the write keeps a concurrent higher fence from slipping in between.
    fn fenced(
        &self,
        fence: u64,
        write: impl FnOnce() -> Result<Response, NativeError>,
    ) -> Response {
        let Ok(mut current) = self.fence.lock() else {
            return Response::Error {
                message: "fence lock poisoned".into(),
            };
        };
        if fence < *current {
            self.stats.fenced.fetch_add(1, Ordering::Relaxed);
            return Response::Fenced { current: *current };
        }
        if fence > *current {
            if let Err(e) = durable::write_atomic(&self.fence_path, fence.to_string().as_bytes()) {
                self.stats.errors.fetch_add(1, Ordering::Relaxed);
                return Response::Error {
                    message: format!("persist fence: {e}"),
                };
            }
            *current = fence;
        }
        write().unwrap_or_else(|e| {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            Response::Error {
                message: e.to_string(),
            }
        })
    }
}

pub struct DataNodeServer {
    id: String,
    shared: Arc<Shared>,
    addr: SocketAddr,
    acceptor: Option<JoinHandle<()>>,
}

impl DataNodeServer {
    /// Serves `root/nvme0.data` on an already-bound `listener` without TLS. The highest
    /// accepted fence is kept in `root/fence`.
    pub fn start(
        id: impl Into<String>,
        root: impl AsRef<Path>,
        listener: TcpListener,
    ) -> Result<Self, NativeError> {
        Self::start_with(id, root, listener, None)
    }

    /// Like [`Self::start`], with mutual TLS when `tls` is set.
    pub fn start_with(
        id: impl Into<String>,
        root: impl AsRef<Path>,
        listener: TcpListener,
        tls: Option<TlsIdentity>,
    ) -> Result<Self, NativeError> {
        let tls = tls.map(|t| t.server_config()).transpose()?;
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let device = FileDevice::open(root.join("nvme0.data"))?;
        let fence_path = root.join("fence");
        let fence = match fs::read_to_string(&fence_path) {
            Ok(s) => s
                .trim()
                .parse()
                .map_err(|e| NativeError::Invalid(format!("corrupt fence file: {e}")))?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let shared = Arc::new(Shared {
            device,
            fence: Mutex::new(fence),
            fence_path,
            tls,
            stop: AtomicBool::new(false),
            stats: Stats::default(),
            conns: Mutex::new(BTreeMap::new()),
            next_conn: AtomicU64::new(0),
            handlers: Mutex::new(Vec::new()),
        });
        let acceptor = {
            let shared = shared.clone();
            thread::spawn(move || accept_loop(listener, &shared))
        };
        Ok(Self {
            id: id.into(),
            shared,
            addr,
            acceptor: Some(acceptor),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Highest fence accepted so far.
    pub fn fence(&self) -> u64 {
        self.shared.fence.lock().map(|f| *f).unwrap_or(0)
    }

    /// Prometheus text exposition for this node's request counters and device size.
    pub fn render_metrics(&self) -> Result<String, NativeError> {
        let s = &self.shared.stats;
        let node = [("node", self.id.as_str())];
        let mut p = PromText::new();
        p.family(
            "atlas_native_data_requests_total",
            "counter",
            "Data-node requests served, by operation.",
        );
        for (op, v) in [
            ("append", &s.appends),
            ("write_at", &s.writes),
            ("read", &s.reads),
        ] {
            p.sample(
                "atlas_native_data_requests_total",
                &[("node", self.id.as_str()), ("op", op)],
                v.load(Ordering::Relaxed),
            );
        }
        for (name, help, v) in [
            (
                "atlas_native_data_fenced_writes_total",
                "Writes rejected for carrying a stale fence.",
                &s.fenced,
            ),
            (
                "atlas_native_data_errors_total",
                "Requests that failed on the local device.",
                &s.errors,
            ),
            (
                "atlas_native_data_written_bytes_total",
                "Bytes written to the local device.",
                &s.bytes_written,
            ),
            (
                "atlas_native_data_read_bytes_total",
                "Bytes read from the local device.",
                &s.bytes_read,
            ),
            (
                "atlas_native_data_tls_handshake_failures_total",
                "Inbound TLS handshakes that failed.",
                &s.tls_handshake_failures,
            ),
        ] {
            p.family(name, "counter", help)
                .sample(name, &node, v.load(Ordering::Relaxed));
        }
        p.family(
            "atlas_native_data_fence",
            "gauge",
            "Highest write fence (Raft term) accepted.",
        )
        .sample("atlas_native_data_fence", &node, self.fence());
        p.family(
            "atlas_native_data_device_bytes",
            "gauge",
            "Size of the served device file.",
        )
        .sample(
            "atlas_native_data_device_bytes",
            &node,
            self.shared.device.len()?,
        );
        Ok(p.finish())
    }

    pub fn shutdown(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        // Join the acceptor first so no connection can be registered after the sweep below.
        if let Some(a) = self.acceptor.take() {
            let _ = a.join();
        }
        if let Ok(conns) = self.shared.conns.lock() {
            for c in conns.values() {
                let _ = c.shutdown(std::net::Shutdown::Both);
            }
        }
        let handlers: Vec<_> = self
            .shared
            .handlers
            .lock()
            .map(|mut h| h.drain(..).collect())
            .unwrap_or_default();
        for h in handlers {
            let _ = h.join();
        }
    }
}

impl Drop for DataNodeServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn accept_loop(listener: TcpListener, shared: &Arc<Shared>) {
    while !shared.stop.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                if stream.set_nonblocking(false).is_err() {
                    continue;
                }
                let _ = stream.set_nodelay(true);
                let Ok(clone) = stream.try_clone() else {
                    continue;
                };
                let conn_id = shared.next_conn.fetch_add(1, Ordering::Relaxed);
                if let Ok(mut conns) = shared.conns.lock() {
                    conns.insert(conn_id, clone);
                }
                let conn_shared = shared.clone();
                let handle = thread::spawn(move || {
                    match Conn::accept(stream, conn_shared.tls.as_ref(), HANDSHAKE_TIMEOUT) {
                        Ok(conn) => serve(conn, &conn_shared),
                        Err(_) => {
                            conn_shared
                                .stats
                                .tls_handshake_failures
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if let Ok(mut conns) = conn_shared.conns.lock() {
                        conns.remove(&conn_id);
                    }
                });
                if let Ok(mut h) = shared.handlers.lock() {
                    h.retain(|h| !h.is_finished());
                    h.push(handle);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(5));
            }
            Err(_) => thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn serve(mut stream: Conn, sh: &Shared) {
    while let Ok(req) = read_header::<Request>(&mut stream) {
        let result = match req {
            Request::Append { fence, len } => read_payload(&mut stream, len).and_then(|data| {
                let resp = sh.fenced(fence, || {
                    let offset = sh.device.append(&data)?;
                    sh.stats.appends.fetch_add(1, Ordering::Relaxed);
                    sh.stats.bytes_written.fetch_add(len, Ordering::Relaxed);
                    Ok(Response::Appended { offset })
                });
                write_message(&mut stream, &resp, &[])
            }),
            Request::WriteAt { fence, offset, len } => {
                read_payload(&mut stream, len).and_then(|data| {
                    let resp = sh.fenced(fence, || {
                        sh.device.write_at(offset, &data)?;
                        sh.stats.writes.fetch_add(1, Ordering::Relaxed);
                        sh.stats.bytes_written.fetch_add(len, Ordering::Relaxed);
                        Ok(Response::Written)
                    });
                    write_message(&mut stream, &resp, &[])
                })
            }
            Request::Read { offset, len } => {
                let read = if len > MAX_PAYLOAD {
                    Err(NativeError::Invalid(format!(
                        "read of {len} bytes too large"
                    )))
                } else {
                    sh.device.read_exact_at(offset, len as usize)
                };
                match read {
                    Ok(buf) => {
                        sh.stats.reads.fetch_add(1, Ordering::Relaxed);
                        sh.stats.bytes_read.fetch_add(len, Ordering::Relaxed);
                        write_message(&mut stream, &Response::Data { len }, &buf)
                    }
                    Err(e) => {
                        sh.stats.errors.fetch_add(1, Ordering::Relaxed);
                        let resp = Response::Error {
                            message: e.to_string(),
                        };
                        write_message(&mut stream, &resp, &[])
                    }
                }
            }
            Request::Len => {
                let resp = match sh.device.len() {
                    Ok(len) => Response::Len { len },
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                };
                write_message(&mut stream, &resp, &[])
            }
        };
        if result.is_err() {
            return;
        }
    }
}

/// Client for a [`DataNodeServer`]. Keeps one connection open and reconnects on failure.
pub struct RemoteDevice {
    target: String,
    timeout: Duration,
    tls: Option<(Arc<ClientConfig>, ServerName<'static>)>,
    conn: Mutex<Option<Conn>>,
}

impl std::fmt::Debug for RemoteDevice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteDevice")
            .field("target", &self.target)
            .field("tls", &self.tls.as_ref().map(|(_, n)| n))
            .finish()
    }
}

impl RemoteDevice {
    /// Plaintext client for `target`: a socket address or a `host:port` re-resolved on every
    /// connection. `timeout` bounds connecting, the handshake and each socket read or write.
    pub fn new(target: impl std::fmt::Display, timeout: Duration) -> Self {
        Self {
            target: target.to_string(),
            timeout,
            tls: None,
            conn: Mutex::new(None),
        }
    }

    /// Mutual-TLS client that requires the data node's certificate to be valid for `node_id`.
    pub fn with_tls(
        target: impl std::fmt::Display,
        node_id: &str,
        identity: &TlsIdentity,
        timeout: Duration,
    ) -> Result<Self, NativeError> {
        Ok(Self {
            target: target.to_string(),
            timeout,
            tls: Some((identity.client_config()?, tls::server_name(node_id)?)),
            conn: Mutex::new(None),
        })
    }

    pub fn target(&self) -> &str {
        &self.target
    }

    fn connect(&self) -> io::Result<Conn> {
        let s = TcpStream::connect_timeout(&tls::resolve(&self.target)?, self.timeout)?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(self.timeout))?;
        s.set_write_timeout(Some(self.timeout))?;
        Conn::connect(s, self.tls.as_ref().map(|(c, n)| (c, n.clone())))
    }

    fn exchange(s: &mut Conn, req: &Request, payload: &[u8]) -> io::Result<(Response, Vec<u8>)> {
        write_message(s, req, payload)?;
        let resp: Response = read_header(s)?;
        let data = match &resp {
            Response::Data { len } => read_payload(s, *len)?,
            _ => Vec::new(),
        };
        Ok((resp, data))
    }

    /// Sends one request. A failure on a reused connection (e.g. the node restarted) is retried
    /// once on a fresh one; a retried append can therefore leave an unreferenced copy behind.
    fn call(&self, req: &Request, payload: &[u8]) -> Result<(Response, Vec<u8>), NativeError> {
        let mut guard = self
            .conn
            .lock()
            .map_err(|_| NativeError::Poisoned("remote device"))?;
        let reused = guard.is_some();
        let mut stream = match guard.take() {
            Some(s) => s,
            None => self.connect()?,
        };
        let out = match Self::exchange(&mut stream, req, payload) {
            Err(_) if reused => {
                stream = self.connect()?;
                Self::exchange(&mut stream, req, payload)
            }
            other => other,
        }?;
        *guard = Some(stream);
        match out.0 {
            Response::Fenced { current } => Err(NativeError::Fenced { current }),
            Response::Error { message } => Err(NativeError::Remote(message)),
            _ => Ok(out),
        }
    }
}

fn unexpected(resp: Response) -> NativeError {
    NativeError::Remote(format!("unexpected data-node response: {resp:?}"))
}

impl BlockStore for RemoteDevice {
    fn append(&self, fence: u64, data: &[u8]) -> Result<u64, NativeError> {
        let req = Request::Append {
            fence,
            len: data.len() as u64,
        };
        match self.call(&req, data)?.0 {
            Response::Appended { offset } => Ok(offset),
            other => Err(unexpected(other)),
        }
    }

    fn write_at(&self, fence: u64, offset: u64, data: &[u8]) -> Result<(), NativeError> {
        let req = Request::WriteAt {
            fence,
            offset,
            len: data.len() as u64,
        };
        match self.call(&req, data)?.0 {
            Response::Written => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    fn read_exact_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, NativeError> {
        let req = Request::Read {
            offset,
            len: len as u64,
        };
        match self.call(&req, &[])? {
            (Response::Data { .. }, data) => Ok(data),
            (other, _) => Err(unexpected(other)),
        }
    }

    fn len(&self) -> Result<u64, NativeError> {
        match self.call(&Request::Len, &[])?.0 {
            Response::Len { len } => Ok(len),
            other => Err(unexpected(other)),
        }
    }
}

fn write_message(w: &mut impl Write, header: &impl Serialize, payload: &[u8]) -> io::Result<()> {
    let body = serde_json::to_vec(header).map_err(io::Error::other)?;
    if body.len() > MAX_HEADER || payload.len() as u64 > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "data-node message too large",
        ));
    }
    let mut frame = Vec::with_capacity(4 + body.len() + payload.len());
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame.extend_from_slice(payload);
    w.write_all(&frame)?;
    w.flush()
}

fn read_header<T: for<'de> Deserialize<'de>>(r: &mut impl Read) -> io::Result<T> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_HEADER {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "data-node header too large",
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    serde_json::from_slice(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

fn read_payload(r: &mut impl Read, len: u64) -> io::Result<Vec<u8>> {
    if len > MAX_PAYLOAD {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "data-node payload too large",
        ));
    }
    let mut buf = vec![0u8; len as usize];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_round_trip_and_size_guards() {
        let mut buf = Vec::new();
        write_message(
            &mut buf,
            &Request::WriteAt {
                fence: 7,
                offset: 9,
                len: 3,
            },
            b"abc",
        )
        .unwrap();
        let mut r = buf.as_slice();
        match read_header::<Request>(&mut r).unwrap() {
            Request::WriteAt { fence, offset, len } => {
                assert_eq!((fence, offset, len), (7, 9, 3));
                assert_eq!(read_payload(&mut r, len).unwrap(), b"abc");
            }
            other => panic!("unexpected {other:?}"),
        }

        let mut huge = ((MAX_HEADER + 1) as u32).to_be_bytes().to_vec();
        huge.extend_from_slice(b"{}");
        assert!(read_header::<Request>(&mut huge.as_slice()).is_err());
        assert!(read_payload(&mut [].as_slice(), MAX_PAYLOAD + 1).is_err());
    }
}
