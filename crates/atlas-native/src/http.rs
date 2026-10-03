// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! A minimal blocking HTTP/1.1 server for the native node's ops and volume API: one request per
//! connection (`Connection: close`), `Content-Length` bodies only, bounded headers and bodies.

use std::{
    collections::BTreeMap,
    io::{self, BufRead, BufReader, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

const MAX_HEADER_BYTES: usize = 16 << 10;
const IO_TIMEOUT: Duration = Duration::from_secs(30);
/// After responding, unread request bytes (e.g. a refused oversized body) are drained up to this
/// much so closing the socket does not reset the connection before the client reads the reply.
const LINGER_DRAIN_BYTES: usize = 4 << 20;
const LINGER_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    /// Header names lower-cased.
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub content_type: &'static str,
    pub body: Vec<u8>,
}

impl Response {
    pub fn text(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8",
            body: body.into().into_bytes(),
        }
    }

    pub fn json(status: u16, value: &serde_json::Value) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: serde_json::to_vec(value).unwrap_or_default(),
        }
    }

    pub fn bytes(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: "application/octet-stream",
            body,
        }
    }
}

pub type Handler = Arc<dyn Fn(Request) -> Response + Send + Sync>;

pub struct HttpServer {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    acceptor: Option<JoinHandle<()>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl HttpServer {
    /// Serves `handler` on an already-bound `listener`; request bodies above `max_body` bytes
    /// are refused with 413.
    pub fn start(listener: TcpListener, max_body: usize, handler: Handler) -> io::Result<Self> {
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let workers: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let acceptor = {
            let stop = stop.clone();
            let workers = workers.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let handler = handler.clone();
                            let h = thread::spawn(move || serve(stream, max_body, &handler));
                            if let Ok(mut w) = workers.lock() {
                                w.retain(|h| !h.is_finished());
                                w.push(h);
                            }
                        }
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => thread::sleep(Duration::from_millis(20)),
                    }
                }
            })
        };
        Ok(Self {
            addr,
            stop,
            acceptor: Some(acceptor),
            workers,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stops accepting and waits for in-flight requests (each bounded by the socket timeouts).
    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(a) = self.acceptor.take() {
            let _ = a.join();
        }
        let workers: Vec<_> = self
            .workers
            .lock()
            .map(|mut w| w.drain(..).collect())
            .unwrap_or_default();
        for w in workers {
            let _ = w.join();
        }
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn serve(stream: TcpStream, max_body: usize, handler: &Handler) {
    if stream.set_nonblocking(false).is_err()
        || stream.set_read_timeout(Some(IO_TIMEOUT)).is_err()
        || stream.set_write_timeout(Some(IO_TIMEOUT)).is_err()
    {
        return;
    }
    let Ok(mut out) = stream.try_clone() else {
        return;
    };
    let resp = match read_request(stream, max_body) {
        Ok(req) => handler(req),
        Err(resp) => resp,
    };
    if write_response(&mut out, &resp).is_err() {
        return;
    }
    let _ = out.shutdown(Shutdown::Write);
    let _ = out.set_read_timeout(Some(LINGER_TIMEOUT));
    let mut sink = [0u8; 8192];
    let mut drained = 0;
    while drained < LINGER_DRAIN_BYTES {
        match out.read(&mut sink) {
            Ok(0) | Err(_) => break,
            Ok(n) => drained += n,
        }
    }
}

fn read_request(stream: TcpStream, max_body: usize) -> Result<Request, Response> {
    let bad = |msg: &str| Response::text(400, msg.to_string());
    let mut r = BufReader::new(stream);
    let mut head = Vec::new();
    loop {
        let mut line = Vec::new();
        let n = (&mut r)
            .take((MAX_HEADER_BYTES - head.len()) as u64 + 1)
            .read_until(b'\n', &mut line)
            .map_err(|_| bad("unreadable request"))?;
        if n == 0 {
            return Err(bad("connection closed mid-request"));
        }
        head.extend_from_slice(&line);
        if head.len() > MAX_HEADER_BYTES {
            return Err(Response::text(431, "request headers too large"));
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
    }
    let head = String::from_utf8(head).map_err(|_| bad("non-UTF-8 request head"))?;
    let mut lines = head.lines();
    let mut parts = lines.next().unwrap_or_default().split_whitespace();
    let (Some(method), Some(target), Some(_version)) = (parts.next(), parts.next(), parts.next())
    else {
        return Err(bad("malformed request line"));
    };
    let mut headers = BTreeMap::new();
    for line in lines.filter(|l| !l.is_empty()) {
        let (k, v) = line
            .split_once(':')
            .ok_or_else(|| bad("malformed header"))?;
        headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
    }
    if headers.contains_key("transfer-encoding") {
        return Err(Response::text(
            411,
            "chunked bodies are not supported; send Content-Length",
        ));
    }
    let len = match headers.get("content-length") {
        Some(v) => v.parse::<usize>().map_err(|_| bad("bad Content-Length"))?,
        None => 0,
    };
    if len > max_body {
        return Err(Response::text(
            413,
            format!("body exceeds {max_body} bytes"),
        ));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)
        .map_err(|_| bad("body shorter than Content-Length"))?;
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p, parse_query(q)),
        None => (target, BTreeMap::new()),
    };
    Ok(Request {
        method: method.to_string(),
        path: path.to_string(),
        query,
        headers,
        body,
    })
}

fn parse_query(q: &str) -> BTreeMap<String, String> {
    q.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => (kv.to_string(), String::new()),
        })
        .collect()
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Payload Too Large",
        421 => "Misdirected Request",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    }
}

fn write_response(w: &mut impl Write, resp: &Response) -> io::Result<()> {
    let head = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        resp.status,
        reason(resp.status),
        resp.content_type,
        resp.body.len()
    );
    w.write_all(head.as_bytes())?;
    w.write_all(&resp.body)?;
    w.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_request_and_enforces_limits() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let handler: Handler = Arc::new(|req: Request| {
            Response::text(
                200,
                format!(
                    "{} {} {:?} {}",
                    req.method,
                    req.path,
                    req.query.get("offset"),
                    req.body.len()
                ),
            )
        });
        let mut srv = HttpServer::start(l, 8, handler).unwrap();
        let send = |raw: &[u8]| {
            let mut s = TcpStream::connect(srv.local_addr()).unwrap();
            s.write_all(raw).unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        };
        let ok = send(b"PUT /v1/x?offset=4 HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc");
        assert!(ok.starts_with("HTTP/1.1 200 OK"), "{ok}");
        assert!(ok.ends_with("PUT /v1/x Some(\"4\") 3"), "{ok}");
        let big = send(b"PUT / HTTP/1.1\r\nContent-Length: 9\r\n\r\n123456789");
        assert!(big.starts_with("HTTP/1.1 413"), "{big}");
        let chunked = send(b"PUT / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n");
        assert!(chunked.starts_with("HTTP/1.1 411"), "{chunked}");
        let junk = send(b"nonsense\r\n\r\n");
        assert!(junk.starts_with("HTTP/1.1 400"), "{junk}");
        srv.shutdown();
    }
}
