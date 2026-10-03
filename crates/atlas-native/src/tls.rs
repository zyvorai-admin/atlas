// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Mutual TLS for the Raft and data-node transports. Every node holds a certificate signed by a
//! shared cluster CA whose DNS SAN is its node id; both sides of every connection verify the other.
//!
//! Uses rustls with an explicit `ring` provider, so it never depends on (or installs) the
//! process-wide default provider.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::Arc,
    time::Duration,
};

use rustls::{
    client::ClientConnection,
    crypto::CryptoProvider,
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, ServerName},
    server::{ServerConnection, WebPkiClientVerifier},
    ClientConfig, RootCertStore, ServerConfig, StreamOwned,
};

/// A node's TLS identity: the cluster CA bundle, its certificate chain and private key (PEM).
#[derive(Debug)]
pub struct TlsIdentity {
    roots: Arc<RootCertStore>,
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

impl Clone for TlsIdentity {
    fn clone(&self) -> Self {
        Self {
            roots: self.roots.clone(),
            chain: self.chain.clone(),
            key: self.key.clone_key(),
        }
    }
}

fn invalid(msg: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

impl TlsIdentity {
    pub fn from_pem(ca_pem: &[u8], cert_pem: &[u8], key_pem: &[u8]) -> io::Result<Self> {
        let mut roots = RootCertStore::empty();
        for ca in CertificateDer::pem_slice_iter(ca_pem) {
            roots
                .add(ca.map_err(|e| invalid(format!("CA bundle: {e}")))?)
                .map_err(|e| invalid(format!("CA certificate: {e}")))?;
        }
        if roots.is_empty() {
            return Err(invalid("CA bundle has no certificates"));
        }
        let chain = CertificateDer::pem_slice_iter(cert_pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| invalid(format!("certificate: {e}")))?;
        if chain.is_empty() {
            return Err(invalid("certificate file has no certificates"));
        }
        let key =
            PrivateKeyDer::from_pem_slice(key_pem).map_err(|e| invalid(format!("key: {e}")))?;
        Ok(Self {
            roots: Arc::new(roots),
            chain,
            key,
        })
    }

    pub fn from_pem_files(
        ca: impl AsRef<std::path::Path>,
        cert: impl AsRef<std::path::Path>,
        key: impl AsRef<std::path::Path>,
    ) -> io::Result<Self> {
        Self::from_pem(
            &std::fs::read(ca)?,
            &std::fs::read(cert)?,
            &std::fs::read(key)?,
        )
    }

    /// Server side: requires a client certificate signed by the cluster CA.
    pub(crate) fn server_config(&self) -> io::Result<Arc<ServerConfig>> {
        let verifier = WebPkiClientVerifier::builder_with_provider(self.roots.clone(), provider())
            .build()
            .map_err(invalid)?;
        let cfg = ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(invalid)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(self.chain.clone(), self.key.clone_key())
            .map_err(invalid)?;
        Ok(Arc::new(cfg))
    }

    /// Client side: presents this identity and verifies the server against the cluster CA.
    pub(crate) fn client_config(&self) -> io::Result<Arc<ClientConfig>> {
        let cfg = ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(invalid)?
            .with_root_certificates(self.roots.clone())
            .with_client_auth_cert(self.chain.clone(), self.key.clone_key())
            .map_err(invalid)?;
        Ok(Arc::new(cfg))
    }
}

/// `name` as a TLS server name; node ids must be valid DNS names to be used with TLS.
pub(crate) fn server_name(name: &str) -> io::Result<ServerName<'static>> {
    ServerName::try_from(name.to_string()).map_err(|e| invalid(format!("server name {name}: {e}")))
}

/// A plaintext or TLS connection over a blocking `TcpStream`.
pub(crate) enum Conn {
    Plain(TcpStream),
    Server(Box<StreamOwned<ServerConnection, TcpStream>>),
    Client(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl Conn {
    /// Wraps an accepted socket, completing the TLS handshake (bounded by `timeout`) when
    /// `tls` is set.
    pub(crate) fn accept(
        sock: TcpStream,
        tls: Option<&Arc<ServerConfig>>,
        timeout: Duration,
    ) -> io::Result<Self> {
        let Some(cfg) = tls else {
            return Ok(Self::Plain(sock));
        };
        let prior = sock.read_timeout()?;
        sock.set_read_timeout(Some(timeout))?;
        let conn = ServerConnection::new(cfg.clone()).map_err(io::Error::other)?;
        let mut s = StreamOwned::new(conn, sock);
        while s.conn.is_handshaking() {
            s.conn.complete_io(&mut s.sock)?;
        }
        s.sock.set_read_timeout(prior)?;
        Ok(Self::Server(Box::new(s)))
    }

    /// Wraps a connected socket (whose timeouts bound the handshake), verifying the server as
    /// `name` when `tls` is set.
    pub(crate) fn connect(
        sock: TcpStream,
        tls: Option<(&Arc<ClientConfig>, ServerName<'static>)>,
    ) -> io::Result<Self> {
        let Some((cfg, name)) = tls else {
            return Ok(Self::Plain(sock));
        };
        let conn = ClientConnection::new(cfg.clone(), name).map_err(io::Error::other)?;
        let mut s = StreamOwned::new(conn, sock);
        while s.conn.is_handshaking() {
            s.conn.complete_io(&mut s.sock)?;
        }
        Ok(Self::Client(Box::new(s)))
    }

    /// Of `candidates`, the names the verified client certificate is valid for. Empty for a
    /// plaintext connection.
    pub(crate) fn peer_names<'a>(&self, candidates: &'a [String]) -> Vec<&'a String> {
        let Self::Server(s) = self else {
            return Vec::new();
        };
        let Some(cert) = s.conn.peer_certificates().and_then(|c| c.first()) else {
            return Vec::new();
        };
        let Ok(ee) = webpki::EndEntityCert::try_from(cert) else {
            return Vec::new();
        };
        candidates
            .iter()
            .filter(|c| {
                server_name(c)
                    .map(|n| ee.verify_is_valid_for_subject_name(&n).is_ok())
                    .unwrap_or(false)
            })
            .collect()
    }

    pub(crate) fn is_tls(&self) -> bool {
        !matches!(self, Self::Plain(_))
    }
}

impl Read for Conn {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Server(s) => s.read(buf),
            Self::Client(s) => s.read(buf),
        }
    }
}

impl Write for Conn {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Server(s) => s.write(buf),
            Self::Client(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Server(s) => s.flush(),
            Self::Client(s) => s.flush(),
        }
    }
}
