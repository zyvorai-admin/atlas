// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0
//! The node HTTP API over reqwest, failing over across endpoints.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use atlas_driver_core::DriverError;
use reqwest::{Method, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{NativeApi, NativeVolume, NodeStatus};

/// Where and how to reach a native cluster.
#[derive(Debug, Clone)]
pub struct HttpApiConfig {
    /// Base URLs of the nodes running the metadata role (`https://host:7480`).
    pub endpoints: Vec<String>,
    /// Bearer token (`api_token_file` on the nodes).
    pub token: Option<String>,
    /// PEM CA bundle to trust in addition to the system roots (`http_tls` with a private CA).
    pub ca_pem: Option<Vec<u8>>,
    /// PEM client certificate followed by its key, for nodes with `http_tls.client_ca`.
    pub identity_pem: Option<Vec<u8>>,
    pub timeout: Duration,
}

pub struct HttpApi {
    client: reqwest::Client,
    endpoints: Vec<String>,
    token: Option<String>,
    /// Index of the endpoint that last answered; tried first next time.
    preferred: AtomicUsize,
}

#[derive(Deserialize)]
struct Created {
    id: String,
}

#[derive(Deserialize)]
struct Volumes {
    volumes: Vec<NativeVolume>,
}

impl HttpApi {
    pub fn new(cfg: HttpApiConfig) -> Result<Self, DriverError> {
        let endpoints: Vec<String> = cfg
            .endpoints
            .iter()
            .map(|e| e.trim().trim_end_matches('/').to_string())
            .filter(|e| !e.is_empty())
            .collect();
        if endpoints.is_empty() {
            return Err(DriverError::Backend(
                "atlas-native: at least one endpoint is required".into(),
            ));
        }
        let mut builder = reqwest::Client::builder().timeout(cfg.timeout);
        if let Some(pem) = &cfg.ca_pem {
            let certs = reqwest::Certificate::from_pem_bundle(pem)
                .map_err(|e| DriverError::Backend(format!("atlas-native CA bundle: {e}")))?;
            if certs.is_empty() {
                return Err(DriverError::Backend(
                    "atlas-native CA bundle has no certificate".into(),
                ));
            }
            builder = builder.tls_certs_merge(certs);
        }
        if let Some(pem) = &cfg.identity_pem {
            let id = reqwest::Identity::from_pem(pem).map_err(|e| {
                DriverError::Backend(format!("atlas-native client certificate: {e}"))
            })?;
            builder = builder.identity(id);
        }
        let client = builder
            .build()
            .map_err(|e| DriverError::Backend(format!("atlas-native HTTP client: {e}")))?;
        Ok(Self {
            client,
            endpoints,
            token: cfg.token.filter(|t| !t.is_empty()),
            preferred: AtomicUsize::new(0),
        })
    }

    /// Sends to each endpoint in turn, starting with the last one that answered, until one
    /// answers with something other than "not the leader" or "try again".
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Vec<u8>, DriverError> {
        let n = self.endpoints.len();
        let start = self.preferred.load(Ordering::Relaxed) % n;
        let mut last = String::new();
        for k in 0..n {
            let i = (start + k) % n;
            let url = format!("{}{path}", self.endpoints[i]);
            let mut req = self.client.request(method.clone(), &url);
            if let Some(t) = &self.token {
                req = req.bearer_auth(t);
            }
            if let Some(b) = &body {
                req = req.json(b);
            }
            let resp = match req.send().await {
                Ok(r) => r,
                Err(e) => {
                    last = format!("{url}: {e}");
                    continue;
                }
            };
            let status = resp.status();
            let bytes = resp
                .bytes()
                .await
                .map_err(|e| DriverError::Unreachable(format!("{url}: {e}")))?
                .to_vec();
            if status.is_success() {
                self.preferred.store(i, Ordering::Relaxed);
                return Ok(bytes);
            }
            let msg = serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
                .unwrap_or_else(|| String::from_utf8_lossy(&bytes).trim().to_string());
            match status {
                StatusCode::MISDIRECTED_REQUEST | StatusCode::SERVICE_UNAVAILABLE => {
                    last = format!("{url}: {status} {msg}");
                }
                StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                    return Err(DriverError::Unreachable(format!(
                        "atlas-native refused the credentials: {status} {msg}"
                    )));
                }
                StatusCode::NOT_FOUND => {
                    return Err(DriverError::Backend(format!("not found: {msg}")));
                }
                _ => return Err(DriverError::Backend(format!("{status}: {msg}"))),
            }
        }
        Err(DriverError::Unreachable(format!(
            "no atlas-native endpoint accepted {method} {path} (last: {last})"
        )))
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T, DriverError> {
        let b = self.call(Method::GET, path, None).await?;
        serde_json::from_slice(&b).map_err(|e| DriverError::Parse(format!("{path}: {e}")))
    }

    async fn post_created(&self, path: &str, body: Value) -> Result<String, DriverError> {
        let b = self.call(Method::POST, path, Some(body)).await?;
        serde_json::from_slice::<Created>(&b)
            .map(|c| c.id)
            .map_err(|e| DriverError::Parse(format!("{path}: {e}")))
    }
}

/// Native ids are server-generated UUIDs; anything else is refused before it reaches a path.
fn path_id(id: &str) -> Result<&str, DriverError> {
    if !id.is_empty() && id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        Ok(id)
    } else {
        Err(DriverError::Backend(format!(
            "invalid atlas-native id {id:?}"
        )))
    }
}

#[async_trait]
impl NativeApi for HttpApi {
    async fn status(&self) -> Result<NodeStatus, DriverError> {
        self.get("/v1/status").await
    }

    async fn volumes(&self) -> Result<Vec<NativeVolume>, DriverError> {
        Ok(self.get::<Volumes>("/v1/volumes").await?.volumes)
    }

    async fn create_volume(&self, name: &str, size_bytes: u64) -> Result<String, DriverError> {
        self.post_created(
            "/v1/volumes",
            json!({ "name": name, "size_bytes": size_bytes }),
        )
        .await
    }

    async fn delete_volume(&self, id: &str) -> Result<(), DriverError> {
        let path = format!("/v1/volumes/{}", path_id(id)?);
        self.call(Method::DELETE, &path, None).await.map(drop)
    }

    async fn create_snapshot(&self, volume_id: &str, name: &str) -> Result<String, DriverError> {
        let path = format!("/v1/volumes/{}/snapshots", path_id(volume_id)?);
        self.post_created(&path, json!({ "name": name })).await
    }

    async fn delete_snapshot(&self, id: &str) -> Result<(), DriverError> {
        let path = format!("/v1/snapshots/{}", path_id(id)?);
        self.call(Method::DELETE, &path, None).await.map(drop)
    }
}
