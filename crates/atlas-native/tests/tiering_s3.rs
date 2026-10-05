// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

//! Tiering against a real S3-compatible bucket (Ceph RGW, MinIO, ...). Needs the `s3` feature
//! and `ATLAS_NATIVE_S3_ENDPOINT`, `_BUCKET`, `_ACCESS_KEY`, `_SECRET_KEY` (and optionally
//! `_REGION`); without them it passes without doing anything.

#![cfg(feature = "s3")]

use std::{sync::Arc, time::Duration};

use atlas_native::{
    object::{ObjectStore, S3ObjectStore},
    EngineConfig, FailureDomain, NativeEngine, Node, TierPolicy,
};

fn store() -> Option<S3ObjectStore> {
    let var = |k: &str| std::env::var(format!("ATLAS_NATIVE_S3_{k}")).ok();
    let (endpoint, bucket, key, secret) = (
        var("ENDPOINT")?,
        var("BUCKET")?,
        var("ACCESS_KEY")?,
        var("SECRET_KEY")?,
    );
    let region = var("REGION").unwrap_or_default();
    Some(S3ObjectStore::new(&endpoint, &region, &bucket, &key, &secret).unwrap())
}

#[test]
fn extents_tier_to_a_real_bucket_and_read_back() {
    let Some(store) = store() else {
        eprintln!("ATLAS_NATIVE_S3_* not set; skipping");
        return;
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let store: Arc<dyn ObjectStore> = Arc::new(store);
    let prefix = format!("atlas-native-test/{}/", uuid::Uuid::new_v4());
    let td = tempfile::tempdir().unwrap();
    let mut c = EngineConfig::new(td.path());
    c.extent_bytes = 1 << 20;
    c.objects = Some(store.clone());
    c.object_prefix = prefix.clone();
    let nodes = (1..=3)
        .map(|i| Node {
            id: format!("n{i}"),
            failure_domain: FailureDomain {
                zone: "z".into(),
                rack: format!("r{i}"),
                host: format!("h{i}"),
            },
            free_bytes: 1 << 30,
            healthy: true,
        })
        .collect();
    let e = NativeEngine::open(c, nodes).unwrap();
    let v = e.create_volume("v", 4 << 20).unwrap();
    let data: Vec<u8> = (0..4u32 << 20).map(|i| (i % 253) as u8).collect();
    e.write(&v, 0, &data).unwrap();

    let policy = TierPolicy {
        cold_after: Duration::ZERO,
        min_bytes: 0,
    };
    let st = e.tier_once(&policy, |_| {}, || false).unwrap();
    assert_eq!(st.tiered, 4, "{st:?}");
    assert_eq!(store.list(&prefix).unwrap().len(), 4);
    assert_eq!(e.read(&v, 0, data.len()).unwrap(), data);

    e.write(&v, 0, &vec![1u8; 4 << 20]).unwrap();
    assert_eq!(e.gc_once().unwrap().reclaimed, 4);
    assert!(
        store.list(&prefix).unwrap().is_empty(),
        "GC deletes the objects"
    );
}
