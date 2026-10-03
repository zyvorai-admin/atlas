// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use ring::digest::{digest, SHA256};

pub fn sha256(data: &[u8]) -> [u8; 32] {
    let d = digest(&SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}

pub fn verify(data: &[u8], expected: &[u8; 32]) -> bool {
    sha256(data) == *expected
}
