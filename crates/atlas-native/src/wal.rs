// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error("wal io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("wal decode error: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("wal index regression: expected > {last}, got {next}")]
    IndexRegression { last: u64, next: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WalRecord<T> {
    pub term: u64,
    pub index: u64,
    pub command: T,
}

#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: File,
    last_index: u64,
}

impl Wal {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, WalError> {
        fs::create_dir_all(root.as_ref())?;
        let path = root.as_ref().join("metadata.wal");
        let last_index = if path.exists() {
            let f = File::open(&path)?;
            let mut last = 0;
            for line in BufReader::new(f).lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                let v: serde_json::Value = serde_json::from_str(&line)?;
                last = v.get("index").and_then(|v| v.as_u64()).unwrap_or(last);
            }
            last
        } else {
            0
        };
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        Ok(Self {
            path,
            file,
            last_index,
        })
    }

    pub fn append<T: Serialize>(&mut self, record: &WalRecord<T>) -> Result<(), WalError> {
        if record.index <= self.last_index {
            return Err(WalError::IndexRegression {
                last: self.last_index,
                next: record.index,
            });
        }
        serde_json::to_writer(&mut self.file, record)?;
        self.file.write_all(b"\n")?;
        self.file.sync_data()?;
        self.last_index = record.index;
        Ok(())
    }

    pub fn replay<T: DeserializeOwned>(&self) -> Result<Vec<WalRecord<T>>, WalError> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let f = File::open(&self.path)?;
        let mut out = Vec::new();
        for line in BufReader::new(f).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(serde_json::from_str(&line)?);
        }
        Ok(out)
    }

    pub fn last_index(&self) -> u64 {
        self.last_index
    }
}
