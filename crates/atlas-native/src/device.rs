// Copyright (c) 2026 ZyvorAI Labs Private Limited.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::engine::NativeError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DeviceId(pub String);

#[derive(Debug)]
pub struct FileDevice {
    id: DeviceId,
    path: PathBuf,
    file: Mutex<File>,
}

impl FileDevice {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, NativeError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok(Self {
            id: DeviceId(Uuid::new_v4().to_string()),
            path,
            file: Mutex::new(file),
        })
    }

    pub fn id(&self) -> &DeviceId {
        &self.id
    }
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, data: &[u8]) -> Result<u64, NativeError> {
        let mut f = self
            .file
            .lock()
            .map_err(|_| NativeError::Poisoned("device"))?;
        let off = f.seek(SeekFrom::End(0))?;
        f.write_all(data)?;
        f.sync_data()?;
        Ok(off)
    }

    /// Overwrites `data.len()` bytes at `offset` (reuse of a freed range). Never extends the file.
    pub fn write_at(&self, offset: u64, data: &[u8]) -> Result<(), NativeError> {
        let mut f = self
            .file
            .lock()
            .map_err(|_| NativeError::Poisoned("device"))?;
        let end = f.seek(SeekFrom::End(0))?;
        if offset + data.len() as u64 > end {
            return Err(NativeError::Invalid(format!(
                "write_at {offset}+{} past device end {end}",
                data.len()
            )));
        }
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(data)?;
        f.sync_data()?;
        Ok(())
    }

    pub fn len(&self) -> Result<u64, NativeError> {
        let f = self
            .file
            .lock()
            .map_err(|_| NativeError::Poisoned("device"))?;
        Ok(f.metadata()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, NativeError> {
        Ok(self.len()? == 0)
    }

    pub fn read_exact_at(&self, offset: u64, len: usize) -> Result<Vec<u8>, NativeError> {
        let mut f = self
            .file
            .lock()
            .map_err(|_| NativeError::Poisoned("device"))?;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }
}
