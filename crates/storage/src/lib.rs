#![forbid(unsafe_code)]

pub mod engine;
pub mod error;
pub(crate) mod fail_point;
pub mod memory;
pub mod options;
pub mod snapshot;
pub mod sys;

#[cfg(not(target_arch = "wasm32"))]
pub mod disk;

#[cfg(not(target_arch = "wasm32"))]
pub mod format {
    pub use crate::disk::format::*;
}

pub use engine::{EngineConcurrencyBounds, RowStream, StorageEngine};
pub use error::StorageError;
pub use memory::MemoryStorageEngine;
pub use options::{KeyRange, ScanDirection, ScanOptions};

#[cfg(not(target_arch = "wasm32"))]
pub use disk::{wal::WalReader, wal::WalWriter, DiskStorageEngine, DiskStorageOptions};
