#![forbid(unsafe_code)]
// What panics or does not run in WebAssembly is called only from `sys`: the lint that refuses it
// elsewhere holds for all the code of the crate but its unit tests, which run on Tokio natively.
#![cfg_attr(test, allow(clippy::disallowed_methods))]

pub mod disk;
pub mod engine;
pub mod error;
pub(crate) mod fail_point;
pub mod files;
pub mod memory;
pub mod options;
pub mod snapshot;
pub mod sys;

pub mod format {
    pub use crate::disk::format::*;
}

pub use disk::{wal::WalReader, wal::WalWriter, DiskStorageEngine, DiskStorageOptions};
pub use engine::{ApplyReport, EngineConcurrencyBounds, RejectReason, RowStream, StorageEngine};
pub use error::StorageError;
pub use memory::MemoryStorageEngine;
pub use options::{KeyRange, ScanDirection, ScanOptions};
