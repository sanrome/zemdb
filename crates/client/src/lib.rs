//! The core of the ZemDB client: one replica per room, kept with the client's durable metadata
//! ([`MetadataStore`]) and synchronized through an abstract transport ([`TransportClient`]).
//!
//! The core uses Tokio's tasks and the native file layer, so in WebAssembly the crate is empty.
#![cfg(not(target_arch = "wasm32"))]
#![forbid(unsafe_code)]

mod client;
mod config;
mod error;
mod metadata;
mod transport;

pub use client::{Client, RoomStatus};
pub use config::ClientConfig;
pub use error::ClientError;
pub use metadata::{FileMetadata, MemoryMetadata, MetadataStore, RoomMetadata};
pub use transport::{endpoint, TransportClient, TransportError};
