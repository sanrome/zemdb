//! The ZemDB client: one replica per room, kept with the client's durable metadata
//! ([`MetadataStore`]) and synchronized through an abstract transport ([`TransportClient`]), with
//! the tokens of the application ([`TokenProvider`]).
//!
//! The facade ([`ZemdbClient`], [`RoomHandle`] and [`TableHandle`]) reads the rows of a room by
//! the names of their columns; under it, the core ([`Client`]) is the low-level API.
//!
//! The core uses Tokio's tasks and the native file layer, so in WebAssembly the crate is empty.
#![cfg(not(target_arch = "wasm32"))]
#![forbid(unsafe_code)]

mod api;
mod client;
mod config;
mod error;
mod events;
#[cfg(feature = "native")]
mod http_transport;
mod metadata;
mod token;
mod transport;
mod worker;

pub use api::{Direction, Key, RoomHandle, Row, Scan, TableHandle, ZemdbClient};
pub use client::{Client, RoomStatus};
pub use config::{ClientConfig, Freshness};
pub use error::{ClientError, Rejection, Violation};
#[cfg(feature = "native")]
pub use http_transport::{HttpTransport, HttpTransportConfig};
pub use metadata::{FileMetadata, MemoryMetadata, MetadataStore, RoomMetadata};
pub use token::{TokenError, TokenProvider};
pub use transport::{endpoint, EventBlocks, EventStream, TransportClient, TransportError};
pub use worker::WorkerStatus;
