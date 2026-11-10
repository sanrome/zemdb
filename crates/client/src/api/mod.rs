//! The facade of the client: [`ZemdbClient`], [`RoomHandle`] and [`TableHandle`] over the
//! [`Client`](crate::Client) of the core, with rows and keys by the names of their columns.
//!
//! The facade reads the replica of a room locally, against the schema of the replica, without
//! sending anything. The arguments and the results of its methods are serializable data of its
//! own ([`Row`], [`Key`], [`Scan`], [`RoomStatus`](crate::RoomStatus) and
//! [`ClientError`](crate::ClientError)); its handles are not, and are cheap to clone.

mod client;
mod room;
mod row;
mod scan;
mod table;

pub use client::ZemdbClient;
pub use room::RoomHandle;
pub use row::{Key, Row};
pub use scan::{Direction, Scan};
pub use table::TableHandle;
