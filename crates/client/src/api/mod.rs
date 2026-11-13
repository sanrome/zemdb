//! The facade of the client: [`ZemdbClient`], [`RoomHandle`] and [`TableHandle`] over the
//! [`Client`](crate::Client) of the core, with rows and keys by the names of their columns.
//!
//! The facade reads the replica of a room locally, against the schema of the replica, without
//! sending anything, also as a [`ChangeStream`] of the values of a read each time they change.
//! The arguments and the results of its methods are serializable data of its own ([`Row`],
//! [`Key`], [`Scan`], [`RoomStatus`](crate::RoomStatus) and
//! [`ClientError`](crate::ClientError)); its handles and streams are not, and its handles are
//! cheap to clone.

mod client;
mod crypto;
mod room;
mod row;
mod scan;
mod table;
mod watch;
mod write;

pub use client::ZemdbClient;
pub use room::RoomHandle;
pub use row::{Key, Row};
pub use scan::{Direction, Scan};
pub use table::TableHandle;
pub use watch::ChangeStream;
