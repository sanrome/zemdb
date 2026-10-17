pub mod actor;
pub mod api;
mod blocking;
pub mod config;
mod dedup;
mod durable;
pub mod error;
mod fail_point;
mod log;
pub mod relay;
mod response_budget;
pub mod schema_registry;

#[cfg(test)]
#[path = "tests/support.rs"]
mod test_support;

pub use actor::{
    ActorExit, ClientEntry, ClientLeaseTracker, ClientState, CommitResponse, HeartbeatResponse,
    RegisterResponse, RoomActor, RoomCommand, RoomEvent, RoomManager, RoomMetadata, RoomMetrics,
    SyncBatchResponse,
};
pub use api::{
    build_router, generate_client_token, serve_until_shutdown, verify_client_token,
    verify_client_token_bound, AdminAuth, AppState, ShutdownSignal, VerifiedClientToken,
    SHUTDOWN_GRACE_PERIOD,
};
pub use config::ServerConfig;
pub use error::ServerError;
pub use log::{RoomLifecycleOverrides, RoomLifecyclePolicy};
pub use relay::SnapshotRelay;
pub use schema_registry::SchemaRegistry;
