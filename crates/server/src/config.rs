use crate::error::ServerError;
use crate::log::policy::PolicySetting;
use crate::log::{RoomLifecycleOverrides, RoomLifecyclePolicy};
use serde::Deserialize;
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use zemdb_core::protocol::codec::MAX_FRAME_SIZE;

/// Server configuration with TOML deserialization and environment variable overrides.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Host address to bind to (e.g. "127.0.0.1" or "0.0.0.0").
    pub host: String,

    /// Port to listen on.
    pub port: u16,

    /// Base directory for server data (schemas, rooms, WALs).
    pub data_dir: PathBuf,

    /// Shared cluster secret for verifying client `auth_token`s.
    pub auth_secret: String,

    /// Administrative Bearer token for accessing control plane endpoints.
    pub admin_secret: String,

    /// Server default of the room lifecycle setting `ram_max_ops`: operations kept in RAM
    /// before the active segment is sealed.
    pub ram_max_ops: usize,

    /// Server default of the room lifecycle setting `ram_max_bytes`: encoded size of the
    /// operations kept in RAM.
    pub ram_max_bytes: u64,

    /// Server default of the room lifecycle setting `segment_max_bytes`: size at which the
    /// active segment is sealed.
    pub segment_max_bytes: u64,

    /// Server default: time-to-live of operations in RAM (seconds).
    pub ram_ttl_secs: u64,

    /// Server default: age after which a sealed segment is compressed (seconds).
    pub warm_disk_ttl_secs: u64,

    /// Server default: age after which a compressed segment is pruned (seconds).
    pub cold_disk_ttl_secs: u64,

    /// Server default: disk space of a room's log before its oldest segments are pruned (bytes).
    pub max_room_disk_bytes: u64,

    /// Server default: inactivity timeout before an active client lease is marked
    /// Disconnected (seconds).
    pub lease_timeout_secs: u64,

    /// Server default: inactivity after which a Disconnected client becomes Dormant even
    /// though its cursor is still inside the retained log (seconds). Unset by default: a
    /// Disconnected client only becomes Dormant once its cursor falls behind the log.
    pub dormant_after_secs: Option<u64>,

    /// Server default: inactivity after which a Dormant client is removed from the roster
    /// (seconds, default 30 days).
    pub forget_dormant_after_secs: u64,

    /// Maximum number of mutation IDs retained in the deduplication cache per room.
    pub dedup_lru_capacity: usize,

    /// TTL duration for staged snapshots in the relay before eviction (seconds, default 7 days).
    pub snapshot_ttl_secs: u64,

    /// Server default: how long a room keeps asking its clients for a snapshot without the
    /// request being renewed (seconds, default 7 days).
    pub snapshot_demand_ttl_secs: u64,

    /// Server default: time without commands, SSE subscribers or snapshot uploads after which
    /// a room actor shuts down (seconds, default 10 minutes; 0 = never).
    pub idle_timeout_secs: u64,

    /// Largest snapshot the relay accepts, in bytes (default 512 MiB).
    pub max_snapshot_bytes: u64,

    /// Time a connection has to deliver the headers of its next request: a client that does
    /// not complete them in time, or that keeps a connection open without a request in
    /// progress for longer, is disconnected (seconds, default 10).
    pub header_read_timeout_secs: u64,

    /// Base time a request body has to arrive in full, counted from the end of its headers
    /// and extended by one second per `body_min_rate_bytes_per_sec` bytes received; a request
    /// whose body is still incomplete at the deadline fails with 408 (seconds, default 60).
    pub body_read_timeout_secs: u64,

    /// Minimum body rate that extends the deadline (bytes per second, default 32 KiB/s): a body
    /// that arrives at least this fast is never cut. See `body_read_timeout_secs`.
    pub body_min_rate_bytes_per_sec: u64,

    /// Most connections served at once (default 10,000). At the limit the server stops
    /// accepting connections until an open one closes; open connections are not affected.
    /// Each connection takes a file descriptor, so the process limit of open files
    /// (`ulimit -n`) must be above this value.
    pub max_connections: usize,

    /// Longest a response may wait for the client to take its next frame (at most 64 KiB), or a
    /// socket write for the client to accept bytes, before the connection is closed (seconds,
    /// default 60). A response whose body has nothing to send, such as an SSE stream without
    /// events, is not waiting for the client.
    pub response_stall_timeout_secs: u64,

    /// Bytes of large responses (`SyncBatch`, `CommitAck`, `SnapshotChunk`) held at once until
    /// their clients take them (default 256 MiB). Without room left, a sync or a snapshot
    /// chunk is answered `Unavailable`, and a commit is acknowledged without its catch-up.
    pub max_inflight_response_bytes: u64,

    /// Most SSE streams open at once (default: half of `max_connections`, at least 1). A
    /// client keeps at most 2 per room: opening a third closes its oldest.
    pub max_sse_streams: Option<usize>,
}

/// Built-in development secrets. They are public, so `validate_secrets` rejects them.
const DEV_AUTH_SECRET: &str = "default_auth_secret_dev_32bytes!";
const DEV_ADMIN_SECRET: &str = "default_admin_secret_dev_32bytes!";

impl Default for ServerConfig {
    fn default() -> Self {
        let policy = RoomLifecyclePolicy::default();
        Self {
            host: "127.0.0.1".to_string(),
            port: 8080,
            data_dir: PathBuf::from("./data"),
            auth_secret: DEV_AUTH_SECRET.to_string(),
            admin_secret: DEV_ADMIN_SECRET.to_string(),
            ram_max_ops: policy.ram_max_ops,
            ram_max_bytes: policy.ram_max_bytes,
            segment_max_bytes: policy.segment_max_bytes,
            ram_ttl_secs: policy.ram_ttl.as_secs(),
            warm_disk_ttl_secs: policy.warm_disk_ttl.as_secs(),
            cold_disk_ttl_secs: policy.cold_disk_ttl.as_secs(),
            max_room_disk_bytes: policy.max_room_disk_bytes,
            lease_timeout_secs: policy.lease_timeout.as_secs(),
            dormant_after_secs: None,
            forget_dormant_after_secs: policy.forget_dormant_after.as_secs(),
            dedup_lru_capacity: 10_000,
            snapshot_ttl_secs: 7 * 24 * 60 * 60,
            snapshot_demand_ttl_secs: policy.snapshot_demand_ttl.as_secs(),
            idle_timeout_secs: policy.idle_timeout.map_or(0, |idle| idle.as_secs()),
            max_snapshot_bytes: 512 * 1024 * 1024,
            header_read_timeout_secs: 10,
            body_read_timeout_secs: 60,
            body_min_rate_bytes_per_sec: 32 * 1024,
            max_connections: 10_000,
            response_stall_timeout_secs: 60,
            max_inflight_response_bytes: 256 * 1024 * 1024,
            max_sse_streams: None,
        }
    }
}

/// Smallest accepted `max_snapshot_bytes` (1 MiB).
const MIN_MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024;

/// Largest accepted `max_snapshot_bytes` (64 GiB).
const MAX_MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Accepted range of `header_read_timeout_secs`: from 1 second to 5 minutes.
const HEADER_READ_TIMEOUT_SECS_RANGE: std::ops::RangeInclusive<u64> = 1..=300;

/// Accepted range of `body_read_timeout_secs`: from 1 second to 1 hour.
const BODY_READ_TIMEOUT_SECS_RANGE: std::ops::RangeInclusive<u64> = 1..=3600;

/// Accepted range of `body_min_rate_bytes_per_sec`: from 1 KiB/s (a 16 MiB body may then take
/// about 4.5 hours) to 1 GiB/s (in practice, no extension beyond the base time).
const BODY_MIN_RATE_BYTES_PER_SEC_RANGE: std::ops::RangeInclusive<u64> = 1024..=1 << 30;

/// Accepted range of `max_connections`: from 1 to 1,000,000.
const MAX_CONNECTIONS_RANGE: std::ops::RangeInclusive<usize> = 1..=1_000_000;

/// Accepted range of `response_stall_timeout_secs`: from 5 seconds to 1 hour.
const RESPONSE_STALL_TIMEOUT_SECS_RANGE: std::ops::RangeInclusive<u64> = 5..=3600;

/// Accepted range of `max_inflight_response_bytes`: from the largest frame, so that any response
/// fits, to 64 GiB.
const MAX_INFLIGHT_RESPONSE_BYTES_RANGE: std::ops::RangeInclusive<u64> =
    MAX_FRAME_SIZE as u64..=64 * 1024 * 1024 * 1024;

/// Minimum length, in bytes, of the client token secret and the admin secret.
const MIN_SECRET_LEN: usize = 32;

impl ServerConfig {
    /// Checks that the configured secrets are safe to serve with.
    ///
    /// Rejects empty secrets, secrets shorter than 32 bytes, the built-in development defaults
    /// (they are public in the source code, so anyone could mint client tokens or act as admin),
    /// and an admin secret equal to the client token secret (whoever can issue client tokens
    /// would also be admin). Error messages never include the secrets.
    pub fn validate_secrets(&self) -> Result<(), ServerError> {
        check_secret(
            "auth_secret (ZEMDB_AUTH_SECRET)",
            &self.auth_secret,
            DEV_AUTH_SECRET,
        )?;
        check_secret(
            "admin_secret (ZEMDB_ADMIN_SECRET)",
            &self.admin_secret,
            DEV_ADMIN_SECRET,
        )?;
        if self.auth_secret == self.admin_secret {
            return Err(ServerError::Config(
                "auth_secret and admin_secret must be different".to_string(),
            ));
        }
        Ok(())
    }

    /// Checks every setting the server cannot run safely with: the secrets (see
    /// [`validate_secrets`](Self::validate_secrets)) and the limits.
    pub fn validate(&self) -> Result<(), ServerError> {
        self.validate_secrets()?;
        self.validate_limits()
    }

    /// Checks that `max_snapshot_bytes`, the HTTP limits and every server default of the room
    /// lifecycle policy lie within their ranges (the constants of this module, and the ranges a
    /// room's overrides must meet). The error names the setting and its environment variable.
    pub fn validate_limits(&self) -> Result<(), ServerError> {
        if !(MIN_MAX_SNAPSHOT_BYTES..=MAX_MAX_SNAPSHOT_BYTES).contains(&self.max_snapshot_bytes) {
            return Err(ServerError::Config(format!(
                "max_snapshot_bytes (ZEMDB_MAX_SNAPSHOT_BYTES) must be between {MIN_MAX_SNAPSHOT_BYTES} \
                 (1 MiB) and {MAX_MAX_SNAPSHOT_BYTES} (64 GiB), got {}",
                self.max_snapshot_bytes
            )));
        }
        check_range(
            "header_read_timeout_secs (ZEMDB_HEADER_READ_TIMEOUT_SECS)",
            self.header_read_timeout_secs,
            HEADER_READ_TIMEOUT_SECS_RANGE,
        )?;
        check_range(
            "body_read_timeout_secs (ZEMDB_BODY_READ_TIMEOUT_SECS)",
            self.body_read_timeout_secs,
            BODY_READ_TIMEOUT_SECS_RANGE,
        )?;
        check_range(
            "body_min_rate_bytes_per_sec (ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC)",
            self.body_min_rate_bytes_per_sec,
            BODY_MIN_RATE_BYTES_PER_SEC_RANGE,
        )?;
        check_range(
            "max_connections (ZEMDB_MAX_CONNECTIONS)",
            self.max_connections,
            MAX_CONNECTIONS_RANGE,
        )?;
        check_range(
            "response_stall_timeout_secs (ZEMDB_RESPONSE_STALL_TIMEOUT_SECS)",
            self.response_stall_timeout_secs,
            RESPONSE_STALL_TIMEOUT_SECS_RANGE,
        )?;
        check_range(
            "max_inflight_response_bytes (ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES)",
            self.max_inflight_response_bytes,
            MAX_INFLIGHT_RESPONSE_BYTES_RANGE,
        )?;
        if let Some(max) = self.max_sse_streams {
            check_range(
                "max_sse_streams (ZEMDB_MAX_SSE_STREAMS)",
                max,
                1..=self.max_connections,
            )?;
        }
        self.lifecycle_settings()
            .check(|setting| format!("{} ({})", setting.field(), setting.env_var()))
            .map_err(ServerError::Config)
    }

    /// Most SSE streams open at once: `max_sse_streams`, or half of `max_connections`.
    pub(crate) fn sse_stream_limit(&self) -> usize {
        self.max_sse_streams
            .unwrap_or((self.max_connections / 2).max(1))
    }

    /// The server defaults of the room lifecycle policy, as settings in their configured
    /// units. `dormant_after_secs` is the only one that may be unset.
    fn lifecycle_settings(&self) -> RoomLifecycleOverrides {
        RoomLifecycleOverrides {
            ram_max_ops: Some(self.ram_max_ops),
            ram_max_bytes: Some(self.ram_max_bytes),
            segment_max_bytes: Some(self.segment_max_bytes),
            ram_ttl_secs: Some(self.ram_ttl_secs),
            warm_disk_ttl_secs: Some(self.warm_disk_ttl_secs),
            cold_disk_ttl_secs: Some(self.cold_disk_ttl_secs),
            max_room_disk_bytes: Some(self.max_room_disk_bytes),
            lease_timeout_secs: Some(self.lease_timeout_secs),
            dormant_after_secs: self.dormant_after_secs,
            forget_dormant_after_secs: Some(self.forget_dormant_after_secs),
            snapshot_demand_ttl_secs: Some(self.snapshot_demand_ttl_secs),
            idle_timeout_secs: Some(self.idle_timeout_secs),
        }
    }

    /// The lifecycle policy of a room without overrides: the server defaults configured here.
    pub fn default_lifecycle_policy(&self) -> RoomLifecyclePolicy {
        // Every setting is present except `dormant_after_secs`, whose absence means unset, so
        // the base must not carry a dormancy of its own.
        RoomLifecyclePolicy {
            dormant_after: None,
            ..RoomLifecyclePolicy::default()
        }
        .with_overrides(&self.lifecycle_settings())
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml_str(toml_str: &str) -> Result<Self, ServerError> {
        toml::from_str(toml_str).map_err(|e| ServerError::Config(e.to_string()))
    }

    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ServerError> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| ServerError::Config(format!("Failed to read config file: {}", e)))?;
        Self::from_toml_str(&content)
    }

    /// Overrides configuration values from environment variables if present.
    ///
    /// An empty variable counts as unset, as deployment templates often expand a missing
    /// variable to an empty string. A variable that is set but not valid Unicode, or that does
    /// not parse as the setting's type, is an error naming the variable: the server must not
    /// start with a setting other than the one the operator asked for. Ranges are checked
    /// afterwards by [`validate`](Self::validate).
    pub fn apply_env_overrides(&mut self) -> Result<(), ServerError> {
        self.apply_overrides_from(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(ServerError::Config(format!("{name} is not valid Unicode")))
            }
        })
    }

    /// Applies the overrides found by `var`, which returns the value of a variable, if set.
    fn apply_overrides_from(
        &mut self,
        var: impl Fn(&str) -> Result<Option<String>, ServerError>,
    ) -> Result<(), ServerError> {
        let var = |name: &str| Ok(var(name)?.filter(|value| !value.is_empty()));
        if let Some(host) = var("ZEMDB_HOST")? {
            self.host = host;
        }
        if let Some(port) = parse_var(&var, "ZEMDB_PORT")? {
            self.port = port;
        }
        if let Some(data_dir) = var("ZEMDB_DATA_DIR")? {
            self.data_dir = PathBuf::from(data_dir);
        }
        if let Some(auth_secret) = var("ZEMDB_AUTH_SECRET")? {
            self.auth_secret = auth_secret;
        }
        if let Some(admin_secret) = var("ZEMDB_ADMIN_SECRET")? {
            self.admin_secret = admin_secret;
        }
        if let Some(cap) = parse_var(&var, "ZEMDB_DEDUP_LRU_CAPACITY")? {
            self.dedup_lru_capacity = cap;
        }
        if let Some(snapshot_ttl) = parse_var(&var, "ZEMDB_SNAPSHOT_TTL_SECS")? {
            self.snapshot_ttl_secs = snapshot_ttl;
        }
        if let Some(max_bytes) = parse_var(&var, "ZEMDB_MAX_SNAPSHOT_BYTES")? {
            self.max_snapshot_bytes = max_bytes;
        }
        if let Some(secs) = parse_var(&var, "ZEMDB_HEADER_READ_TIMEOUT_SECS")? {
            self.header_read_timeout_secs = secs;
        }
        if let Some(secs) = parse_var(&var, "ZEMDB_BODY_READ_TIMEOUT_SECS")? {
            self.body_read_timeout_secs = secs;
        }
        if let Some(rate) = parse_var(&var, "ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC")? {
            self.body_min_rate_bytes_per_sec = rate;
        }
        if let Some(max) = parse_var(&var, "ZEMDB_MAX_CONNECTIONS")? {
            self.max_connections = max;
        }
        if let Some(secs) = parse_var(&var, "ZEMDB_RESPONSE_STALL_TIMEOUT_SECS")? {
            self.response_stall_timeout_secs = secs;
        }
        if let Some(bytes) = parse_var(&var, "ZEMDB_MAX_INFLIGHT_RESPONSE_BYTES")? {
            self.max_inflight_response_bytes = bytes;
        }
        if let Some(max) = parse_var(&var, "ZEMDB_MAX_SSE_STREAMS")? {
            self.max_sse_streams = Some(max);
        }

        use PolicySetting::*;
        if let Some(ops) = parse_var(&var, RamMaxOps.env_var())? {
            self.ram_max_ops = ops;
        }
        if let Some(bytes) = parse_var(&var, RamMaxBytes.env_var())? {
            self.ram_max_bytes = bytes;
        }
        if let Some(bytes) = parse_var(&var, SegmentMaxBytes.env_var())? {
            self.segment_max_bytes = bytes;
        }
        if let Some(secs) = parse_var(&var, RamTtl.env_var())? {
            self.ram_ttl_secs = secs;
        }
        if let Some(secs) = parse_var(&var, WarmDiskTtl.env_var())? {
            self.warm_disk_ttl_secs = secs;
        }
        if let Some(secs) = parse_var(&var, ColdDiskTtl.env_var())? {
            self.cold_disk_ttl_secs = secs;
        }
        if let Some(bytes) = parse_var(&var, MaxRoomDiskBytes.env_var())? {
            self.max_room_disk_bytes = bytes;
        }
        if let Some(secs) = parse_var(&var, LeaseTimeout.env_var())? {
            self.lease_timeout_secs = secs;
        }
        if let Some(secs) = parse_var(&var, DormantAfter.env_var())? {
            self.dormant_after_secs = Some(secs);
        }
        if let Some(secs) = parse_var(&var, ForgetDormantAfter.env_var())? {
            self.forget_dormant_after_secs = secs;
        }
        if let Some(secs) = parse_var(&var, SnapshotDemandTtl.env_var())? {
            self.snapshot_demand_ttl_secs = secs;
        }
        if let Some(secs) = parse_var(&var, IdleTimeout.env_var())? {
            self.idle_timeout_secs = secs;
        }
        Ok(())
    }

    /// Loads the TOML file (or the defaults), applies the environment overrides and validates
    /// the result.
    pub fn load_with_env(file_path: Option<impl AsRef<Path>>) -> Result<Self, ServerError> {
        let mut config = match file_path {
            Some(path) => Self::from_file(path)?,
            None => Self::default(),
        };
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }

    /// Takes the exclusive lock of `data_dir` (`server.lock`, created with the directory if
    /// needed), which keeps a second server off the same files, and syncs `data_dir`, whose
    /// entries an earlier run may have created without a successful sync. A server takes it
    /// before touching any other file and holds the returned handle until it exits.
    pub fn lock_data_dir(&self) -> Result<std::fs::File, ServerError> {
        crate::durable::create_dir_all_synced(&self.data_dir)?;
        let lock = crate::durable::lock_exclusive(&self.data_dir.join("server.lock")).map_err(
            |e| match e.kind() {
                std::io::ErrorKind::WouldBlock => ServerError::Config(format!(
                    "data_dir {:?} is in use by another server process: {e}",
                    self.data_dir
                )),
                _ => e.into(),
            },
        )?;
        crate::durable::sync_dir(&self.data_dir)?;
        Ok(lock)
    }
}

/// Reads the variable `name` through `var` and parses it as `T`. An unparsable value is an
/// error naming the variable; the value is only echoed for numeric settings, never secrets.
fn parse_var<T>(
    var: &impl Fn(&str) -> Result<Option<String>, ServerError>,
    name: &str,
) -> Result<Option<T>, ServerError>
where
    T: FromStr,
    T::Err: Display,
{
    match var(name)? {
        None => Ok(None),
        Some(raw) => raw.parse().map(Some).map_err(|err| {
            ServerError::Config(format!("{name} has an invalid value {raw:?}: {err}"))
        }),
    }
}

/// Checks that `value` lies within `range`; the error names the setting.
fn check_range<T>(
    name: &str,
    value: T,
    range: std::ops::RangeInclusive<T>,
) -> Result<(), ServerError>
where
    T: PartialOrd + Display,
{
    if range.contains(&value) {
        return Ok(());
    }
    Err(ServerError::Config(format!(
        "{name} must be between {} and {}, got {value}",
        range.start(),
        range.end()
    )))
}

fn check_secret(name: &str, value: &str, development_default: &str) -> Result<(), ServerError> {
    if value.is_empty() {
        return Err(ServerError::Config(format!("{name} must be set")));
    }
    if value == development_default {
        return Err(ServerError::Config(format!(
            "{name} is the built-in development value, which is public; set a private secret"
        )));
    }
    if value.len() < MIN_SECRET_LEN {
        return Err(ServerError::Config(format!(
            "{name} must be at least {MIN_SECRET_LEN} bytes long"
        )));
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
