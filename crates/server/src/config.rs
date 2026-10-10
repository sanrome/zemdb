use crate::error::ServerError;
use crate::log::policy::PolicySetting;
use crate::log::{RoomLifecycleOverrides, RoomLifecyclePolicy};
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Server configuration with TOML deserialization and environment variable overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host address to bind to (e.g. "127.0.0.1" or "0.0.0.0").
    #[serde(default = "default_host")]
    pub host: String,

    /// Port to listen on.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Base directory for server data (schemas, rooms, WALs).
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,

    /// Shared cluster secret for verifying client `auth_token`s.
    #[serde(default = "default_auth_secret")]
    pub auth_secret: String,

    /// Administrative Bearer token for accessing control plane endpoints.
    #[serde(default = "default_admin_secret")]
    pub admin_secret: String,

    /// Server default of the room lifecycle setting `ram_max_ops`: operations kept in RAM
    /// before the active segment is sealed.
    #[serde(default = "default_ram_max_ops")]
    pub ram_max_ops: usize,

    /// Server default: time-to-live of operations in RAM (seconds).
    #[serde(default = "default_ram_ttl_secs")]
    pub ram_ttl_secs: u64,

    /// Server default: age after which a sealed segment is compressed (seconds).
    #[serde(default = "default_warm_disk_ttl_secs")]
    pub warm_disk_ttl_secs: u64,

    /// Server default: age after which a compressed segment is pruned (seconds).
    #[serde(default = "default_cold_disk_ttl_secs")]
    pub cold_disk_ttl_secs: u64,

    /// Server default: disk space of a room's log before its oldest segments are pruned (bytes).
    #[serde(default = "default_max_room_disk_bytes")]
    pub max_room_disk_bytes: u64,

    /// Server default: inactivity timeout before an active client lease is marked
    /// Disconnected (seconds).
    #[serde(default = "default_lease_timeout_secs")]
    pub lease_timeout_secs: u64,

    /// Server default: inactivity after which a Disconnected client becomes Dormant even
    /// though its cursor is still inside the retained log (seconds). Unset by default: a
    /// Disconnected client only becomes Dormant once its cursor falls behind the log.
    #[serde(default)]
    pub dormant_after_secs: Option<u64>,

    /// Maximum number of mutation IDs retained in the deduplication LRU cache per room.
    #[serde(default = "default_dedup_lru_capacity")]
    pub dedup_lru_capacity: usize,

    /// TTL duration for staged snapshots in the relay before eviction (seconds, default 7 days).
    #[serde(default = "default_snapshot_ttl_secs")]
    pub snapshot_ttl_secs: u64,

    /// Server default: how long a room keeps asking its clients for a snapshot without the
    /// request being renewed (seconds, default 7 days).
    #[serde(default = "default_snapshot_demand_ttl_secs")]
    pub snapshot_demand_ttl_secs: u64,

    /// Server default: time without commands, SSE subscribers or snapshot uploads after which
    /// a room actor shuts down (seconds, default 10 minutes; 0 = never).
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,

    /// Largest snapshot the relay accepts, in bytes (default 512 MiB).
    #[serde(default = "default_max_snapshot_bytes")]
    pub max_snapshot_bytes: u64,

    /// Time a connection has to deliver the headers of its next request: a client that does
    /// not complete them in time, or that keeps a connection open without a request in
    /// progress for longer, is disconnected (seconds, default 10).
    #[serde(default = "default_header_read_timeout_secs")]
    pub header_read_timeout_secs: u64,

    /// Base time a request body has to arrive in full, counted from the end of its headers
    /// and extended by one second per `body_min_rate_bytes_per_sec` bytes received; a request
    /// whose body is still incomplete at the deadline fails with 408 (seconds, default 60).
    /// Responses, such as SSE streams, have no time limit.
    #[serde(default = "default_body_read_timeout_secs")]
    pub body_read_timeout_secs: u64,

    /// Minimum rate of a request body: each `body_min_rate_bytes_per_sec` bytes received
    /// extend its deadline by one second (bytes per second, default 32 KiB/s). A body that
    /// arrives at least this fast is never cut; one that trickles in slower is cut close to
    /// `body_read_timeout_secs`.
    #[serde(default = "default_body_min_rate_bytes_per_sec")]
    pub body_min_rate_bytes_per_sec: u64,

    /// Most connections served at once (default 10,000). At the limit the server stops
    /// accepting connections until an open one closes; open connections are not affected.
    /// Each connection takes a file descriptor, so the process limit of open files
    /// (`ulimit -n`) must be above this value.
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_auth_secret() -> String {
    "default_auth_secret_dev_32bytes!".to_string()
}

fn default_admin_secret() -> String {
    "default_admin_secret_dev_32bytes!".to_string()
}

fn default_ram_max_ops() -> usize {
    RoomLifecyclePolicy::default().ram_max_ops
}

fn default_ram_ttl_secs() -> u64 {
    RoomLifecyclePolicy::default().ram_ttl.as_secs()
}

fn default_warm_disk_ttl_secs() -> u64 {
    RoomLifecyclePolicy::default().warm_disk_ttl.as_secs()
}

fn default_cold_disk_ttl_secs() -> u64 {
    RoomLifecyclePolicy::default().cold_disk_ttl.as_secs()
}

fn default_max_room_disk_bytes() -> u64 {
    RoomLifecyclePolicy::default().max_room_disk_bytes
}

fn default_lease_timeout_secs() -> u64 {
    RoomLifecyclePolicy::default().lease_timeout.as_secs()
}

fn default_idle_timeout_secs() -> u64 {
    RoomLifecyclePolicy::default()
        .idle_timeout
        .map_or(0, |idle| idle.as_secs())
}

fn default_dedup_lru_capacity() -> usize {
    10_000
}

/// Seven days, in seconds.
const SEVEN_DAYS_SECS: u64 = 7 * 24 * 60 * 60;

fn default_snapshot_ttl_secs() -> u64 {
    SEVEN_DAYS_SECS
}

fn default_snapshot_demand_ttl_secs() -> u64 {
    RoomLifecyclePolicy::default().snapshot_demand_ttl.as_secs()
}

fn default_max_snapshot_bytes() -> u64 {
    512 * 1024 * 1024
}

fn default_header_read_timeout_secs() -> u64 {
    10
}

fn default_body_read_timeout_secs() -> u64 {
    60
}

fn default_body_min_rate_bytes_per_sec() -> u64 {
    32 * 1024
}

fn default_max_connections() -> usize {
    10_000
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            data_dir: default_data_dir(),
            auth_secret: default_auth_secret(),
            admin_secret: default_admin_secret(),
            ram_max_ops: default_ram_max_ops(),
            ram_ttl_secs: default_ram_ttl_secs(),
            warm_disk_ttl_secs: default_warm_disk_ttl_secs(),
            cold_disk_ttl_secs: default_cold_disk_ttl_secs(),
            max_room_disk_bytes: default_max_room_disk_bytes(),
            lease_timeout_secs: default_lease_timeout_secs(),
            dormant_after_secs: None,
            dedup_lru_capacity: default_dedup_lru_capacity(),
            snapshot_ttl_secs: default_snapshot_ttl_secs(),
            snapshot_demand_ttl_secs: default_snapshot_demand_ttl_secs(),
            idle_timeout_secs: default_idle_timeout_secs(),
            max_snapshot_bytes: default_max_snapshot_bytes(),
            header_read_timeout_secs: default_header_read_timeout_secs(),
            body_read_timeout_secs: default_body_read_timeout_secs(),
            body_min_rate_bytes_per_sec: default_body_min_rate_bytes_per_sec(),
            max_connections: default_max_connections(),
        }
    }
}

/// Smallest accepted `max_snapshot_bytes` (1 MiB).
pub const MIN_MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024;

/// Largest accepted `max_snapshot_bytes` (64 GiB).
pub const MAX_MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Accepted range of `header_read_timeout_secs`: from 1 second to 5 minutes.
pub const HEADER_READ_TIMEOUT_SECS_RANGE: std::ops::RangeInclusive<u64> = 1..=300;

/// Accepted range of `body_read_timeout_secs`: from 1 second to 1 hour.
pub const BODY_READ_TIMEOUT_SECS_RANGE: std::ops::RangeInclusive<u64> = 1..=3600;

/// Accepted range of `body_min_rate_bytes_per_sec`: from 1 KiB/s (a 16 MiB body may then take
/// about 4.5 hours) to 1 GiB/s (in practice, no extension beyond the base time).
pub const BODY_MIN_RATE_BYTES_PER_SEC_RANGE: std::ops::RangeInclusive<u64> = 1024..=1 << 30;

/// Accepted range of `max_connections`: from 1 to 1,000,000.
pub const MAX_CONNECTIONS_RANGE: std::ops::RangeInclusive<usize> = 1..=1_000_000;

/// Minimum length, in bytes, of the client token secret and the admin secret.
pub const MIN_SECRET_LEN: usize = 32;

impl ServerConfig {
    /// Checks that the configured secrets are safe to serve with.
    ///
    /// Rejects empty secrets, secrets shorter than [`MIN_SECRET_LEN`] bytes, the built-in
    /// development defaults (they are public in the source code, so anyone could mint client
    /// tokens or act as admin), and an admin secret equal to the client token secret (whoever
    /// can issue client tokens would also be admin). Error messages never include the secrets.
    pub fn validate_secrets(&self) -> Result<(), ServerError> {
        check_secret(
            "auth_secret (ZEMDB_AUTH_SECRET)",
            &self.auth_secret,
            &default_auth_secret(),
        )?;
        check_secret(
            "admin_secret (ZEMDB_ADMIN_SECRET)",
            &self.admin_secret,
            &default_admin_secret(),
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

    /// Checks that `max_snapshot_bytes` lies within
    /// [`MIN_MAX_SNAPSHOT_BYTES`]..=[`MAX_MAX_SNAPSHOT_BYTES`], that the HTTP limits lie within
    /// [`HEADER_READ_TIMEOUT_SECS_RANGE`], [`BODY_READ_TIMEOUT_SECS_RANGE`],
    /// [`BODY_MIN_RATE_BYTES_PER_SEC_RANGE`] and [`MAX_CONNECTIONS_RANGE`], and that every
    /// server default of the room lifecycle policy
    /// lies within its range (the same ranges a room's overrides must meet). The error names
    /// the setting and its environment variable.
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
        self.lifecycle_settings()
            .check(|setting| format!("{} ({})", setting.field(), setting.env_var()))
            .map_err(ServerError::Config)
    }

    /// The server defaults of the room lifecycle policy, as settings in their configured
    /// units. `dormant_after_secs` is the only one that may be unset.
    fn lifecycle_settings(&self) -> RoomLifecycleOverrides {
        RoomLifecycleOverrides {
            ram_max_ops: Some(self.ram_max_ops),
            ram_ttl_secs: Some(self.ram_ttl_secs),
            warm_disk_ttl_secs: Some(self.warm_disk_ttl_secs),
            cold_disk_ttl_secs: Some(self.cold_disk_ttl_secs),
            max_room_disk_bytes: Some(self.max_room_disk_bytes),
            lease_timeout_secs: Some(self.lease_timeout_secs),
            dormant_after_secs: self.dormant_after_secs,
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
    /// An empty variable counts as unset. A variable that is set but not valid Unicode, or
    /// that does not parse as the setting's type, is an error naming the variable: the server must not start with a setting other
    /// than the one the operator asked for. Ranges are checked afterwards by
    /// [`validate`](Self::validate).
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
    /// An empty value counts as unset, as deployment templates often expand a missing
    /// variable to an empty string.
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

        use PolicySetting::*;
        if let Some(ops) = parse_var(&var, RamMaxOps.env_var())? {
            self.ram_max_ops = ops;
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
        if let Some(secs) = parse_var(&var, SnapshotDemandTtl.env_var())? {
            self.snapshot_demand_ttl_secs = secs;
        }
        if let Some(secs) = parse_var(&var, IdleTimeout.env_var())? {
            self.idle_timeout_secs = secs;
        }
        Ok(())
    }

    /// Load configuration with optional TOML file and automatic environment variable overrides.
    pub fn load_with_env(file_path: Option<impl AsRef<Path>>) -> Result<Self, ServerError> {
        let mut config = match file_path {
            Some(path) => Self::from_file(path)?,
            None => Self::default(),
        };
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
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
