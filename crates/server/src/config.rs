use crate::error::ServerError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

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

    /// Inactivity timeout before an active client lease is marked Disconnected (seconds).
    #[serde(default = "default_lease_timeout_secs")]
    pub lease_timeout_secs: u64,

    /// Inactivity after which a Disconnected client becomes Dormant even though its cursor is
    /// still inside the retained log (seconds). Unset by default: a Disconnected client only
    /// becomes Dormant once its cursor falls behind the log.
    #[serde(default)]
    pub dormant_after_secs: Option<u64>,

    /// Maximum number of mutation IDs retained in the deduplication LRU cache per room.
    #[serde(default = "default_dedup_lru_capacity")]
    pub dedup_lru_capacity: usize,

    /// TTL duration for staged snapshots in the relay before eviction (seconds, default 7 days).
    #[serde(default = "default_snapshot_ttl_secs")]
    pub snapshot_ttl_secs: u64,

    /// How long a room keeps asking its clients for a snapshot without the request being
    /// renewed (seconds, default 7 days).
    #[serde(default = "default_snapshot_demand_ttl_secs")]
    pub snapshot_demand_ttl_secs: u64,

    /// Largest snapshot the relay accepts, in bytes (default 512 MiB).
    #[serde(default = "default_max_snapshot_bytes")]
    pub max_snapshot_bytes: u64,
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

fn default_lease_timeout_secs() -> u64 {
    90
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
    SEVEN_DAYS_SECS
}

fn default_max_snapshot_bytes() -> u64 {
    512 * 1024 * 1024
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            data_dir: default_data_dir(),
            auth_secret: default_auth_secret(),
            admin_secret: default_admin_secret(),
            lease_timeout_secs: default_lease_timeout_secs(),
            dormant_after_secs: None,
            dedup_lru_capacity: default_dedup_lru_capacity(),
            snapshot_ttl_secs: default_snapshot_ttl_secs(),
            snapshot_demand_ttl_secs: default_snapshot_demand_ttl_secs(),
            max_snapshot_bytes: default_max_snapshot_bytes(),
        }
    }
}

/// Smallest accepted `max_snapshot_bytes` (1 MiB).
pub const MIN_MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024;

/// Largest accepted `max_snapshot_bytes` (64 GiB).
pub const MAX_MAX_SNAPSHOT_BYTES: u64 = 64 * 1024 * 1024 * 1024;

/// Smallest accepted `snapshot_demand_ttl_secs`.
pub const MIN_SNAPSHOT_DEMAND_TTL_SECS: u64 = 60;

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
    /// [`MIN_MAX_SNAPSHOT_BYTES`]..=[`MAX_MAX_SNAPSHOT_BYTES`], that `snapshot_demand_ttl_secs`
    /// is at least [`MIN_SNAPSHOT_DEMAND_TTL_SECS`] and that `dormant_after_secs`, if set, is
    /// not 0.
    pub fn validate_limits(&self) -> Result<(), ServerError> {
        if !(MIN_MAX_SNAPSHOT_BYTES..=MAX_MAX_SNAPSHOT_BYTES).contains(&self.max_snapshot_bytes) {
            return Err(ServerError::Config(format!(
                "max_snapshot_bytes (ZEMDB_MAX_SNAPSHOT_BYTES) must be between {MIN_MAX_SNAPSHOT_BYTES} \
                 (1 MiB) and {MAX_MAX_SNAPSHOT_BYTES} (64 GiB), got {}",
                self.max_snapshot_bytes
            )));
        }
        // A demand that expires within a few ticks would turn on and off with every request.
        if self.snapshot_demand_ttl_secs < MIN_SNAPSHOT_DEMAND_TTL_SECS {
            return Err(ServerError::Config(format!(
                "snapshot_demand_ttl_secs (ZEMDB_SNAPSHOT_DEMAND_TTL_SECS) must be at least \
                 {MIN_SNAPSHOT_DEMAND_TTL_SECS}, got {}",
                self.snapshot_demand_ttl_secs
            )));
        }
        if self.dormant_after_secs == Some(0) {
            return Err(ServerError::Config(
                "dormant_after_secs (ZEMDB_DORMANT_AFTER_SECS) must be greater than 0 when set"
                    .to_string(),
            ));
        }
        Ok(())
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
    pub fn apply_env_overrides(&mut self) {
        if let Ok(host) = std::env::var("ZEMDB_HOST") {
            self.host = host;
        }
        if let Ok(port_str) = std::env::var("ZEMDB_PORT") {
            if let Ok(port) = port_str.parse::<u16>() {
                self.port = port;
            }
        }
        if let Ok(data_dir) = std::env::var("ZEMDB_DATA_DIR") {
            self.data_dir = PathBuf::from(data_dir);
        }
        if let Ok(auth_sec) = std::env::var("ZEMDB_AUTH_SECRET") {
            self.auth_secret = auth_sec;
        }
        if let Ok(admin_sec) = std::env::var("ZEMDB_ADMIN_SECRET") {
            self.admin_secret = admin_sec;
        }
        if let Ok(lease_str) = std::env::var("ZEMDB_LEASE_TIMEOUT_SECS") {
            if let Ok(lease) = lease_str.parse::<u64>() {
                self.lease_timeout_secs = lease;
            }
        }
        if let Ok(dormant_str) = std::env::var("ZEMDB_DORMANT_AFTER_SECS") {
            if let Ok(dormant_after) = dormant_str.parse::<u64>() {
                self.dormant_after_secs = Some(dormant_after);
            }
        }
        if let Ok(lru_str) = std::env::var("ZEMDB_DEDUP_LRU_CAPACITY") {
            if let Ok(cap) = lru_str.parse::<usize>() {
                self.dedup_lru_capacity = cap;
            }
        }
        if let Ok(snap_str) = std::env::var("ZEMDB_SNAPSHOT_TTL_SECS") {
            if let Ok(snap_ttl) = snap_str.parse::<u64>() {
                self.snapshot_ttl_secs = snap_ttl;
            }
        }
        if let Ok(demand_str) = std::env::var("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS") {
            if let Ok(demand_ttl) = demand_str.parse::<u64>() {
                self.snapshot_demand_ttl_secs = demand_ttl;
            }
        }
        if let Ok(max_str) = std::env::var("ZEMDB_MAX_SNAPSHOT_BYTES") {
            if let Ok(max_bytes) = max_str.parse::<u64>() {
                self.max_snapshot_bytes = max_bytes;
            }
        }
    }

    /// Load configuration with optional TOML file and automatic environment variable overrides.
    pub fn load_with_env(file_path: Option<impl AsRef<Path>>) -> Result<Self, ServerError> {
        let mut config = match file_path {
            Some(path) => Self::from_file(path)?,
            None => Self::default(),
        };
        config.apply_env_overrides();
        config.validate()?;
        Ok(config)
    }
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
