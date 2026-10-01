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

    /// Inactivity timeout before marking a client lease as Dormant (seconds).
    #[serde(default = "default_lease_timeout_secs")]
    pub lease_timeout_secs: u64,

    /// Maximum number of mutation IDs retained in the deduplication LRU cache per room.
    #[serde(default = "default_dedup_lru_capacity")]
    pub dedup_lru_capacity: usize,

    /// TTL duration for staged snapshots in the relay before eviction (seconds).
    #[serde(default = "default_snapshot_ttl_secs")]
    pub snapshot_ttl_secs: u64,
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

fn default_snapshot_ttl_secs() -> u64 {
    600
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
            dedup_lru_capacity: default_dedup_lru_capacity(),
            snapshot_ttl_secs: default_snapshot_ttl_secs(),
        }
    }
}

impl ServerConfig {
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
    }

    /// Load configuration with optional TOML file and automatic environment variable overrides.
    pub fn load_with_env(file_path: Option<impl AsRef<Path>>) -> Result<Self, ServerError> {
        let mut config = match file_path {
            Some(path) => Self::from_file(path)?,
            None => Self::default(),
        };
        config.apply_env_overrides();
        Ok(config)
    }
}
