use super::*;
use crate::log::policy::MAX_POLICY_DURATION_SECS;
use std::time::Duration;

fn config_with(auth: &str, admin: &str) -> ServerConfig {
    ServerConfig {
        auth_secret: auth.to_string(),
        admin_secret: admin.to_string(),
        ..ServerConfig::default()
    }
}

const STRONG_AUTH: &str = "9f2c7e1a5b8d3f6e0a4c7b1d9e2f5a8c";
const STRONG_ADMIN: &str = "3b6e9a2d5c8f1e4a7d0b3c6f9e2a5d8b";

#[test]
fn built_in_development_secrets_are_rejected() {
    assert!(ServerConfig::default().validate_secrets().is_err());
    assert!(config_with(STRONG_AUTH, DEV_ADMIN_SECRET)
        .validate_secrets()
        .is_err());
    assert!(config_with(DEV_AUTH_SECRET, STRONG_ADMIN)
        .validate_secrets()
        .is_err());
}

#[test]
fn empty_and_short_secrets_are_rejected() {
    assert!(config_with("", STRONG_ADMIN).validate_secrets().is_err());
    assert!(config_with(STRONG_AUTH, "").validate_secrets().is_err());
    let short = "x".repeat(MIN_SECRET_LEN - 1);
    assert!(config_with(&short, STRONG_ADMIN)
        .validate_secrets()
        .is_err());
    assert!(config_with(STRONG_AUTH, &short).validate_secrets().is_err());
}

#[test]
fn identical_auth_and_admin_secrets_are_rejected() {
    assert!(config_with(STRONG_AUTH, STRONG_AUTH)
        .validate_secrets()
        .is_err());
}

#[test]
fn strong_distinct_secrets_are_accepted() {
    assert!(config_with(STRONG_AUTH, STRONG_ADMIN)
        .validate_secrets()
        .is_ok());
}

#[test]
fn rejection_does_not_echo_the_secret() {
    let short = "short-secret-value";
    let err = config_with(short, STRONG_ADMIN)
        .validate_secrets()
        .unwrap_err()
        .to_string();
    assert!(!err.contains(short), "error leaks the secret: {err}");
}

fn load_with_max_snapshot_bytes(max: u64) -> Result<ServerConfig, ServerError> {
    let config = ServerConfig::from_toml_str(&format!(
        "auth_secret = \"{STRONG_AUTH}\"\nadmin_secret = \"{STRONG_ADMIN}\"\nmax_snapshot_bytes = {max}\n"
    ))?;
    config.validate()?;
    Ok(config)
}

#[test]
fn max_snapshot_bytes_outside_its_range_fails_to_load() {
    for max in [0, 1024 * 1024 - 1, 64 * 1024 * 1024 * 1024 + 1] {
        let err = load_with_max_snapshot_bytes(max).unwrap_err().to_string();
        assert!(err.contains("max_snapshot_bytes"), "{max}: {err}");
    }
    for max in [1024 * 1024, 512 * 1024 * 1024, 64 * 1024 * 1024 * 1024] {
        assert_eq!(
            load_with_max_snapshot_bytes(max)
                .unwrap()
                .max_snapshot_bytes,
            max
        );
    }
}

#[test]
fn lifecycle_and_snapshot_defaults() {
    let config = ServerConfig::default();
    assert_eq!(config.dormant_after_secs, None);
    assert_eq!(config.snapshot_ttl_secs, 7 * 24 * 60 * 60);
    assert_eq!(config.snapshot_demand_ttl_secs, 7 * 24 * 60 * 60);

    let parsed = ServerConfig::from_toml_str("").unwrap();
    assert_eq!(parsed.dormant_after_secs, None);
    assert_eq!(parsed.snapshot_demand_ttl_secs, 7 * 24 * 60 * 60);

    let parsed =
        ServerConfig::from_toml_str("dormant_after_secs = 3600\nsnapshot_demand_ttl_secs = 120\n")
            .unwrap();
    assert_eq!(parsed.dormant_after_secs, Some(3600));
    assert_eq!(parsed.snapshot_demand_ttl_secs, 120);
}

fn strong_config() -> ServerConfig {
    config_with(STRONG_AUTH, STRONG_ADMIN)
}

#[test]
fn snapshot_demand_ttl_below_a_minute_is_rejected() {
    for ttl in [0, 1, 59] {
        let err = ServerConfig {
            snapshot_demand_ttl_secs: ttl,
            ..strong_config()
        }
        .validate()
        .unwrap_err()
        .to_string();
        assert!(
            err.contains("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS"),
            "{ttl}: {err}"
        );
    }
    for ttl in [60, 7 * 24 * 60 * 60] {
        ServerConfig {
            snapshot_demand_ttl_secs: ttl,
            ..strong_config()
        }
        .validate()
        .unwrap();
    }
}

#[test]
fn zero_dormancy_timeout_is_rejected() {
    let err = ServerConfig {
        dormant_after_secs: Some(0),
        ..strong_config()
    }
    .validate()
    .unwrap_err()
    .to_string();
    assert!(err.contains("ZEMDB_DORMANT_AFTER_SECS"), "{err}");

    for dormant_after in [None, Some(1), Some(3600)] {
        ServerConfig {
            dormant_after_secs: dormant_after,
            ..strong_config()
        }
        .validate()
        .unwrap();
    }
}

/// Applies the environment `vars` to the default configuration.
fn with_env(vars: &[(&str, &str)]) -> Result<ServerConfig, ServerError> {
    let vars: std::collections::HashMap<String, String> = vars
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    let mut config = strong_config();
    config.apply_overrides_from(|name| Ok(vars.get(name).cloned()))?;
    Ok(config)
}

#[test]
fn unparsable_environment_values_fail_naming_the_variable() {
    for (name, value) in [
        ("ZEMDB_PORT", "eighty"),
        ("ZEMDB_PORT", "70000"),
        ("ZEMDB_DEDUP_LRU_CAPACITY", "-1"),
        ("ZEMDB_SNAPSHOT_TTL_SECS", "1h"),
        ("ZEMDB_MAX_SNAPSHOT_BYTES", "1e9"),
        ("ZEMDB_RAM_MAX_OPS", "many"),
        ("ZEMDB_RAM_TTL_SECS", "0x10"),
        ("ZEMDB_WARM_TTL_SECS", "1.5"),
        ("ZEMDB_COLD_TTL_SECS", "-30"),
        ("ZEMDB_ROOM_MAX_DISK_BYTES", "500MB"),
        ("ZEMDB_LEASE_TIMEOUT_SECS", " 90"),
        ("ZEMDB_DORMANT_AFTER_SECS", "never"),
        ("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS", "7d"),
        ("ZEMDB_ROOM_IDLE_TIMEOUT_SECS", "off"),
        ("ZEMDB_HEADER_READ_TIMEOUT_SECS", "10s"),
        ("ZEMDB_BODY_READ_TIMEOUT_SECS", "-1"),
        ("ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC", "32KiB"),
        ("ZEMDB_MAX_CONNECTIONS", "10k"),
    ] {
        let err = with_env(&[(name, value)]).unwrap_err();
        assert!(
            matches!(&err, ServerError::Config(msg) if msg.contains(name)),
            "{name}={value:?}: {err:?}"
        );
    }
}

#[test]
fn lifecycle_environment_variables_set_the_server_defaults() {
    let config = with_env(&[
        ("ZEMDB_RAM_MAX_OPS", "250"),
        ("ZEMDB_RAM_MAX_BYTES", "2097152"),
        ("ZEMDB_SEGMENT_MAX_BYTES", "4194304"),
        ("ZEMDB_RAM_TTL_SECS", "30"),
        ("ZEMDB_WARM_TTL_SECS", "0"),
        ("ZEMDB_COLD_TTL_SECS", "86400"),
        ("ZEMDB_ROOM_MAX_DISK_BYTES", "2097152"),
        ("ZEMDB_LEASE_TIMEOUT_SECS", "15"),
        ("ZEMDB_DORMANT_AFTER_SECS", "3600"),
        ("ZEMDB_FORGET_DORMANT_AFTER_SECS", "7200"),
        ("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS", "600"),
        ("ZEMDB_ROOM_IDLE_TIMEOUT_SECS", "0"),
    ])
    .unwrap();
    config.validate().unwrap();
    let secs = std::time::Duration::from_secs;
    assert_eq!(
        config.default_lifecycle_policy(),
        RoomLifecyclePolicy {
            ram_max_ops: 250,
            ram_max_bytes: 2 * 1024 * 1024,
            segment_max_bytes: 4 * 1024 * 1024,
            ram_ttl: secs(30),
            warm_disk_ttl: secs(0),
            cold_disk_ttl: secs(86_400),
            max_room_disk_bytes: 2 * 1024 * 1024,
            lease_timeout: secs(15),
            dormant_after: Some(secs(3600)),
            forget_dormant_after: secs(7200),
            snapshot_demand_ttl: secs(600),
            idle_timeout: None,
        }
    );
}

#[test]
fn default_configuration_gives_the_default_policy() {
    let config = ServerConfig::default();
    config.validate_limits().unwrap();
    assert_eq!(
        config.default_lifecycle_policy(),
        RoomLifecyclePolicy::default()
    );
    assert_eq!(config.idle_timeout_secs, 600);
}

#[test]
fn lifecycle_defaults_outside_their_range_fail_naming_the_variable() {
    type Case = (fn(&mut ServerConfig), &'static str);
    let cases: [Case; 10] = [
        (|c| c.ram_max_ops = 0, "ZEMDB_RAM_MAX_OPS"),
        (|c| c.ram_max_ops = 100_001, "ZEMDB_RAM_MAX_OPS"),
        (|c| c.ram_ttl_secs = 0, "ZEMDB_RAM_TTL_SECS"),
        (
            |c| c.warm_disk_ttl_secs = MAX_POLICY_DURATION_SECS + 1,
            "ZEMDB_WARM_TTL_SECS",
        ),
        (|c| c.cold_disk_ttl_secs = u64::MAX, "ZEMDB_COLD_TTL_SECS"),
        (
            |c| c.max_room_disk_bytes = 1024,
            "ZEMDB_ROOM_MAX_DISK_BYTES",
        ),
        (|c| c.lease_timeout_secs = 0, "ZEMDB_LEASE_TIMEOUT_SECS"),
        (
            |c| c.dormant_after_secs = Some(u64::MAX),
            "ZEMDB_DORMANT_AFTER_SECS",
        ),
        (
            |c| c.forget_dormant_after_secs = 0,
            "ZEMDB_FORGET_DORMANT_AFTER_SECS",
        ),
        (
            |c| c.idle_timeout_secs = u64::MAX,
            "ZEMDB_ROOM_IDLE_TIMEOUT_SECS",
        ),
    ];
    for (configure, variable) in cases {
        let mut config = strong_config();
        configure(&mut config);
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains(variable), "{variable}: {err}");
    }
}

#[test]
fn empty_environment_values_count_as_unset() {
    // Deployment templates often expand a missing variable to an empty string.
    let names = [
        "ZEMDB_HOST",
        "ZEMDB_PORT",
        "ZEMDB_DATA_DIR",
        "ZEMDB_AUTH_SECRET",
        "ZEMDB_ADMIN_SECRET",
        "ZEMDB_DEDUP_LRU_CAPACITY",
        "ZEMDB_SNAPSHOT_TTL_SECS",
        "ZEMDB_MAX_SNAPSHOT_BYTES",
        "ZEMDB_RAM_MAX_OPS",
        "ZEMDB_RAM_TTL_SECS",
        "ZEMDB_WARM_TTL_SECS",
        "ZEMDB_COLD_TTL_SECS",
        "ZEMDB_ROOM_MAX_DISK_BYTES",
        "ZEMDB_LEASE_TIMEOUT_SECS",
        "ZEMDB_DORMANT_AFTER_SECS",
        "ZEMDB_SNAPSHOT_DEMAND_TTL_SECS",
        "ZEMDB_ROOM_IDLE_TIMEOUT_SECS",
        "ZEMDB_HEADER_READ_TIMEOUT_SECS",
        "ZEMDB_BODY_READ_TIMEOUT_SECS",
        "ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC",
        "ZEMDB_MAX_CONNECTIONS",
    ];
    let vars: Vec<(&str, &str)> = names.iter().map(|name| (*name, "")).collect();
    assert_eq!(with_env(&vars).unwrap(), strong_config());
}

#[test]
fn http_limits_have_defaults_and_toml_keys() {
    let config = ServerConfig::default();
    assert_eq!(config.header_read_timeout_secs, 10);
    assert_eq!(config.body_read_timeout_secs, 60);
    assert_eq!(config.body_min_rate_bytes_per_sec, 32 * 1024);
    assert_eq!(config.max_connections, 10_000);
    assert_eq!(ServerConfig::from_toml_str("").unwrap(), config);

    let parsed = ServerConfig::from_toml_str(
        "header_read_timeout_secs = 5\nbody_read_timeout_secs = 120\n\
         body_min_rate_bytes_per_sec = 4096\nmax_connections = 500\n",
    )
    .unwrap();
    assert_eq!(parsed.header_read_timeout_secs, 5);
    assert_eq!(parsed.body_read_timeout_secs, 120);
    assert_eq!(parsed.body_min_rate_bytes_per_sec, 4096);
    assert_eq!(parsed.max_connections, 500);
}

#[test]
fn http_limit_environment_variables_set_the_limits() {
    let config = with_env(&[
        ("ZEMDB_HEADER_READ_TIMEOUT_SECS", "5"),
        ("ZEMDB_BODY_READ_TIMEOUT_SECS", "3600"),
        ("ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC", "1024"),
        ("ZEMDB_MAX_CONNECTIONS", "1"),
    ])
    .unwrap();
    config.validate().unwrap();
    assert_eq!(config.header_read_timeout_secs, 5);
    assert_eq!(config.body_read_timeout_secs, 3600);
    assert_eq!(config.body_min_rate_bytes_per_sec, 1024);
    assert_eq!(config.max_connections, 1);
}

#[test]
fn http_limits_outside_their_range_fail_naming_the_variable() {
    for (name, value) in [
        ("ZEMDB_HEADER_READ_TIMEOUT_SECS", "0"),
        ("ZEMDB_HEADER_READ_TIMEOUT_SECS", "301"),
        ("ZEMDB_BODY_READ_TIMEOUT_SECS", "0"),
        ("ZEMDB_BODY_READ_TIMEOUT_SECS", "3601"),
        ("ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC", "0"),
        ("ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC", "1023"),
        ("ZEMDB_BODY_MIN_RATE_BYTES_PER_SEC", "1073741825"),
        ("ZEMDB_MAX_CONNECTIONS", "0"),
        ("ZEMDB_MAX_CONNECTIONS", "1000001"),
    ] {
        let err = with_env(&[(name, value)]).unwrap().validate().unwrap_err();
        assert!(
            matches!(&err, ServerError::Config(msg) if msg.contains(name)),
            "{name}={value:?}: {err:?}"
        );
    }
}

#[test]
fn test_server_config_toml_and_env_overrides() {
    let toml_str = r#"
        host = "0.0.0.0"
        port = 9000
        data_dir = "/var/zemdb"
        auth_secret = "custom_cluster_secret_123456789!"
        admin_secret = "custom_admin_secret_1234567890!"
        lease_timeout_secs = 120
        dedup_lru_capacity = 25000
        snapshot_ttl_secs = 300
    "#;

    let mut config = ServerConfig::from_toml_str(toml_str).unwrap();
    assert_eq!(config.host, "0.0.0.0");
    assert_eq!(config.port, 9000);
    assert_eq!(config.data_dir.to_str().unwrap(), "/var/zemdb");
    assert_eq!(config.lease_timeout_secs, 120);
    assert_eq!(config.dedup_lru_capacity, 25000);
    assert_eq!(config.snapshot_ttl_secs, 300);
    assert_eq!(config.max_snapshot_bytes, 512 * 1024 * 1024);

    let vars = std::collections::HashMap::from([
        ("ZEMDB_PORT", "9999"),
        ("ZEMDB_HOST", "192.168.1.50"),
        ("ZEMDB_SNAPSHOT_TTL_SECS", "1800"),
        ("ZEMDB_MAX_SNAPSHOT_BYTES", "1048576"),
        ("ZEMDB_DORMANT_AFTER_SECS", "7200"),
        ("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS", "3600"),
        ("ZEMDB_RAM_MAX_OPS", "250"),
        ("ZEMDB_ROOM_IDLE_TIMEOUT_SECS", "0"),
    ]);
    config
        .apply_overrides_from(|name| Ok(vars.get(name).map(|value| value.to_string())))
        .unwrap();

    assert_eq!(config.port, 9999);
    assert_eq!(config.host, "192.168.1.50");
    assert_eq!(config.snapshot_ttl_secs, 1800);
    assert_eq!(config.max_snapshot_bytes, 1024 * 1024);
    assert_eq!(config.dormant_after_secs, Some(7200));
    assert_eq!(config.snapshot_demand_ttl_secs, 3600);
    assert_eq!(config.ram_max_ops, 250);
    assert_eq!(config.idle_timeout_secs, 0);
    let policy = config.default_lifecycle_policy();
    assert_eq!(policy.ram_max_ops, 250);
    assert_eq!(policy.lease_timeout, Duration::from_secs(120));
    assert_eq!(policy.dormant_after, Some(Duration::from_secs(7200)));
    assert_eq!(policy.idle_timeout, None);
}
