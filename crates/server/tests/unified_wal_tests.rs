use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use tempfile::tempdir;
use zemdb_core::id::{MutationId, SequenceNumber};
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::{ErrorCode, SequencedOperation};
use zemdb_core::value::{PrimaryKey, Value};
use zemdb_server::config::ServerConfig;
use zemdb_server::dedup::DedupLruCache;
use zemdb_server::error::ServerError;
use zemdb_server::log::{RoomLifecyclePolicy, TieredLog};

fn make_test_op(seq: u64) -> SequencedOperation {
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    let op = Operation::delete(1, pk, seq * 1000);
    SequencedOperation::new(SequenceNumber::new(seq), op)
}

#[test]
fn test_unified_wal_append_and_recovery_roundtrip() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    // 1. Initial creation
    {
        let (mut log, recovered_mutations) =
            TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        assert_eq!(log.head_seq().get(), 0);
        assert_eq!(recovered_mutations.len(), 0);

        // 2. Append 10 entries with distinct mutation IDs
        for i in 1..=10 {
            let seq = SequenceNumber::new(i);
            let mutation_id = MutationId::from_u128(i as u128 + 1000);
            let op = make_test_op(seq.get());

            log.append(op, Some(mutation_id)).unwrap();
            assert_eq!(log.head_seq().get(), i);
        }
    }

    // 3. Re-open and verify full recovery of head_seq, deltas, and mutation IDs
    let (log2, recovered_mutations2) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log2.head_seq().get(), 10);
    assert_eq!(recovered_mutations2.len(), 10);

    for (idx, (mut_id, seq)) in recovered_mutations2.iter().enumerate() {
        let expected_seq = idx as u64 + 1;
        assert_eq!(seq.get(), expected_seq);
        assert_eq!(*mut_id, MutationId::from_u128(expected_seq as u128 + 1000));
    }

    let (deltas, has_more) = log2.fetch_deltas(SequenceNumber::new(0), 100).unwrap();
    assert_eq!(deltas.len(), 10);
    assert!(!has_more);
}

#[test]
fn test_unified_wal_dedup_idempotency() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    let mut expected_entries = Vec::new();
    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();

        for i in 1..=5 {
            let seq = SequenceNumber::new(i);
            let mutation_id = MutationId::from_u128(i as u128 * 42);
            let op = make_test_op(seq.get());
            log.append(op, Some(mutation_id)).unwrap();
            expected_entries.push((mutation_id, seq));
        }
    }

    // Reopen and hydrate DedupLruCache from recovered log entries
    let (_, recovered_mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    let mut dedup_cache = DedupLruCache::new(100);
    dedup_cache.hydrate(recovered_mutations);

    assert_eq!(dedup_cache.len(), 5);

    // Verify all recorded mutations are recognized as duplicates
    for (mut_id, seq) in expected_entries {
        assert_eq!(dedup_cache.is_duplicate(&mut_id), Some(seq));
    }

    // Verify an uncommitted mutation is not recognized as a duplicate
    let unknown_mut = MutationId::from_u128(999999);
    assert_eq!(dedup_cache.is_duplicate(&unknown_mut), None);

    // Record new mutation and verify it becomes duplicate
    dedup_cache.record(unknown_mut, SequenceNumber::new(6));
    assert_eq!(
        dedup_cache.is_duplicate(&unknown_mut),
        Some(SequenceNumber::new(6))
    );
}

#[test]
fn test_unified_wal_crc_tampering_detection() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        for i in 1..=3 {
            let seq = SequenceNumber::new(i);
            let mutation_id = MutationId::from_u128(i as u128);
            let op = make_test_op(seq.get());
            log.append(op, Some(mutation_id)).unwrap();
        }
    }

    let active_path = dir.path().join("segments").join("active.wal");
    let mut data = std::fs::read(&active_path).unwrap();

    // Corrupt one byte in the middle of the first batch (after batch header)
    data[18] ^= 0xFF;
    std::fs::write(&active_path, data).unwrap();

    let err = TieredLog::open_or_create(dir.path(), policy).err().unwrap();
    match err {
        ServerError::WalCorruption(msg) => {
            assert!(msg.contains("CRC") || msg.contains("crc") || msg.contains("Corruption"));
        }
        other => panic!("Expected WalCorruption, got: {:?}", other),
    }
}

#[test]
fn test_unified_wal_torn_write_recovery_and_truncation() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        for i in 1..=3 {
            let seq = SequenceNumber::new(i);
            let mutation_id = MutationId::from_u128(i as u128);
            let op = make_test_op(seq.get());
            log.append(op, Some(mutation_id)).unwrap();
        }
    }

    let active_path = dir.path().join("segments").join("active.wal");
    let clean_len = std::fs::metadata(&active_path).unwrap().len();

    // Simulate a torn write by appending incomplete bytes to active.wal
    {
        let mut file = OpenOptions::new().append(true).open(&active_path).unwrap();
        file.write_all(&[0x52, 0x49, 0x4D, 0x42, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB])
            .unwrap();
    }

    let damaged_len = std::fs::metadata(&active_path).unwrap().len();
    assert_eq!(damaged_len, clean_len + 10);

    // Reopen log: torn write should be detected and automatically truncated to clean_len
    let (mut log2, recovered_mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log2.head_seq().get(), 3);
    assert_eq!(recovered_mutations.len(), 3);

    let truncated_disk_len = std::fs::metadata(&active_path).unwrap().len();
    assert_eq!(truncated_disk_len, clean_len);

    // Verify subsequent append proceeds contiguously
    let op4 = make_test_op(4);
    log2.append(op4, Some(MutationId::from_u128(4))).unwrap();
    assert_eq!(log2.head_seq().get(), 4);
}

#[test]
fn test_unified_wal_zero_filled_eof_truncation() {
    let dir = tempdir().unwrap();
    let policy = RoomLifecyclePolicy::default();

    {
        let (mut log, _) = TieredLog::open_or_create(dir.path(), policy.clone()).unwrap();
        let op1 = make_test_op(1);
        log.append(op1, Some(MutationId::from_u128(1))).unwrap();
    }

    let active_path = dir.path().join("segments").join("active.wal");
    let clean_len = std::fs::metadata(&active_path).unwrap().len();

    // Simulate zero-filled tail appended due to crash during allocation
    {
        let mut file = OpenOptions::new().append(true).open(&active_path).unwrap();
        file.write_all(&[0u8; 512]).unwrap();
    }

    let (log2, recovered_mutations) = TieredLog::open_or_create(dir.path(), policy).unwrap();
    assert_eq!(log2.head_seq().get(), 1);
    assert_eq!(recovered_mutations.len(), 1);

    let disk_len = std::fs::metadata(&active_path).unwrap().len();
    assert_eq!(disk_len, clean_len);
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

    // Test environment variable overrides
    std::env::set_var("ZEMDB_PORT", "9999");
    std::env::set_var("ZEMDB_HOST", "192.168.1.50");
    std::env::set_var("ZEMDB_SNAPSHOT_TTL_SECS", "1800");
    std::env::set_var("ZEMDB_MAX_SNAPSHOT_BYTES", "1048576");
    std::env::set_var("ZEMDB_DORMANT_AFTER_SECS", "7200");
    std::env::set_var("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS", "3600");
    std::env::set_var("ZEMDB_RAM_MAX_OPS", "250");
    std::env::set_var("ZEMDB_ROOM_IDLE_TIMEOUT_SECS", "0");
    config.apply_env_overrides().unwrap();

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

    // Clean up env vars
    std::env::remove_var("ZEMDB_PORT");
    std::env::remove_var("ZEMDB_HOST");
    std::env::remove_var("ZEMDB_SNAPSHOT_TTL_SECS");
    std::env::remove_var("ZEMDB_MAX_SNAPSHOT_BYTES");
    std::env::remove_var("ZEMDB_DORMANT_AFTER_SECS");
    std::env::remove_var("ZEMDB_SNAPSHOT_DEMAND_TTL_SECS");
    std::env::remove_var("ZEMDB_RAM_MAX_OPS");
    std::env::remove_var("ZEMDB_ROOM_IDLE_TIMEOUT_SECS");
}

#[test]
fn test_server_error_mapping() {
    let schema_err = ServerError::SchemaViolation("Missing column 'title'".to_string());
    assert_eq!(schema_err.to_error_code(), ErrorCode::SchemaViolation);
    assert_eq!(
        schema_err.to_status_code(),
        axum::http::StatusCode::BAD_REQUEST
    );

    let unauth_err = ServerError::Unauthorized("Invalid bearer token".to_string());
    assert_eq!(unauth_err.to_error_code(), ErrorCode::Unauthorized);
    assert_eq!(
        unauth_err.to_status_code(),
        axum::http::StatusCode::UNAUTHORIZED
    );

    let behind_err = ServerError::BehindCompaction;
    assert_eq!(behind_err.to_error_code(), ErrorCode::BehindCompaction);
    assert_eq!(behind_err.to_status_code(), axum::http::StatusCode::GONE);

    let room_nf = ServerError::RoomNotFound("room-123".to_string());
    assert_eq!(room_nf.to_error_code(), ErrorCode::RoomNotFound);
    assert_eq!(room_nf.to_status_code(), axum::http::StatusCode::NOT_FOUND);

    let invalid_seq_err = ServerError::InvalidSequence {
        expected: SequenceNumber::new(10),
        actual: SequenceNumber::new(20),
    };
    assert_eq!(invalid_seq_err.to_error_code(), ErrorCode::InvalidSequence);
    assert_eq!(
        invalid_seq_err.to_status_code(),
        axum::http::StatusCode::BAD_REQUEST
    );

    let gw_err = ServerError::Timeout("Actor timed out".to_string());
    assert_eq!(gw_err.to_error_code(), ErrorCode::Timeout);
    assert_eq!(
        gw_err.to_status_code(),
        axum::http::StatusCode::GATEWAY_TIMEOUT
    );

    let locked_err =
        ServerError::RoomLocked("Room is already locked by another process".to_string());
    assert_eq!(locked_err.to_error_code(), ErrorCode::RoomLocked);
    assert_eq!(locked_err.to_status_code(), axum::http::StatusCode::LOCKED);

    let superseded_err = ServerError::SnapshotSuperseded("a newer snapshot is active".to_string());
    assert_eq!(
        superseded_err.to_error_code(),
        ErrorCode::SnapshotSuperseded
    );
    assert_eq!(
        superseded_err.to_status_code(),
        axum::http::StatusCode::CONFLICT
    );
}
