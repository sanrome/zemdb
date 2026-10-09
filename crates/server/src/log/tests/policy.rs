use super::*;

#[test]
fn overrides_replace_only_the_settings_they_set() {
    let defaults = RoomLifecyclePolicy {
        dormant_after: Some(Duration::from_secs(3600)),
        ..RoomLifecyclePolicy::default()
    };
    assert_eq!(
        defaults.with_overrides(&RoomLifecycleOverrides::default()),
        defaults
    );

    let overrides = RoomLifecycleOverrides {
        ram_max_ops: Some(2),
        lease_timeout_secs: Some(5),
        idle_timeout_secs: Some(30),
        ..RoomLifecycleOverrides::default()
    };
    assert_eq!(
        defaults.with_overrides(&overrides),
        RoomLifecyclePolicy {
            ram_max_ops: 2,
            lease_timeout: Duration::from_secs(5),
            idle_timeout: Some(Duration::from_secs(30)),
            ..defaults.clone()
        }
    );
}

#[test]
fn zero_idle_timeout_override_means_never() {
    let overrides = RoomLifecycleOverrides {
        idle_timeout_secs: Some(0),
        ..RoomLifecycleOverrides::default()
    };
    overrides.validate().unwrap();
    let policy = RoomLifecyclePolicy::default().with_overrides(&overrides);
    assert_eq!(policy.idle_timeout, None);
}

#[test]
fn overrides_outside_their_range_are_bad_requests_naming_the_field() {
    let cases = [
        (
            RoomLifecycleOverrides {
                ram_max_ops: Some(0),
                ..Default::default()
            },
            "lifecycle.ram_max_ops",
        ),
        (
            RoomLifecycleOverrides {
                ram_max_ops: Some(MAX_RAM_MAX_OPS as usize + 1),
                ..Default::default()
            },
            "lifecycle.ram_max_ops",
        ),
        (
            RoomLifecycleOverrides {
                ram_ttl_secs: Some(0),
                ..Default::default()
            },
            "lifecycle.ram_ttl_secs",
        ),
        (
            RoomLifecycleOverrides {
                cold_disk_ttl_secs: Some(MAX_POLICY_DURATION_SECS + 1),
                ..Default::default()
            },
            "lifecycle.cold_disk_ttl_secs",
        ),
        (
            RoomLifecycleOverrides {
                max_room_disk_bytes: Some(MIN_ROOM_DISK_BYTES - 1),
                ..Default::default()
            },
            "lifecycle.max_room_disk_bytes",
        ),
        (
            RoomLifecycleOverrides {
                lease_timeout_secs: Some(0),
                ..Default::default()
            },
            "lifecycle.lease_timeout_secs",
        ),
        (
            RoomLifecycleOverrides {
                dormant_after_secs: Some(0),
                ..Default::default()
            },
            "lifecycle.dormant_after_secs",
        ),
        (
            RoomLifecycleOverrides {
                snapshot_demand_ttl_secs: Some(MIN_SNAPSHOT_DEMAND_TTL_SECS - 1),
                ..Default::default()
            },
            "lifecycle.snapshot_demand_ttl_secs",
        ),
        (
            RoomLifecycleOverrides {
                idle_timeout_secs: Some(u64::MAX),
                ..Default::default()
            },
            "lifecycle.idle_timeout_secs",
        ),
    ];
    for (overrides, field) in cases {
        match overrides.validate() {
            Err(ServerError::BadRequest(msg)) => assert!(msg.contains(field), "{field}: {msg}"),
            other => panic!("{field}: expected BadRequest, got {other:?}"),
        }
    }

    RoomLifecycleOverrides {
        ram_max_ops: Some(1),
        ram_ttl_secs: Some(1),
        warm_disk_ttl_secs: Some(0),
        cold_disk_ttl_secs: Some(0),
        max_room_disk_bytes: Some(MIN_ROOM_DISK_BYTES),
        lease_timeout_secs: Some(1),
        dormant_after_secs: Some(MAX_POLICY_DURATION_SECS),
        snapshot_demand_ttl_secs: Some(MIN_SNAPSHOT_DEMAND_TTL_SECS),
        idle_timeout_secs: Some(1),
    }
    .validate()
    .unwrap();
}

#[test]
fn overrides_json_uses_whole_seconds_and_omits_unset_fields() {
    let overrides: RoomLifecycleOverrides =
        serde_json::from_str(r#"{"ram_max_ops": 2, "idle_timeout_secs": 0}"#).unwrap();
    assert_eq!(
        overrides,
        RoomLifecycleOverrides {
            ram_max_ops: Some(2),
            idle_timeout_secs: Some(0),
            ..Default::default()
        }
    );
    assert_eq!(
        serde_json::to_value(&overrides).unwrap(),
        serde_json::json!({"ram_max_ops": 2, "idle_timeout_secs": 0})
    );
    assert_eq!(
        serde_json::from_str::<RoomLifecycleOverrides>("{}").unwrap(),
        RoomLifecycleOverrides::default()
    );
}

#[test]
fn overrides_json_rejects_unknown_fields_and_wrong_types() {
    // A misspelled field, or the duration shape of an older format, must not be ignored.
    assert!(serde_json::from_str::<RoomLifecycleOverrides>(r#"{"ram_ttl": 5}"#).is_err());
    assert!(serde_json::from_str::<RoomLifecycleOverrides>(
        r#"{"ram_ttl_secs": {"secs": 5, "nanos": 0}}"#
    )
    .is_err());
    assert!(serde_json::from_str::<RoomLifecycleOverrides>(r#"{"ram_ttl_secs": -1}"#).is_err());
}

#[test]
fn effective_policy_json_is_in_whole_seconds() {
    let policy = RoomLifecyclePolicy {
        ram_ttl: Duration::from_millis(50),
        dormant_after: None,
        idle_timeout: None,
        ..RoomLifecyclePolicy::default()
    };
    let json = serde_json::to_value(&policy).unwrap();
    assert_eq!(json["ram_ttl_secs"], 1, "sub-second durations round up");
    assert_eq!(json["lease_timeout_secs"], 90);
    assert_eq!(json["dormant_after_secs"], serde_json::Value::Null);
    assert_eq!(json["idle_timeout_secs"], 0);

    let round_trip: RoomLifecyclePolicy = serde_json::from_value(json).unwrap();
    assert_eq!(
        round_trip,
        RoomLifecyclePolicy {
            ram_ttl: Duration::from_secs(1),
            ..policy
        }
    );
}
