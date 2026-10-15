use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::error::ServerError;

/// Longest duration any lifecycle setting accepts, in seconds (10 years). It keeps every
/// `Instant` and `SystemTime` computed from a setting far from overflowing.
pub const MAX_POLICY_DURATION_SECS: u64 = 10 * 365 * 24 * 60 * 60;

/// Largest accepted `ram_max_ops`: the operations of a room kept in RAM.
pub const MAX_RAM_MAX_OPS: u64 = 100_000;

/// Smallest accepted `ram_max_bytes` and `segment_max_bytes` (1 MiB).
pub const MIN_LOG_BYTES_LIMIT: u64 = 1024 * 1024;

/// Largest accepted `ram_max_bytes` and `segment_max_bytes` (1 GiB).
pub const MAX_LOG_BYTES_LIMIT: u64 = 1024 * 1024 * 1024;

/// Smallest accepted `max_room_disk_bytes` (1 MiB).
pub const MIN_ROOM_DISK_BYTES: u64 = 1024 * 1024;

/// Smallest accepted `snapshot_demand_ttl_secs`: a demand that expires within a few
/// maintenance ticks would turn on and off with every request.
pub const MIN_SNAPSHOT_DEMAND_TTL_SECS: u64 = 60;

/// Lifecycle policy of a room: retention, tier rotation and eviction of its immutable delta
/// log, the lifecycle of its clients, and when its actor shuts down for inactivity.
///
/// This is the effective policy a room runs with: the server defaults overlaid with the
/// room's [`RoomLifecycleOverrides`]. In JSON (the admin room metrics) durations are whole
/// seconds (`*_secs`, rounded up), `dormant_after_secs` is `null` when unset and
/// `idle_timeout_secs` is `0` when the room never shuts down for inactivity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "PolicyJson", from = "PolicyJson")]
pub struct RoomLifecyclePolicy {
    /// Maximum number of sequenced operations retained in RAM before rotating disk segments
    /// and evicting older deltas from memory.
    pub ram_max_ops: usize,

    /// Maximum encoded size of the operations retained in RAM. Whichever of this and
    /// `ram_max_ops` is reached first evicts the oldest; an operation larger than this on its
    /// own is not kept in RAM.
    pub ram_max_bytes: u64,

    /// Size of the active segment (`active.wal`) at which it is sealed, if `ram_max_ops`
    /// operations have not sealed it first.
    pub segment_max_bytes: u64,

    /// Maximum age of an operation in RAM; older ones are evicted (they stay on disk).
    pub ram_ttl: Duration,

    /// Age threshold after which sealed uncompressed Warm Disk segments (.wal)
    /// are compressed into Cold Disk segments (.wal.zst).
    pub warm_disk_ttl: Duration,

    /// Retention threshold after which Cold Disk segments (.wal.zst) are pruned,
    /// advancing the Room's `tail_seq` and marking older cursors `BehindCompaction`.
    pub cold_disk_ttl: Duration,

    /// Disk space of the room's log above which the oldest cold segments are pruned early.
    pub max_room_disk_bytes: u64,

    /// Inactivity after which an active client lease is marked Disconnected.
    pub lease_timeout: Duration,

    /// Inactivity after which a Disconnected client becomes Dormant even though its cursor is
    /// still inside the retained log. `None`: a Disconnected client only becomes Dormant once
    /// its cursor falls behind the log.
    pub dormant_after: Option<Duration>,

    /// Inactivity after which a Dormant client is removed from the roster; if it comes back,
    /// it must register again.
    pub forget_dormant_after: Duration,

    /// How long the room keeps asking its clients for a snapshot without the request being
    /// renewed.
    pub snapshot_demand_ttl: Duration,

    /// Time without commands, SSE subscribers or snapshot uploads after which the room actor
    /// shuts down; the next request reopens the room from disk. `None`: never.
    pub idle_timeout: Option<Duration>,
}

impl Default for RoomLifecyclePolicy {
    fn default() -> Self {
        Self {
            ram_max_ops: 1_000,
            ram_max_bytes: 8 * 1024 * 1024,                // 8 MiB
            segment_max_bytes: 32 * 1024 * 1024,           // 32 MiB
            ram_ttl: Duration::from_secs(300),             // 5 minutes
            warm_disk_ttl: Duration::from_secs(86_400),    // 24 hours
            cold_disk_ttl: Duration::from_secs(2_592_000), // 30 days
            max_room_disk_bytes: 500 * 1024 * 1024,        // 500 MB
            lease_timeout: Duration::from_secs(90),
            dormant_after: None,
            forget_dormant_after: Duration::from_secs(2_592_000), // 30 days
            snapshot_demand_ttl: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            idle_timeout: Some(Duration::from_secs(600)),         // 10 minutes
        }
    }
}

impl RoomLifecyclePolicy {
    /// Preset policy with aggressive thresholds for fast tests. Rooms using it never shut down
    /// for inactivity.
    #[cfg(test)]
    pub(crate) fn test_policy() -> Self {
        Self {
            ram_max_ops: 5,
            ram_ttl: Duration::from_millis(50),
            warm_disk_ttl: Duration::from_millis(100),
            cold_disk_ttl: Duration::from_millis(200),
            max_room_disk_bytes: 10 * 1024 * 1024,
            idle_timeout: None,
            ..Self::default()
        }
    }

    /// This policy with every setting present in `overrides` replaced by the override.
    ///
    /// Overrides are validated setting by setting and no rule relates two settings, so valid
    /// overrides on a valid policy always give a valid policy, whatever the server defaults
    /// are when the room reopens.
    pub fn with_overrides(&self, overrides: &RoomLifecycleOverrides) -> Self {
        let secs = Duration::from_secs;
        let o = overrides;
        Self {
            ram_max_ops: o.ram_max_ops.unwrap_or(self.ram_max_ops),
            ram_max_bytes: o.ram_max_bytes.unwrap_or(self.ram_max_bytes),
            segment_max_bytes: o.segment_max_bytes.unwrap_or(self.segment_max_bytes),
            ram_ttl: o.ram_ttl_secs.map_or(self.ram_ttl, secs),
            warm_disk_ttl: o.warm_disk_ttl_secs.map_or(self.warm_disk_ttl, secs),
            cold_disk_ttl: o.cold_disk_ttl_secs.map_or(self.cold_disk_ttl, secs),
            max_room_disk_bytes: o.max_room_disk_bytes.unwrap_or(self.max_room_disk_bytes),
            lease_timeout: o.lease_timeout_secs.map_or(self.lease_timeout, secs),
            dormant_after: o.dormant_after_secs.map(secs).or(self.dormant_after),
            forget_dormant_after: o
                .forget_dormant_after_secs
                .map_or(self.forget_dormant_after, secs),
            snapshot_demand_ttl: o
                .snapshot_demand_ttl_secs
                .map_or(self.snapshot_demand_ttl, secs),
            idle_timeout: match o.idle_timeout_secs {
                Some(0) => None,
                Some(idle) => Some(secs(idle)),
                None => self.idle_timeout,
            },
        }
    }
}

/// Lifecycle settings of one room that differ from the server defaults, stored in the room's
/// `meta_room.json` and accepted by `POST /admin/rooms`. Every field is optional: an absent
/// one follows the server default (its environment variable), also when the default changes
/// later. Durations are whole seconds. `idle_timeout_secs: 0` means the room never shuts
/// down for inactivity; `dormant_after_secs` can only be set, not cleared.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoomLifecycleOverrides {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_max_ops: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_max_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_max_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ram_ttl_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm_disk_ttl_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cold_disk_ttl_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_room_disk_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lease_timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dormant_after_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forget_dormant_after_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_demand_ttl_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout_secs: Option<u64>,
}

impl RoomLifecycleOverrides {
    /// Checks every present setting against its range (see `PolicySetting`). An invalid
    /// override is the request's fault: `BadRequest`, naming the field.
    pub fn validate(&self) -> Result<(), ServerError> {
        self.check(|setting| format!("lifecycle.{}", setting.field()))
            .map_err(ServerError::BadRequest)
    }

    /// Checks every present setting, naming an invalid one with `label`.
    pub(crate) fn check(&self, label: impl Fn(PolicySetting) -> String) -> Result<(), String> {
        for (setting, value) in self.settings() {
            if let Some(value) = value {
                setting
                    .check(value)
                    .map_err(|range| format!("{} must be {range}, got {value}", label(setting)))?;
            }
        }
        Ok(())
    }

    fn settings(&self) -> [(PolicySetting, Option<u64>); 12] {
        use PolicySetting::*;
        [
            (
                RamMaxOps,
                self.ram_max_ops
                    .map(|ops| u64::try_from(ops).unwrap_or(u64::MAX)),
            ),
            (RamMaxBytes, self.ram_max_bytes),
            (SegmentMaxBytes, self.segment_max_bytes),
            (RamTtl, self.ram_ttl_secs),
            (WarmDiskTtl, self.warm_disk_ttl_secs),
            (ColdDiskTtl, self.cold_disk_ttl_secs),
            (MaxRoomDiskBytes, self.max_room_disk_bytes),
            (LeaseTimeout, self.lease_timeout_secs),
            (DormantAfter, self.dormant_after_secs),
            (ForgetDormantAfter, self.forget_dormant_after_secs),
            (SnapshotDemandTtl, self.snapshot_demand_ttl_secs),
            (IdleTimeout, self.idle_timeout_secs),
        ]
    }
}

/// One setting of the lifecycle policy, as it is configured: an integer in its own unit
/// (operations, seconds or bytes). The same ranges apply to the server defaults (environment
/// variables, validated at startup) and to a room's overrides (validated by the admin API).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PolicySetting {
    RamMaxOps,
    RamMaxBytes,
    SegmentMaxBytes,
    RamTtl,
    WarmDiskTtl,
    ColdDiskTtl,
    MaxRoomDiskBytes,
    LeaseTimeout,
    DormantAfter,
    ForgetDormantAfter,
    SnapshotDemandTtl,
    IdleTimeout,
}

impl PolicySetting {
    /// Field name in the server configuration, `meta_room.json` and the admin JSON.
    pub(crate) fn field(self) -> &'static str {
        match self {
            Self::RamMaxOps => "ram_max_ops",
            Self::RamMaxBytes => "ram_max_bytes",
            Self::SegmentMaxBytes => "segment_max_bytes",
            Self::RamTtl => "ram_ttl_secs",
            Self::WarmDiskTtl => "warm_disk_ttl_secs",
            Self::ColdDiskTtl => "cold_disk_ttl_secs",
            Self::MaxRoomDiskBytes => "max_room_disk_bytes",
            Self::LeaseTimeout => "lease_timeout_secs",
            Self::DormantAfter => "dormant_after_secs",
            Self::ForgetDormantAfter => "forget_dormant_after_secs",
            Self::SnapshotDemandTtl => "snapshot_demand_ttl_secs",
            Self::IdleTimeout => "idle_timeout_secs",
        }
    }

    /// Environment variable holding the server default.
    pub(crate) fn env_var(self) -> &'static str {
        match self {
            Self::RamMaxOps => "ZEMDB_RAM_MAX_OPS",
            Self::RamMaxBytes => "ZEMDB_RAM_MAX_BYTES",
            Self::SegmentMaxBytes => "ZEMDB_SEGMENT_MAX_BYTES",
            Self::RamTtl => "ZEMDB_RAM_TTL_SECS",
            Self::WarmDiskTtl => "ZEMDB_WARM_TTL_SECS",
            Self::ColdDiskTtl => "ZEMDB_COLD_TTL_SECS",
            Self::MaxRoomDiskBytes => "ZEMDB_ROOM_MAX_DISK_BYTES",
            Self::LeaseTimeout => "ZEMDB_LEASE_TIMEOUT_SECS",
            Self::DormantAfter => "ZEMDB_DORMANT_AFTER_SECS",
            Self::ForgetDormantAfter => "ZEMDB_FORGET_DORMANT_AFTER_SECS",
            Self::SnapshotDemandTtl => "ZEMDB_SNAPSHOT_DEMAND_TTL_SECS",
            Self::IdleTimeout => "ZEMDB_ROOM_IDLE_TIMEOUT_SECS",
        }
    }

    /// Accepted values, inclusive.
    ///
    /// A RAM TTL of 0 would send every read to disk, so durations start at 1 s, except
    /// the warm and cold TTLs (0 compresses or prunes a segment as soon as it is sealed or
    /// compressed) and the idle timeout (0 = never). A lease timeout or dormancy of 0 would
    /// disconnect clients between requests.
    fn bounds(self) -> (u64, u64) {
        match self {
            Self::RamMaxOps => (1, MAX_RAM_MAX_OPS),
            Self::RamMaxBytes | Self::SegmentMaxBytes => (MIN_LOG_BYTES_LIMIT, MAX_LOG_BYTES_LIMIT),
            Self::RamTtl | Self::LeaseTimeout | Self::DormantAfter | Self::ForgetDormantAfter => {
                (1, MAX_POLICY_DURATION_SECS)
            }
            Self::WarmDiskTtl | Self::ColdDiskTtl | Self::IdleTimeout => {
                (0, MAX_POLICY_DURATION_SECS)
            }
            Self::MaxRoomDiskBytes => (MIN_ROOM_DISK_BYTES, u64::MAX),
            Self::SnapshotDemandTtl => (MIN_SNAPSHOT_DEMAND_TTL_SECS, MAX_POLICY_DURATION_SECS),
        }
    }

    /// Checks `value` against the setting's range; the error describes the range.
    pub(crate) fn check(self, value: u64) -> Result<(), String> {
        let (min, max) = self.bounds();
        if (min..=max).contains(&value) {
            return Ok(());
        }
        Err(if max == u64::MAX {
            format!("at least {min}")
        } else {
            format!("between {min} and {max}")
        })
    }
}

/// JSON form of [`RoomLifecyclePolicy`]: whole seconds, like the overrides.
#[derive(Serialize, Deserialize)]
struct PolicyJson {
    ram_max_ops: usize,
    ram_max_bytes: u64,
    segment_max_bytes: u64,
    ram_ttl_secs: u64,
    warm_disk_ttl_secs: u64,
    cold_disk_ttl_secs: u64,
    max_room_disk_bytes: u64,
    lease_timeout_secs: u64,
    dormant_after_secs: Option<u64>,
    forget_dormant_after_secs: u64,
    snapshot_demand_ttl_secs: u64,
    idle_timeout_secs: u64,
}

/// Whole seconds of `duration`, rounded up so that a short non-zero duration is not shown
/// as 0 (which means "never" for the idle timeout).
fn ceil_secs(duration: Duration) -> u64 {
    duration.as_secs() + u64::from(duration.subsec_nanos() > 0)
}

impl From<RoomLifecyclePolicy> for PolicyJson {
    fn from(policy: RoomLifecyclePolicy) -> Self {
        Self {
            ram_max_ops: policy.ram_max_ops,
            ram_max_bytes: policy.ram_max_bytes,
            segment_max_bytes: policy.segment_max_bytes,
            ram_ttl_secs: ceil_secs(policy.ram_ttl),
            warm_disk_ttl_secs: ceil_secs(policy.warm_disk_ttl),
            cold_disk_ttl_secs: ceil_secs(policy.cold_disk_ttl),
            max_room_disk_bytes: policy.max_room_disk_bytes,
            lease_timeout_secs: ceil_secs(policy.lease_timeout),
            dormant_after_secs: policy.dormant_after.map(ceil_secs),
            forget_dormant_after_secs: ceil_secs(policy.forget_dormant_after),
            snapshot_demand_ttl_secs: ceil_secs(policy.snapshot_demand_ttl),
            idle_timeout_secs: policy.idle_timeout.map_or(0, ceil_secs),
        }
    }
}

impl From<PolicyJson> for RoomLifecyclePolicy {
    fn from(json: PolicyJson) -> Self {
        let secs = Duration::from_secs;
        Self {
            ram_max_ops: json.ram_max_ops,
            ram_max_bytes: json.ram_max_bytes,
            segment_max_bytes: json.segment_max_bytes,
            ram_ttl: secs(json.ram_ttl_secs),
            warm_disk_ttl: secs(json.warm_disk_ttl_secs),
            cold_disk_ttl: secs(json.cold_disk_ttl_secs),
            max_room_disk_bytes: json.max_room_disk_bytes,
            lease_timeout: secs(json.lease_timeout_secs),
            dormant_after: json.dormant_after_secs.map(secs),
            forget_dormant_after: secs(json.forget_dormant_after_secs),
            snapshot_demand_ttl: secs(json.snapshot_demand_ttl_secs),
            idle_timeout: (json.idle_timeout_secs > 0).then(|| secs(json.idle_timeout_secs)),
        }
    }
}

#[cfg(test)]
#[path = "tests/policy.rs"]
mod tests;
