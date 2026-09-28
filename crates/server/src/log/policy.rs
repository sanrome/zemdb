use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Retention, tier rotation, and eviction policy for a Room's immutable delta log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoomLifecyclePolicy {
    /// Maximum number of sequenced operations retained in RAM before rotating disk segments
    /// and evicting older deltas from memory.
    pub ram_max_ops: usize,

    /// Maximum time-to-live for operations in RAM before segment rotation and eviction.
    pub ram_ttl: Duration,

    /// Age threshold after which sealed uncompressed Warm Disk segments (.wal)
    /// are compressed into Cold Disk segments (.wal.zst).
    pub warm_disk_ttl: Duration,

    /// Retention threshold after which Cold Disk segments (.wal.zst) are pruned,
    /// advancing the Room's `tail_seq` and marking older cursors `BehindCompaction`.
    pub cold_disk_ttl: Duration,

    /// Maximum cumulative disk space (bytes) allocated to this room before early eviction of cold segments.
    pub max_room_disk_bytes: u64,
}

impl Default for RoomLifecyclePolicy {
    fn default() -> Self {
        Self {
            ram_max_ops: 1_000,
            ram_ttl: Duration::from_secs(300),          // 5 minutes
            warm_disk_ttl: Duration::from_secs(86_400),  // 24 hours
            cold_disk_ttl: Duration::from_secs(2_592_000), // 30 days
            max_room_disk_bytes: 500 * 1024 * 1024,     // 500 MB
        }
    }
}

impl RoomLifecyclePolicy {
    /// Preset policy with aggressive thresholds designed for rapid unit/integration testing.
    pub fn test_policy() -> Self {
        Self {
            ram_max_ops: 5,
            ram_ttl: Duration::from_millis(50),
            warm_disk_ttl: Duration::from_millis(100),
            cold_disk_ttl: Duration::from_millis(200),
            max_room_disk_bytes: 10 * 1024 * 1024,
        }
    }
}
