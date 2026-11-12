use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The configuration of a [`Client`](crate::Client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// The person the client writes as, the part of its client id before `#`: the id the
    /// backend issues the person's tokens with. Not empty, and without `#`.
    pub person: String,
    /// How long a write retries until its `CommitAck` (30 s): no attempt is sent after it.
    pub write_timeout: Duration,
    /// The longest first delay between two retries (100 ms), which doubles after each retry.
    /// Above zero.
    pub backoff_base: Duration,
    /// The longest delay between two retries (5 s). Not shorter than `backoff_base`.
    pub backoff_max: Duration,
    /// The largest snapshot the client downloads (512 MiB, the server's default).
    pub max_snapshot_bytes: u64,
    /// Whether a write of the facade is prechecked (on by default): before it is sent, the
    /// storage tells what the replica would decide on it in its state, and one that the replica
    /// would reject is `PrecheckFailed`, with nothing sent. An advice: the replica may be behind
    /// the log, or change before the server sequences the write. The core never prechecks
    /// ([`Client::write`](crate::Client::write)).
    pub precheck: bool,
    /// The mode the worker of a room starts in ([`Freshness::Periodic`]).
    pub freshness: Freshness,
    /// How often the worker of a room sends a heartbeat (30 s). Above zero, and below the 60 s
    /// in which the server waits for a client it designates to start uploading a snapshot; the
    /// application chooses it below the lease of its rooms too (90 s by default), which the
    /// client does not know.
    pub heartbeat_interval: Duration,
    /// The longest first delay before the worker of a room tries a step that failed again
    /// (1 s), which doubles after each retry. Above zero.
    pub retry_base: Duration,
    /// The longest delay before the worker of a room tries a step again (30 s). Not shorter
    /// than `retry_base`.
    pub retry_max: Duration,
    /// How long a worker in [`Freshness::Live`] waits to open the event stream again after the
    /// server answered that its events are disabled (10 min). Above zero.
    pub events_retry: Duration,
}

impl ClientConfig {
    /// The configuration of a client of `person`.
    pub fn new(person: impl Into<String>) -> Self {
        Self {
            person: person.into(),
            write_timeout: Duration::from_secs(30),
            backoff_base: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
            max_snapshot_bytes: 512 << 20,
            precheck: true,
            freshness: Freshness::default(),
            heartbeat_interval: Duration::from_secs(30),
            retry_base: Duration::from_secs(1),
            retry_max: Duration::from_secs(30),
            events_retry: Duration::from_secs(600),
        }
    }
}

/// How soon the worker of a room catches its replica up with what the others write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Freshness {
    /// At once, by the event stream of the room, and at the latest with each heartbeat.
    Live,
    /// With each heartbeat whose ack reports a head past the replica's.
    #[default]
    Periodic,
    /// With each write and each sync of the application. The worker still sends heartbeats,
    /// uploads the snapshot the server asks it for and downloads the one the replica waits for.
    Manual,
}
