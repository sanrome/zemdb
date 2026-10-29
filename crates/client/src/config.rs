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
}

impl ClientConfig {
    /// The configuration of a client of `person`.
    pub fn new(person: impl Into<String>) -> Self {
        Self {
            person: person.into(),
            write_timeout: Duration::from_secs(30),
            backoff_base: Duration::from_millis(100),
            backoff_max: Duration::from_secs(5),
        }
    }
}
