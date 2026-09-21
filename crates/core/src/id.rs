use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;

/// Strongly typed identifier for a collaborative Room.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RoomId(pub String);

impl RoomId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for RoomId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for RoomId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for RoomId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for RoomId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<RoomId> for String {
    fn from(id: RoomId) -> Self {
        id.0
    }
}

/// Strongly typed identifier for a participating Client.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ClientId(pub String);

impl ClientId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for ClientId {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ClientId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ClientId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<ClientId> for String {
    fn from(id: ClientId) -> Self {
        id.0
    }
}

/// Strictly monotonic sequence number per Room assigned by the server coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct SequenceNumber(pub u64);

impl SequenceNumber {
    pub const fn new(seq: u64) -> Self {
        Self(seq)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }

    pub const fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Deref for SequenceNumber {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for SequenceNumber {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<SequenceNumber> for u64 {
    fn from(seq: SequenceNumber) -> Self {
        seq.0
    }
}

/// Unique mutation ID (UUID v4 or 16-byte random) for commit idempotency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MutationId(pub [u8; 16]);

impl MutationId {
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    pub const fn from_u128(val: u128) -> Self {
        Self(val.to_be_bytes())
    }
}

impl Deref for MutationId {
    type Target = [u8; 16];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for MutationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl From<[u8; 16]> for MutationId {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<u128> for MutationId {
    fn from(val: u128) -> Self {
        Self::from_u128(val)
    }
}

impl From<MutationId> for [u8; 16] {
    fn from(m: MutationId) -> Self {
        m.0
    }
}

/// Correlation ID for multiplexing and pairing asynchronous requests and responses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default)]
#[serde(transparent)]
pub struct CorrelationId(pub u64);

impl CorrelationId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(&self) -> u64 {
        self.0
    }
}

impl Deref for CorrelationId {
    type Target = u64;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for CorrelationId {
    fn from(n: u64) -> Self {
        Self(n)
    }
}

impl From<CorrelationId> for u64 {
    fn from(c: CorrelationId) -> Self {
        c.0
    }
}
