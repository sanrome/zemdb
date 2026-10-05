use serde::{Deserialize, Serialize};
use std::fmt;

/// Error returned when a string is not a valid identifier of the requested kind.
///
/// `value` holds the rejected input truncated to [`MAX_ERROR_VALUE_CHARS`] characters (with a
/// trailing `…` when cut), so error messages built from untrusted input stay bounded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} {value:?}: {reason}")]
pub struct InvalidIdError {
    pub kind: &'static str,
    pub value: String,
    pub reason: &'static str,
}

/// Maximum number of characters of the rejected input kept in an [`InvalidIdError`].
pub const MAX_ERROR_VALUE_CHARS: usize = 80;

impl InvalidIdError {
    fn new(kind: &'static str, value: &str, reason: &'static str) -> Self {
        let value = match value.char_indices().nth(MAX_ERROR_VALUE_CHARS) {
            Some((cut, _)) => format!("{}…", &value[..cut]),
            None => value.to_string(),
        };
        Self {
            kind,
            value,
            reason,
        }
    }
}

/// Maximum length of identifiers used as file and directory names (room and schema IDs).
pub const MAX_PATH_ID_LEN: usize = 64;

/// Maximum length, in bytes, of a client identifier.
pub const MAX_CLIENT_ID_LEN: usize = 256;

/// Device names that Windows reserves in every directory, regardless of extension.
const WINDOWS_RESERVED_NAMES: [&str; 24] = [
    "con", "prn", "aux", "nul", "com0", "com1", "com2", "com3", "com4", "com5", "com6", "com7",
    "com8", "com9", "lpt0", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
];

/// Validates an identifier that is used as a file or directory name.
///
/// Only lowercase ASCII letters, digits, `-` and `_` are allowed. This rules out path
/// separators and `..` (path traversal), and uppercase letters: on case-insensitive file
/// systems (the macOS and Windows defaults) `Room` and `room` would share the same files.
fn validate_path_id(kind: &'static str, value: &str) -> Result<(), InvalidIdError> {
    let fail = |reason| Err(InvalidIdError::new(kind, value, reason));
    if value.is_empty() {
        return fail("must not be empty");
    }
    if value.len() > MAX_PATH_ID_LEN {
        return fail("must be at most 64 characters long");
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
    {
        return fail("may only contain lowercase letters, digits, '-' and '_'");
    }
    if WINDOWS_RESERVED_NAMES.contains(&value) {
        return fail("is a reserved device name on Windows");
    }
    Ok(())
}

/// Validates a client identifier, which is never used in file paths.
fn validate_client_id(value: &str) -> Result<(), InvalidIdError> {
    let fail = |reason| Err(InvalidIdError::new("client id", value, reason));
    if value.is_empty() {
        return fail("must not be empty");
    }
    if value.len() > MAX_CLIENT_ID_LEN {
        return fail("must be at most 256 bytes long");
    }
    if value.chars().any(char::is_control) {
        return fail("must not contain control characters");
    }
    if value.chars().any(is_invisible_or_bidi) {
        return fail("must not contain invisible, formatting or bidirectional control characters");
    }
    if value.starts_with(char::is_whitespace) || value.ends_with(char::is_whitespace) {
        return fail("must not start or end with whitespace");
    }
    Ok(())
}

/// Unicode format, invisible, bidirectional-control, filler, variation-selector, tag and
/// line/paragraph separator characters that `char::is_control` (general category Cc only) lets
/// through. They can hide text or reorder what surrounds them in logs and UIs.
///
/// This is a denylist of the characters most used for spoofing, not a guarantee that two
/// different client IDs never look alike: no Unicode normalization is applied, so for example
/// precomposed and decomposed accents remain distinct IDs. Tokens bind the exact bytes, so this
/// only affects how IDs are displayed, never authentication.
fn is_invisible_or_bidi(c: char) -> bool {
    matches!(
        c,
        '\u{00AD}'
            | '\u{061C}'
            | '\u{180E}'
            | '\u{200B}'..='\u{200F}'
            | '\u{2028}'..='\u{202E}'
            | '\u{2060}'..='\u{2064}'
            | '\u{2066}'..='\u{206F}'
            | '\u{FEFF}'
            | '\u{FFF9}'..='\u{FFFB}'
            // Combining grapheme joiner, Hangul and Khmer fillers, Mongolian selectors
            | '\u{034F}'
            | '\u{115F}'..='\u{1160}'
            | '\u{17B4}'..='\u{17B5}'
            | '\u{180B}'..='\u{180F}'
            | '\u{3164}'
            | '\u{FFA0}'
            // Remaining format characters (general category Cf)
            | '\u{0600}'..='\u{0605}'
            | '\u{06DD}'
            | '\u{070F}'
            | '\u{0890}'..='\u{0891}'
            | '\u{08E2}'
            | '\u{110BD}'
            | '\u{110CD}'
            | '\u{13430}'..='\u{1343F}'
            | '\u{1BCA0}'..='\u{1BCA3}'
            | '\u{1D173}'..='\u{1D17A}'
            // Variation selectors and tag characters
            | '\u{FE00}'..='\u{FE0F}'
            | '\u{E0000}'..='\u{E007F}'
            | '\u{E0100}'..='\u{E01EF}'
    )
}

/// Defines a string identifier newtype that can only hold values accepted by `$validate`.
///
/// Construction (`new`, `TryFrom`) and deserialization both validate, so an invalid
/// identifier cannot exist anywhere in the program, whatever its source.
macro_rules! validated_string_id {
    ($(#[$meta:meta])* $name:ident, $validate:expr) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            /// Validates `id` and wraps it.
            pub fn new(id: impl Into<String>) -> Result<Self, InvalidIdError> {
                let id = id.into();
                ($validate)(id.as_str())?;
                Ok(Self(id))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl TryFrom<String> for $name {
            type Error = InvalidIdError;

            fn try_from(s: String) -> Result<Self, Self::Error> {
                Self::new(s)
            }
        }

        impl TryFrom<&str> for $name {
            type Error = InvalidIdError;

            fn try_from(s: &str) -> Result<Self, Self::Error> {
                Self::new(s)
            }
        }

        impl std::str::FromStr for $name {
            type Err = InvalidIdError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::new(s)
            }
        }

        impl From<$name> for String {
            fn from(id: $name) -> Self {
                id.0
            }
        }
    };
}

validated_string_id!(
    /// Strongly typed identifier for a collaborative Room.
    ///
    /// Used as a file and directory name: lowercase letters, digits, `-` and `_`, 1 to 64
    /// characters, and not a Windows reserved device name.
    RoomId,
    |value: &str| validate_path_id("room id", value)
);

validated_string_id!(
    /// Strongly typed identifier for a Schema definition or template.
    ///
    /// Used as a file name: lowercase letters, digits, `-` and `_`, 1 to 64 characters, and
    /// not a Windows reserved device name.
    SchemaId,
    |value: &str| validate_path_id("schema id", value)
);

validated_string_id!(
    /// Strongly typed identifier for a participating Client.
    ///
    /// Never used in file paths: any text of 1 to 256 bytes without control characters or
    /// invisible, formatting and bidirectional control characters.
    ClientId,
    validate_client_id
);

/// Strictly monotonic sequence number per Room assigned by the server coordinator.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct SequenceNumber(u64);

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
pub struct MutationId([u8; 16]);

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

impl AsRef<[u8]> for MutationId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8; 16]> for MutationId {
    fn as_ref(&self) -> &[u8; 16] {
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
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
#[serde(transparent)]
pub struct CorrelationId(u64);

impl CorrelationId {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub const fn get(&self) -> u64 {
        self.0
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
