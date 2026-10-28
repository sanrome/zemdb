/// The configuration of a [`Client`](crate::Client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    /// The person the client writes as, the part of its client id before `#`: the id the
    /// backend issues the person's tokens with. Not empty, and without `#`.
    pub person: String,
}

impl ClientConfig {
    /// The configuration of a client of `person`.
    pub fn new(person: impl Into<String>) -> Self {
        Self {
            person: person.into(),
        }
    }
}
