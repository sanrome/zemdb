use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::warn;
use zemdb_core::id::{ClientId, SequenceNumber};

use crate::durable;
use crate::error::ServerError;
use crate::log::retention::is_behind_tail;

/// Lifecycle status of a registered room client.
///
/// Every activity of a client recomputes its state from its cursor: a cursor behind the
/// retained log (`cursor < tail_seq - 1`) makes it `Bootstrapping`, any other makes it
/// `Connected`. Only the maintenance tick moves a client to `Disconnected` or `Dormant`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientState {
    /// Active, but its cursor is behind the retained log: it needs a base snapshot.
    /// Holds an active lease, but does NOT block proactive log pruning.
    Bootstrapping,
    /// Active, with its cursor inside the retained log.
    Connected,
    /// Its lease expired without activity, but its cursor is still within the retained log.
    /// Blocks proactive log pruning, since it can still catch up from the log.
    Disconnected,
    /// Inactive and behind the retained log (or, when configured, inactive for longer than
    /// the dormancy timeout). Does not block pruning; on its next activity the cursor decides
    /// whether it is `Connected` or `Bootstrapping`.
    Dormant,
}

/// Metadata entry tracked for each registered client in a room.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientEntry {
    pub client_id: ClientId,
    pub state: ClientState,
    pub last_ack_seq: SequenceNumber,
    #[serde(skip, default = "Instant::now")]
    pub last_heartbeat: Instant,
}

/// Persistent tracker for client leases, cursors, and room membership, which also keeps the
/// room's snapshot demand on disk.
///
/// Membership changes (register, deregister) are written to disk immediately. Cursor, state
/// and snapshot demand changes only update memory and mark the roster dirty; the owner flushes
/// them with `persist_if_dirty`. Losing an unflushed cursor advance only makes the server
/// retain more log.
#[derive(Debug)]
pub struct ClientLeaseTracker {
    path: PathBuf,
    clients: HashMap<ClientId, ClientEntry>,
    /// Wall-clock time of the last renewal of the room's snapshot demand, if it is on.
    snapshot_demand: Option<SystemTime>,
    dirty: bool,
}

impl ClientLeaseTracker {
    /// Opens an existing client metadata file or creates a new empty tracker.
    ///
    /// An entry that cannot be parsed is skipped with a warning; a roster that cannot be
    /// parsed at all is discarded with a warning and the tracker starts empty. Dropped
    /// clients register again, and until they do no client cursor allows proactive pruning,
    /// which is the safe direction. An invalid snapshot demand is dropped with a warning (the
    /// next client that needs a snapshot turns it on again). Errors reading the file are
    /// still reported.
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self, ServerError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            durable::create_dir_all_synced(parent)?;
        }

        match fs::remove_file(durable::tmp_path_for(&path)) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(e.into()),
        };
        let (clients, snapshot_demand) = if content.trim().is_empty() {
            (HashMap::new(), None)
        } else {
            parse_roster(&path, &content)
        };

        Ok(Self {
            path,
            clients,
            snapshot_demand,
            dirty: false,
        })
    }

    /// Atomically and durably persists the client roster to disk, clearing the dirty mark.
    pub fn save(&mut self) -> Result<(), ServerError> {
        self.write_roster(self.clients.values().collect())?;
        self.dirty = false;
        Ok(())
    }

    /// Writes `clients`, with the current snapshot demand, as the roster file. Used to persist
    /// a roster change before applying it in memory, so that a failed write leaves the
    /// tracker unchanged.
    fn write_roster(&self, clients: Vec<&ClientEntry>) -> Result<(), ServerError> {
        let roster = RosterFile {
            clients,
            snapshot_demand: self.snapshot_demand.map(PersistedDemand::from_time),
        };
        let json = serde_json::to_string_pretty(&roster).map_err(|e| {
            ServerError::Serialization(format!("Failed to serialize clients roster: {}", e))
        })?;
        durable::write_atomic(&self.path, json.as_bytes())?;
        Ok(())
    }

    /// Persists the roster if it changed since the last successful save.
    pub fn persist_if_dirty(&mut self) -> Result<(), ServerError> {
        if self.dirty {
            self.save()?;
        }
        Ok(())
    }

    /// Wall-clock time at which the room's snapshot demand was last renewed, if it is on.
    pub fn snapshot_demand(&self) -> Option<SystemTime> {
        self.snapshot_demand
    }

    /// Records the room's snapshot demand: the time of its last renewal, or `None` once it
    /// is off. A change only marks the roster dirty.
    pub fn set_snapshot_demand(&mut self, renewed_at: Option<SystemTime>) {
        if self.snapshot_demand != renewed_at {
            self.snapshot_demand = renewed_at;
            self.dirty = true;
        }
    }

    /// Records a renewal of the room's snapshot demand at the wall-clock time `now`. The stored
    /// renewal time only moves once it is `granularity` old, so frequent renewals do not
    /// rewrite the roster on every maintenance tick.
    pub fn renew_snapshot_demand(&mut self, now: SystemTime, granularity: Duration) {
        let stale = match self.snapshot_demand {
            None => true,
            // A stored time in the future (the clock moved backwards) is replaced as well.
            Some(stored) => now
                .duration_since(stored)
                .map_or(true, |age| age >= granularity),
        };
        if stale {
            self.set_snapshot_demand(Some(now));
        }
    }

    /// Registers a client, setting its initial state based on its current cursor vs tail_seq:
    /// - If `current_seq` is behind `tail_seq - 1` (or None in a pruned room), client enters `Bootstrapping`.
    /// - Otherwise, client enters `Connected` with its actual acknowledged sequence.
    ///
    /// The roster is written with the new entry first; only then does the tracker change, so a
    /// failed write leaves the client as it was (unregistered, or with its previous entry).
    pub fn register_client(
        &mut self,
        client_id: &ClientId,
        current_seq: Option<SequenceNumber>,
        tail_seq: SequenceNumber,
    ) -> Result<ClientState, ServerError> {
        let last_ack = current_seq.unwrap_or(SequenceNumber::new(0));
        let initial_state = if is_behind_tail(last_ack, tail_seq) {
            ClientState::Bootstrapping
        } else {
            ClientState::Connected
        };

        let entry = ClientEntry {
            client_id: client_id.clone(),
            state: initial_state,
            last_ack_seq: last_ack,
            last_heartbeat: Instant::now(),
        };
        let roster = self
            .clients
            .values()
            .filter(|other| &other.client_id != client_id)
            .chain(std::iter::once(&entry))
            .collect();
        self.write_roster(roster)?;

        // The file now holds the whole in-memory roster, including any pending change.
        self.clients.insert(client_id.clone(), entry);
        self.dirty = false;
        Ok(initial_state)
    }

    /// Records activity of a registered client and recomputes its lifecycle state from its
    /// cursor: the stored cursor, moved forward to `reported_cursor` if that is ahead (a
    /// cursor never moves backwards).
    ///
    /// - Behind the retained log (`cursor < tail_seq - 1`): the client becomes `Bootstrapping`
    ///   and its cursor is left unchanged.
    /// - Otherwise: the client becomes `Connected` and its cursor advances.
    ///
    /// Either way the lease is refreshed. Returns the new state, or `None` for a client that
    /// is not registered, which is never added to the roster. Changes only mark the roster
    /// dirty.
    pub fn observe(
        &mut self,
        client_id: &ClientId,
        reported_cursor: Option<SequenceNumber>,
        tail_seq: SequenceNumber,
    ) -> Option<ClientState> {
        let entry = self.clients.get_mut(client_id)?;
        let cursor = match reported_cursor {
            Some(reported) => reported.max(entry.last_ack_seq),
            None => entry.last_ack_seq,
        };
        let state = if is_behind_tail(cursor, tail_seq) {
            ClientState::Bootstrapping
        } else {
            ClientState::Connected
        };

        entry.last_heartbeat = Instant::now();
        let mut changed = entry.state != state;
        entry.state = state;
        if state == ClientState::Connected && cursor != entry.last_ack_seq {
            entry.last_ack_seq = cursor;
            changed = true;
        }
        if changed {
            self.dirty = true;
        }
        Some(state)
    }

    /// Explicitly deregisters a client from the room roster. As for registration, the roster
    /// is written before the client is removed from memory. Returns whether it was registered.
    pub fn deregister_client(&mut self, client_id: &ClientId) -> Result<bool, ServerError> {
        if !self.clients.contains_key(client_id) {
            return Ok(false);
        }
        let roster = self
            .clients
            .values()
            .filter(|other| &other.client_id != client_id)
            .collect();
        self.write_roster(roster)?;

        self.clients.remove(client_id);
        self.dirty = false;
        Ok(true)
    }

    /// Evaluates timeouts for all registered clients (changes only mark the roster dirty):
    /// - `Connected` / `Bootstrapping` -> `Disconnected` when the lease expires without activity.
    /// - `Disconnected` -> `Dormant` when its cursor falls behind `tail_seq - 1`, or, only if
    ///   `dormant_after` is set, once it has been inactive for longer than that.
    pub fn check_timeouts(
        &mut self,
        lease_timeout: Duration,
        dormant_after: Option<Duration>,
        tail_seq: SequenceNumber,
    ) -> bool {
        let mut modified = false;
        let now = Instant::now();

        for entry in self.clients.values_mut() {
            let inactive_for = now.saturating_duration_since(entry.last_heartbeat);
            match entry.state {
                ClientState::Connected | ClientState::Bootstrapping => {
                    if inactive_for > lease_timeout {
                        entry.state = ClientState::Disconnected;
                        modified = true;
                    }
                }
                ClientState::Disconnected => {
                    let fallen_behind = is_behind_tail(entry.last_ack_seq, tail_seq);
                    let dormant_by_time = dormant_after.is_some_and(|limit| inactive_for > limit);
                    if fallen_behind || dormant_by_time {
                        entry.state = ClientState::Dormant;
                        modified = true;
                    }
                }
                ClientState::Dormant => {}
            }
        }

        if modified {
            self.dirty = true;
        }

        modified
    }

    /// Returns the minimum cursor among all registered clients ONLY IF all non-dormant clients
    /// are currently `Connected`. If any non-dormant client is `Disconnected`, returns `None`
    /// to avoid prematurely truncating logs that the disconnected client may still need.
    /// Clients in `Bootstrapping` do not block proactive pruning.
    pub fn min_connected_ack_seq(&self) -> Option<SequenceNumber> {
        if self.clients.is_empty() {
            return None;
        }

        // If any non-dormant member is Disconnected, do not prune proactively
        let any_disconnected = self
            .clients
            .values()
            .any(|c| c.state == ClientState::Disconnected);
        if any_disconnected {
            return None;
        }

        // Find min ack across Connected clients (ignoring Dormant and Bootstrapping)
        self.clients
            .values()
            .filter(|c| c.state == ClientState::Connected)
            .map(|c| c.last_ack_seq)
            .min()
    }

    /// Checks if a client is registered in the room roster.
    pub fn is_registered(&self, client_id: &ClientId) -> bool {
        self.clients.contains_key(client_id)
    }

    /// Checks if a client is currently in the `Connected` state.
    pub fn is_connected(&self, client_id: &ClientId) -> bool {
        self.clients
            .get(client_id)
            .is_some_and(|c| c.state == ClientState::Connected)
    }

    /// Returns a reference to a client entry if registered.
    pub fn get_client(&self, client_id: &ClientId) -> Option<&ClientEntry> {
        self.clients.get(client_id)
    }

    /// Returns a mutable reference to a client entry if registered.
    pub fn get_client_mut(&mut self, client_id: &ClientId) -> Option<&mut ClientEntry> {
        self.clients.get_mut(client_id)
    }

    /// Chooses the client that should upload a snapshot: among `Connected` clients not in
    /// `excluded`, the one with the highest cursor, then the most recent activity, then the
    /// lowest client id (so the choice is deterministic).
    pub fn pick_uploader(&self, excluded: &HashSet<ClientId>) -> Option<ClientId> {
        self.clients
            .values()
            .filter(|c| c.state == ClientState::Connected && !excluded.contains(&c.client_id))
            .max_by(|a, b| {
                a.last_ack_seq
                    .cmp(&b.last_ack_seq)
                    .then(a.last_heartbeat.cmp(&b.last_heartbeat))
                    .then(b.client_id.cmp(&a.client_id))
            })
            .map(|c| c.client_id.clone())
    }

    /// Returns (bootstrapping, connected, disconnected, dormant, total) client counts.
    pub fn client_counts(&self) -> (usize, usize, usize, usize, usize) {
        let mut bootstrapping = 0;
        let mut connected = 0;
        let mut disconnected = 0;
        let mut dormant = 0;

        for entry in self.clients.values() {
            match entry.state {
                ClientState::Bootstrapping => bootstrapping += 1,
                ClientState::Connected => connected += 1,
                ClientState::Disconnected => disconnected += 1,
                ClientState::Dormant => dormant += 1,
            }
        }

        (
            bootstrapping,
            connected,
            disconnected,
            dormant,
            self.clients.len(),
        )
    }
}

/// On-disk layout of the roster file. Files written before the snapshot demand was persisted
/// hold only the array of clients, and still load.
#[derive(Serialize)]
struct RosterFile<'a> {
    clients: Vec<&'a ClientEntry>,
    snapshot_demand: Option<PersistedDemand>,
}

/// The room's snapshot demand on disk: when it was last renewed, in wall-clock milliseconds
/// since the Unix epoch (an `Instant` does not survive a restart).
#[derive(Debug, Serialize, Deserialize)]
struct PersistedDemand {
    renewed_at_unix_ms: u64,
}

impl PersistedDemand {
    fn from_time(time: SystemTime) -> Self {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .map(|since| since.as_millis())
            .unwrap_or(0);
        Self {
            renewed_at_unix_ms: u64::try_from(millis).unwrap_or(u64::MAX),
        }
    }

    fn to_time(&self) -> Option<SystemTime> {
        UNIX_EPOCH.checked_add(Duration::from_millis(self.renewed_at_unix_ms))
    }
}

/// Parses a roster file: the current object (`clients` and `snapshot_demand`) or the earlier
/// array of clients.
///
/// Entries are parsed one by one so that a single invalid entry (for example a client id that
/// no longer passes validation) only drops that client, and an invalid snapshot demand only
/// drops the demand. A file whose clients cannot be found at all yields an empty roster. Each
/// loss is logged as a warning.
fn parse_roster(
    path: &Path,
    content: &str,
) -> (HashMap<ClientId, ClientEntry>, Option<SystemTime>) {
    let unreadable = |error: &dyn std::fmt::Display| {
        warn!(
            path = %path.display(),
            error = %error,
            "Unreadable clients roster; starting with an empty roster"
        );
        (HashMap::new(), None)
    };
    let (raw_entries, raw_demand) = match serde_json::from_str::<serde_json::Value>(content) {
        Ok(serde_json::Value::Array(entries)) => (entries, None),
        Ok(serde_json::Value::Object(mut object)) => match object.remove("clients") {
            Some(serde_json::Value::Array(entries)) => (entries, object.remove("snapshot_demand")),
            _ => return unreadable(&"missing clients array"),
        },
        Ok(_) => return unreadable(&"not a roster"),
        Err(e) => return unreadable(&e),
    };

    let mut clients = HashMap::new();
    for raw in raw_entries {
        match serde_json::from_value::<ClientEntry>(raw) {
            Ok(mut entry) => {
                entry.last_heartbeat = Instant::now();
                clients.insert(entry.client_id.clone(), entry);
            }
            Err(e) => {
                warn!(
                    path = %path.display(),
                    error = %e,
                    "Skipping invalid entry in clients roster"
                );
            }
        }
    }

    let snapshot_demand = match raw_demand {
        None | Some(serde_json::Value::Null) => None,
        Some(raw) => match serde_json::from_value::<PersistedDemand>(raw)
            .ok()
            .and_then(|demand| demand.to_time())
        {
            Some(renewed_at) => Some(renewed_at),
            None => {
                warn!(
                    path = %path.display(),
                    "Invalid snapshot demand in clients roster; treating it as off"
                );
                None
            }
        },
    };

    (clients, snapshot_demand)
}

#[cfg(test)]
#[path = "tests/lease.rs"]
mod tests;
