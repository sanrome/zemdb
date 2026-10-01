use zemdb_core::id::{ClientId, SequenceNumber};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::error::ServerError;

/// Lifecycle status of a registered room client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientState {
    /// Actively bootstrapping (downloading/applying a base snapshot).
    /// Holds an active lease and emits heartbeats, but does NOT block proactive log pruning.
    Bootstrapping,
    /// Actively connected and communicating via heartbeats, syncs, or commits.
    Connected,
    /// Offline or disconnected, but its cursor is still within the server's retained logs.
    Disconnected,
    /// Offline so long that its pending deltas were pruned from disk (last_ack_seq < tail_seq - 1).
    /// Requires a full base snapshot to catch up.
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

/// Persistent tracker for client leases, cursors, and room membership.
#[derive(Debug)]
pub struct ClientLeaseTracker {
    path: PathBuf,
    clients: HashMap<ClientId, ClientEntry>,
}

impl ClientLeaseTracker {
    /// Opens an existing client metadata file or creates a new empty tracker.
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self, ServerError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let mut clients = HashMap::new();

        if path.exists() {
            let content = fs::read_to_string(&path)?;
            if !content.trim().is_empty() {
                let entries: Vec<ClientEntry> = serde_json::from_str(&content).map_err(|e| {
                    ServerError::Serialization(format!(
                        "Failed to parse clients roster from {}: {}",
                        path.display(),
                        e
                    ))
                })?;
                for mut entry in entries {
                    entry.last_heartbeat = Instant::now();
                    clients.insert(entry.client_id.clone(), entry);
                }
            }
        }

        Ok(Self { path, clients })
    }

    /// Atomically persists the client roster to disk.
    pub fn save(&self) -> Result<(), ServerError> {
        let tmp_path = self.path.with_extension("tmp");
        let entries: Vec<&ClientEntry> = self.clients.values().collect();
        let json = serde_json::to_string_pretty(&entries).map_err(|e| {
            ServerError::Serialization(format!("Failed to serialize clients roster: {}", e))
        })?;

        fs::write(&tmp_path, json.as_bytes())?;
        fs::rename(&tmp_path, &self.path)?;
        Ok(())
    }

    /// Registers a client, setting its initial state based on its current cursor vs tail_seq:
    /// - If `current_seq` is behind `tail_seq - 1` (or None in a pruned room), client enters `Bootstrapping`.
    /// - Otherwise, client enters `Connected` with its actual acknowledged sequence.
    pub fn register_client(
        &mut self,
        client_id: &ClientId,
        current_seq: Option<SequenceNumber>,
        tail_seq: SequenceNumber,
    ) -> Result<ClientState, ServerError> {
        let is_behind = match current_seq {
            Some(seq) => tail_seq.get() > 0 && seq.get() < tail_seq.get().saturating_sub(1),
            None => tail_seq.get() > 1,
        };

        let initial_state = if is_behind {
            ClientState::Bootstrapping
        } else {
            ClientState::Connected
        };

        let last_ack = current_seq.unwrap_or(SequenceNumber::new(0));

        if let Some(entry) = self.clients.get_mut(client_id) {
            entry.state = initial_state;
            entry.last_heartbeat = Instant::now();
            entry.last_ack_seq = last_ack;
        } else {
            self.clients.insert(
                client_id.clone(),
                ClientEntry {
                    client_id: client_id.clone(),
                    state: initial_state,
                    last_ack_seq: last_ack,
                    last_heartbeat: Instant::now(),
                },
            );
        }

        self.save()?;
        Ok(initial_state)
    }

    /// Records an explicit acknowledgment of applied sequences from a client,
    /// advancing its cursor, resetting its lease timer, and persisting to disk.
    /// Acknowledgment promotes a Bootstrapping client to Connected.
    pub fn record_ack(
        &mut self,
        client_id: &ClientId,
        ack_seq: SequenceNumber,
    ) -> Result<(), ServerError> {
        if let Some(entry) = self.clients.get_mut(client_id) {
            entry.state = ClientState::Connected;
            entry.last_heartbeat = Instant::now();
            if ack_seq > entry.last_ack_seq {
                entry.last_ack_seq = ack_seq;
            }
        } else {
            self.clients.insert(
                client_id.clone(),
                ClientEntry {
                    client_id: client_id.clone(),
                    state: ClientState::Connected,
                    last_ack_seq: ack_seq,
                    last_heartbeat: Instant::now(),
                },
            );
        }

        self.save()
    }

    /// Records a heartbeat for a client, resetting its lease timer and marking it Connected.
    /// Does NOT modify the client's acknowledged sequence cursor.
    pub fn record_heartbeat(&mut self, client_id: &ClientId) -> Result<(), ServerError> {
        if let Some(entry) = self.clients.get_mut(client_id) {
            let was_disconnected = entry.state == ClientState::Disconnected;
            entry.state = ClientState::Connected;
            entry.last_heartbeat = Instant::now();
            if was_disconnected {
                return self.save();
            }
            Ok(())
        } else {
            Err(ServerError::Internal(format!(
                "Client {} not registered",
                client_id
            )))
        }
    }

    /// Records client activity without modifying its acknowledged sequence.
    pub fn record_activity(&mut self, client_id: &ClientId) {
        if let Some(entry) = self.clients.get_mut(client_id) {
            entry.state = ClientState::Connected;
            entry.last_heartbeat = Instant::now();
        }
    }

    /// Explicitly deregisters a client from the room roster.
    pub fn deregister_client(&mut self, client_id: &ClientId) -> Result<bool, ServerError> {
        let removed = self.clients.remove(client_id).is_some();
        if removed {
            self.save()?;
        }
        Ok(removed)
    }

    /// Evaluates timeouts for all registered clients:
    /// - `Connected` / `Bootstrapping` -> `Disconnected` when lease expires without heartbeat.
    /// - `Disconnected` -> `Dormant` when its cursor falls behind `tail_seq - 1` or exceeds 90s inactivity.
    pub fn check_timeouts(&mut self, lease_timeout: Duration, tail_seq: SequenceNumber) -> bool {
        let mut modified = false;
        let now = Instant::now();
        let max_disconnected_duration = Duration::from_secs(90);

        for entry in self.clients.values_mut() {
            match entry.state {
                ClientState::Connected | ClientState::Bootstrapping => {
                    if now.duration_since(entry.last_heartbeat) > lease_timeout {
                        entry.state = ClientState::Disconnected;
                        modified = true;
                    }
                }
                ClientState::Disconnected => {
                    let fallen_behind = tail_seq.get() > 0
                        && entry.last_ack_seq.get() < tail_seq.get().saturating_sub(1);
                    let disconnected_timed_out =
                        now.duration_since(entry.last_heartbeat) > max_disconnected_duration;

                    if fallen_behind || disconnected_timed_out {
                        entry.state = ClientState::Dormant;
                        modified = true;
                    }
                }
                ClientState::Dormant => {}
            }
        }

        if modified {
            let _ = self.save();
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

    /// Checks if a client is in the `Bootstrapping` state.
    pub fn is_bootstrapping(&self, client_id: &ClientId) -> bool {
        self.clients
            .get(client_id)
            .is_some_and(|c| c.state == ClientState::Bootstrapping)
    }

    /// Checks if a client is in the `Dormant` state.
    pub fn is_dormant(&self, client_id: &ClientId) -> bool {
        self.clients
            .get(client_id)
            .is_some_and(|c| c.state == ClientState::Dormant)
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
