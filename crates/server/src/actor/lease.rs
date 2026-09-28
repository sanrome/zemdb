use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use rimdb_core::id::{ClientId, SequenceNumber};
use serde::{Deserialize, Serialize};

use crate::error::ServerError;

/// Tri-state lifecycle status of a registered room client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientState {
    /// Actively connected and communicating via heartbeats or commits.
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

    /// Registers a client or marks an existing one as Connected.
    pub fn register_client(
        &mut self,
        client_id: &ClientId,
        current_head: SequenceNumber,
    ) -> Result<(), ServerError> {
        if let Some(entry) = self.clients.get_mut(client_id) {
            entry.state = ClientState::Connected;
            entry.last_heartbeat = Instant::now();
            // If client was dormant, registration with head_seq re-anchors its cursor
            if entry.state == ClientState::Dormant {
                entry.last_ack_seq = current_head;
            }
        } else {
            self.clients.insert(
                client_id.clone(),
                ClientEntry {
                    client_id: client_id.clone(),
                    state: ClientState::Connected,
                    last_ack_seq: current_head,
                    last_heartbeat: Instant::now(),
                },
            );
        }

        self.save()
    }

    /// Records an explicit acknowledgment of applied sequences from a client,
    /// advancing its cursor, resetting its lease timer, and persisting to disk.
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
    /// - `Connected` -> `Disconnected` when lease expires without heartbeat.
    /// - `Disconnected` -> `Dormant` when its cursor falls behind `tail_seq - 1`.
    pub fn check_timeouts(&mut self, lease_timeout: Duration, tail_seq: SequenceNumber) -> bool {
        let mut modified = false;
        let now = Instant::now();

        for entry in self.clients.values_mut() {
            match entry.state {
                ClientState::Connected => {
                    if now.duration_since(entry.last_heartbeat) > lease_timeout {
                        entry.state = ClientState::Disconnected;
                        modified = true;
                    }
                }
                ClientState::Disconnected => {
                    if tail_seq.get() > 0
                        && entry.last_ack_seq.get() < tail_seq.get().saturating_sub(1)
                    {
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

        // Find min ack across Connected clients (ignoring Dormant)
        self.clients
            .values()
            .filter(|c| c.state == ClientState::Connected)
            .map(|c| c.last_ack_seq)
            .min()
    }

    /// Checks if a client is in the `Dormant` state.
    pub fn is_dormant(&self, client_id: &ClientId) -> bool {
        self.clients
            .get(client_id)
            .is_some_and(|c| c.state == ClientState::Dormant)
    }

    /// Returns a reference to a client entry if registered.
    pub fn get_client(&self, client_id: &ClientId) -> Option<&ClientEntry> {
        self.clients.get(client_id)
    }

    /// Returns (connected, disconnected, dormant, total) client counts.
    pub fn client_counts(&self) -> (usize, usize, usize, usize) {
        let mut connected = 0;
        let mut disconnected = 0;
        let mut dormant = 0;

        for entry in self.clients.values() {
            match entry.state {
                ClientState::Connected => connected += 1,
                ClientState::Disconnected => disconnected += 1,
                ClientState::Dormant => dormant += 1,
            }
        }

        (connected, disconnected, dormant, self.clients.len())
    }
}
