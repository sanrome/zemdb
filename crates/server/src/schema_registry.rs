use dashmap::DashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use zemdb_core::schema::{ColumnDef, Schema};
use zemdb_core::SchemaId;

use crate::blocking::blocking_io;
use crate::durable;
use crate::error::ServerError;
use crate::fail_point;

/// Thread-safe registry for room schemas with persistent disk storage.
///
/// Reads never wait: they clone the `Arc` of the current version. Writes are serialized by a
/// single registry-wide lock, held from reading the current version to publishing the new one.
/// Without it two writes to the same schema would share its `.tmp` file, and `add_column`, a
/// read-modify-write, could overwrite a column added concurrently with the copy it read
/// before. Schema writes are rare administrative operations, so one lock for every schema
/// costs nothing measurable, and unlike a lock per schema id it needs no map that grows with
/// the ids requested and has to be cleaned up.
#[derive(Debug)]
pub struct SchemaRegistry {
    dir: PathBuf,
    schemas: DashMap<SchemaId, Arc<Schema>>,
    write_lock: Mutex<()>,
}

impl SchemaRegistry {
    /// Opens or creates the schema registry at the given directory, loading any existing schema files.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, ServerError> {
        let dir = dir.into();
        durable::create_dir_all_synced(&dir)?;

        let schemas = DashMap::new();

        // Scan directory for {schema_id}.json files. Temporary files left by an interrupted
        // write are discarded; the matching `.json` still holds the last complete version.
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                fs::remove_file(&path)?;
                continue;
            }
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    // A file whose name is not a schema id was not written by the registry
                    // (macOS `._*` metadata, manual backups such as `todo.old.json`): skip it.
                    // A validly named file with corrupt content still fails the startup.
                    let schema_id = match SchemaId::new(stem) {
                        Ok(id) => id,
                        Err(err) => {
                            tracing::warn!(
                                path = %path.display(),
                                error = %err,
                                "Ignoring schema file whose name is not a valid schema id"
                            );
                            continue;
                        }
                    };
                    let content = fs::read_to_string(&path)?;
                    let schema: Schema = serde_json::from_str(&content).map_err(|e| {
                        ServerError::Serialization(format!(
                            "Failed to parse schema JSON from {}: {}",
                            path.display(),
                            e
                        ))
                    })?;
                    schemas.insert(schema_id, Arc::new(schema));
                }
            }
        }

        Ok(Self {
            dir,
            schemas,
            write_lock: Mutex::new(()),
        })
    }

    /// Path of the file that stores schema `id`.
    pub(crate) fn schema_path(&self, id: &SchemaId) -> PathBuf {
        self.dir.join(format!("{}.json", id.as_str()))
    }

    /// Takes the registry-wide write lock. It guards no data of its own: every write replaces
    /// the file atomically and publishes the new version only afterwards, so a writer that
    /// panicked left nothing half-done and the poison is ignored.
    fn lock_writes(&self) -> MutexGuard<'_, ()> {
        self.write_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Registers a new schema, persisting it to disk atomically.
    ///
    /// A schema id is declared once: an id that is already registered is
    /// `SchemaAlreadyExists`, checked under the write lock, and nothing is written. Replacing a
    /// schema could renumber its tables or drop and retype columns under rows already in the
    /// rooms' logs; schemas evolve only through [`SchemaRegistry::add_column`].
    pub fn register_schema(
        &self,
        id: SchemaId,
        schema: Schema,
    ) -> Result<Arc<Schema>, ServerError> {
        // Waiting for another writer is blocking too, so it happens inside `blocking_io`.
        blocking_io(|| {
            let _write = self.lock_writes();
            if self.schemas.contains_key(&id) {
                return Err(ServerError::SchemaAlreadyExists(id.to_string()));
            }
            self.persist(id, schema)
        })
    }

    /// Writes `schema` to disk and publishes it. The caller holds the write lock.
    fn persist(&self, id: SchemaId, schema: Schema) -> Result<Arc<Schema>, ServerError> {
        let path = self.schema_path(&id);

        let json = serde_json::to_string_pretty(&schema).map_err(|e| {
            ServerError::Serialization(format!("Failed to serialize schema {}: {}", id, e))
        })?;

        durable::write_atomic(&path, json.as_bytes())?;

        let arc_schema = Arc::new(schema);
        self.schemas.insert(id, Arc::clone(&arc_schema));
        Ok(arc_schema)
    }

    /// Retrieves an Arc reference to the schema if registered.
    pub fn get_schema(&self, id: &SchemaId) -> Option<Arc<Schema>> {
        self.schemas.get(id).map(|r| Arc::clone(r.value()))
    }

    /// Adds a nullable column to an existing table in an append-only evolution.
    pub fn add_column(
        &self,
        id: &SchemaId,
        table_name: &str,
        column: ColumnDef,
    ) -> Result<Arc<Schema>, ServerError> {
        blocking_io(|| {
            let _write = self.lock_writes();
            self.add_column_locked(id, table_name, column)
        })
    }

    fn add_column_locked(
        &self,
        id: &SchemaId,
        table_name: &str,
        column: ColumnDef,
    ) -> Result<Arc<Schema>, ServerError> {
        let existing = self
            .get_schema(id)
            .ok_or_else(|| ServerError::SchemaNotFound(id.to_string()))?;

        let mut evolved_schema = (*existing).clone();
        fail_point::hook("schema_add_column_after_read", &self.schema_path(id));
        // Rejects an unknown table, a duplicate column name and a non-nullable column.
        evolved_schema
            .add_column(table_name, column)
            .map_err(|e| ServerError::SchemaViolation(e.to_string()))?;

        self.persist(id.clone(), evolved_schema)
    }

    /// Lists all registered SchemaIds.
    pub fn list_schemas(&self) -> Vec<SchemaId> {
        self.schemas.iter().map(|kv| kv.key().clone()).collect()
    }
}

#[cfg(test)]
#[path = "tests/schema_registry.rs"]
mod tests;
