use dashmap::DashMap;
use zemdb_core::schema::{ColumnDef, Schema};
use zemdb_core::SchemaId;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::ServerError;

/// Thread-safe registry for room schemas with persistent disk storage.
#[derive(Debug)]
pub struct SchemaRegistry {
    dir: PathBuf,
    schemas: DashMap<SchemaId, Arc<Schema>>,
}

impl SchemaRegistry {
    /// Opens or creates the schema registry at the given directory, loading any existing schema files.
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self, ServerError> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        let schemas = DashMap::new();

        // Scan directory for {schema_id}.json files
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("json") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    let schema_id = SchemaId::new(stem);
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

        Ok(Self { dir, schemas })
    }

    /// Registers a new schema or updates an existing one, persisting it to disk atomically.
    pub fn register_schema(
        &self,
        id: SchemaId,
        schema: Schema,
    ) -> Result<Arc<Schema>, ServerError> {
        let path = self.dir.join(format!("{}.json", id.as_str()));
        let tmp_path = self.dir.join(format!("{}.json.tmp", id.as_str()));

        let json = serde_json::to_string_pretty(&schema).map_err(|e| {
            ServerError::Serialization(format!("Failed to serialize schema {}: {}", id, e))
        })?;

        fs::write(&tmp_path, json.as_bytes())?;
        fs::rename(&tmp_path, &path)?;

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
        let existing = self
            .get_schema(id)
            .ok_or_else(|| ServerError::SchemaNotFound(id.to_string()))?;

        let mut evolved_schema = (*existing).clone();
        let table_id = evolved_schema.get_table_id(table_name).ok_or_else(|| {
            ServerError::SchemaViolation(format!("Table '{}' not found", table_name))
        })?;

        let table = evolved_schema
            .tables_by_id
            .get_mut(&table_id)
            .ok_or_else(|| {
                ServerError::SchemaViolation(format!("Table ID '{}' not found", table_id))
            })?;

        // add_column in TableSchema enforces nullable: true and column name uniqueness
        table
            .add_column(column)
            .map_err(|e| ServerError::SchemaViolation(e.to_string()))?;

        self.register_schema(id.clone(), evolved_schema)
    }

    /// Lists all registered SchemaIds.
    pub fn list_schemas(&self) -> Vec<SchemaId> {
        self.schemas.iter().map(|kv| kv.key().clone()).collect()
    }
}
