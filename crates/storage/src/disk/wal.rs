use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use zemdb_core::SequencedOperation;

use crate::disk::format::{decode_wal_batch_from_slice, encode_wal_batch, WalBatchDecodeResult};
use crate::error::StorageError;

/// Sequential writer and encoder for append-only WAL records.
#[derive(Debug, Default, Clone, Copy)]
pub struct WalWriter;

impl WalWriter {
    /// Encodes and appends a single sequenced operation to the given WAL file.
    ///
    /// The write is complete when this returns, and its error, if any, is returned here; making
    /// it durable is up to the caller.
    pub async fn write_record(
        file: &mut File,
        op: &SequencedOperation,
    ) -> Result<usize, StorageError> {
        let bytes = encode_wal_batch(std::slice::from_ref(op), None)?;
        Self::write_flushed(file, &bytes).await?;
        Ok(bytes.len())
    }

    /// Encodes and appends a batch of sequenced operations to the given WAL file.
    ///
    /// The write is complete when this returns, and its error, if any, is returned here; making
    /// it durable is up to the caller.
    pub async fn write_batch(
        file: &mut File,
        ops: &[SequencedOperation],
    ) -> Result<usize, StorageError> {
        let buffer = Self::encode_batch(ops)?;
        Self::write_flushed(file, &buffer).await?;
        Ok(buffer.len())
    }

    /// Writes `bytes` and waits for the write: Tokio's `write_all` returns once the bytes are
    /// handed to the blocking pool, and only a `flush` (or a later write) reports its error.
    async fn write_flushed(file: &mut File, bytes: &[u8]) -> std::io::Result<()> {
        file.write_all(bytes).await?;
        file.flush().await
    }

    /// Encodes a batch of sequenced operations into an atomically framed contiguous byte buffer.
    pub fn encode_batch(ops: &[SequencedOperation]) -> Result<Vec<u8>, StorageError> {
        encode_wal_batch(ops, None)
    }
}

/// Sequential reader of the framed batches in an in-memory byte slice.
#[derive(Debug)]
pub struct WalReader<'a> {
    slice: &'a [u8],
    offset: usize,
}

impl<'a> WalReader<'a> {
    /// Creates a new `WalReader` positioned at the start of the byte slice.
    pub fn new(slice: &'a [u8]) -> Self {
        Self { slice, offset: 0 }
    }

    /// Returns the current byte offset within the slice.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Reads the next batch that holds operations, skipping frames without any.
    pub fn next_batch(&mut self) -> Result<WalBatchDecodeResult, StorageError> {
        loop {
            if self.offset >= self.slice.len() {
                return Ok(WalBatchDecodeResult::CleanEof);
            }

            let result = decode_wal_batch_from_slice(&self.slice[self.offset..])?;
            if let WalBatchDecodeResult::Ok {
                ops,
                bytes_consumed,
                ..
            } = &result
            {
                self.offset += bytes_consumed;
                if ops.is_empty() {
                    continue;
                }
            }
            return Ok(result);
        }
    }

    /// Reads all valid records sequentially until EOF or a torn-write is encountered.
    pub fn read_all(
        &mut self,
    ) -> Result<(Vec<SequencedOperation>, usize, Option<String>), StorageError> {
        let mut ops = Vec::new();
        let mut torn_write = None;

        loop {
            match self.next_batch()? {
                WalBatchDecodeResult::Ok { ops: batch_ops, .. } => {
                    ops.extend(batch_ops);
                }
                WalBatchDecodeResult::CleanEof => break,
                WalBatchDecodeResult::TornWrite { reason, .. } => {
                    torn_write = Some(reason);
                    break;
                }
            }
        }

        Ok((ops, self.offset, torn_write))
    }
}
