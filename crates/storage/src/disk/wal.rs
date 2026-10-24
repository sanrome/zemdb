use zemdb_core::SequencedOperation;

use crate::disk::format::{decode_wal_batch_from_slice, encode_wal_batch, WalBatchDecodeResult};
use crate::error::StorageError;
use crate::files::RoomFile;

/// Encoder of append-only WAL records, and their writer to a file of the file layer.
#[derive(Debug, Default, Clone, Copy)]
pub struct WalWriter;

impl WalWriter {
    /// Encodes a single sequenced operation and writes it at `offset` (see
    /// [`Self::write_batch`]).
    pub async fn write_record(
        file: &impl RoomFile,
        offset: u64,
        op: &SequencedOperation,
    ) -> Result<usize, StorageError> {
        Self::write_batch(file, offset, std::slice::from_ref(op)).await
    }

    /// Encodes a batch of sequenced operations and writes it to the given WAL file at `offset`,
    /// its end for an append. Returns the number of bytes written.
    ///
    /// The write is complete when this returns, and its error, if any, is returned here; making
    /// it durable is up to the caller.
    pub async fn write_batch(
        file: &impl RoomFile,
        offset: u64,
        ops: &[SequencedOperation],
    ) -> Result<usize, StorageError> {
        let buffer = Self::encode_batch(ops)?;
        let len = buffer.len();
        file.write_at(offset, buffer).await?;
        Ok(len)
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
    pub fn new(slice: &'a [u8]) -> Self {
        Self { slice, offset: 0 }
    }

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
}
