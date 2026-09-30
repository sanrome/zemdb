use rimdb_core::SequencedOperation;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

use crate::disk::format::{
    decode_wal_batch_from_slice, encode_wal_batch, WalBatchDecodeResult, WalDecodeResult,
};
use crate::error::StorageError;

/// Sequential writer and encoder for append-only WAL records.
#[derive(Debug, Default, Clone, Copy)]
pub struct WalWriter;

impl WalWriter {
    /// Encodes and appends a single sequenced operation to the given WAL file.
    pub async fn write_record(
        file: &mut File,
        op: &SequencedOperation,
    ) -> Result<usize, StorageError> {
        let bytes = encode_wal_batch(std::slice::from_ref(op), None)?;
        file.write_all(&bytes).await?;
        Ok(bytes.len())
    }

    /// Encodes and appends a batch of sequenced operations to the given WAL file.
    pub async fn write_batch(
        file: &mut File,
        ops: &[SequencedOperation],
    ) -> Result<usize, StorageError> {
        let buffer = Self::encode_batch(ops)?;
        file.write_all(&buffer).await?;
        Ok(buffer.len())
    }

    /// Encodes a batch of sequenced operations into an atomically framed contiguous byte buffer.
    pub fn encode_batch(ops: &[SequencedOperation]) -> Result<Vec<u8>, StorageError> {
        encode_wal_batch(ops, None)
    }
}

use std::collections::VecDeque;

/// Sequential reader for reading WAL records from an in-memory byte slice.
#[derive(Debug)]
pub struct WalReader<'a> {
    slice: &'a [u8],
    offset: usize,
    pending_ops: VecDeque<SequencedOperation>,
}

impl<'a> WalReader<'a> {
    /// Creates a new `WalReader` positioned at the start of the byte slice.
    pub fn new(slice: &'a [u8]) -> Self {
        Self {
            slice,
            offset: 0,
            pending_ops: VecDeque::new(),
        }
    }

    /// Returns the current byte offset within the slice.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Reads the next batch from the current offset.
    pub fn next_batch(&mut self) -> Result<WalBatchDecodeResult, StorageError> {
        if self.offset >= self.slice.len() {
            return Ok(WalBatchDecodeResult::CleanEof);
        }

        let result = decode_wal_batch_from_slice(&self.slice[self.offset..])?;
        if let WalBatchDecodeResult::Ok { bytes_consumed, .. } = &result {
            self.offset += bytes_consumed;
        }
        Ok(result)
    }

    /// Reads the next single record from the current offset, buffering remaining operations in multi-op batches.
    pub fn next_record(&mut self) -> Result<WalDecodeResult, StorageError> {
        if let Some(op) = self.pending_ops.pop_front() {
            return Ok(WalDecodeResult::Ok {
                op,
                mutation_id: None,
                bytes_consumed: 0,
            });
        }

        if self.offset >= self.slice.len() {
            return Ok(WalDecodeResult::CleanEof);
        }

        match self.next_batch()? {
            WalBatchDecodeResult::Ok {
                mut ops,
                mutation_id,
                bytes_consumed,
            } => {
                if ops.is_empty() {
                    return Err(StorageError::WalCorruption("Empty WAL batch".to_string()));
                }
                let first = ops.remove(0);
                self.pending_ops.extend(ops);
                Ok(WalDecodeResult::Ok {
                    op: first,
                    mutation_id,
                    bytes_consumed,
                })
            }
            WalBatchDecodeResult::CleanEof => Ok(WalDecodeResult::CleanEof),
            WalBatchDecodeResult::TornWrite {
                valid_bytes_offset,
                reason,
            } => Ok(WalDecodeResult::TornWrite {
                valid_bytes_offset,
                reason,
            }),
        }
    }

    /// Reads all valid records sequentially until EOF or a torn-write is encountered.
    pub fn read_all(
        &mut self,
    ) -> Result<(Vec<SequencedOperation>, usize, Option<String>), StorageError> {
        let mut ops = Vec::new();
        let mut torn_write = None;

        while self.offset < self.slice.len() {
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
