use crate::error::ServerError;
use rimdb_core::id::{ClientId, MutationId, SequenceNumber};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic bytes identifying a Micro-WAL record ("MW" = 0x4D, 0x57).
pub const MICRO_WAL_MAGIC: [u8; 2] = [0x4D, 0x57];

/// Minimum record header size: 2B magic + 8B seq + 16B mutation_id + 2B client_id_len = 28 bytes.
pub const RECORD_HEADER_SIZE: usize = 28;

/// Fixed trailing checksum size: 4B CRC32.
pub const RECORD_CRC_SIZE: usize = 4;

/// Minimum possible record size (with 0-byte client ID): 32 bytes.
pub const MIN_RECORD_SIZE: usize = RECORD_HEADER_SIZE + RECORD_CRC_SIZE;

/// A recovered entry from the Micro-WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroWalEntry {
    pub seq: SequenceNumber,
    pub mutation_id: MutationId,
    pub client_id: ClientId,
}

/// Recovery result containing the latest sequence, all recovered entries, and truncated bytes if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MicroWalRecovery {
    pub head_seq: SequenceNumber,
    pub entries: Vec<MicroWalEntry>,
    pub truncated_bytes: usize,
}

/// Append-only Micro-WAL engine for durable atomic sequence persistence.
#[derive(Debug)]
pub struct MicroWal {
    file: File,
    path: PathBuf,
    head_seq: SequenceNumber,
}

impl MicroWal {
    /// Opens an existing Micro-WAL or creates a new one, running recovery and in-place truncation if needed.
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<(Self, MicroWalRecovery), ServerError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let recovery = Self::recover(&mut file, &path)?;
        let head_seq = recovery.head_seq;

        // Position file pointer at the very end for subsequent appends
        file.seek(SeekFrom::End(0))?;

        Ok((
            Self {
                file,
                path,
                head_seq,
            },
            recovery,
        ))
    }

    /// Appends a new sequence assignment to the Micro-WAL and syncs physically to disk before returning.
    pub fn append(
        &mut self,
        seq: SequenceNumber,
        mutation_id: &MutationId,
        client_id: &ClientId,
    ) -> Result<(), ServerError> {
        if seq.get() <= self.head_seq.get() && self.head_seq.get() != 0 {
            return Err(ServerError::Wal(format!(
                "Sequence number {} must be strictly greater than head sequence {}",
                seq.get(),
                self.head_seq.get()
            )));
        }

        let client_bytes = client_id.as_str().as_bytes();
        if client_bytes.len() > u16::MAX as usize {
            return Err(ServerError::Wal("ClientId exceeds maximum u16 length".to_string()));
        }
        let client_len = client_bytes.len() as u16;

        let total_size = RECORD_HEADER_SIZE + client_bytes.len() + RECORD_CRC_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Header: [magic: 2B][seq: 8B LE][mutation_id: 16B][client_id_len: 2B LE]
        buf.extend_from_slice(&MICRO_WAL_MAGIC);
        buf.extend_from_slice(&seq.get().to_le_bytes());
        buf.extend_from_slice(mutation_id.as_bytes());
        buf.extend_from_slice(&client_len.to_le_bytes());
        buf.extend_from_slice(client_bytes);

        // CRC32 over [seq + mutation_id + client_id_len + client_bytes]
        let crc = crc32fast::hash(&buf[2..]);
        buf.extend_from_slice(&crc.to_le_bytes());

        self.file.write_all(&buf)?;
        self.file.flush()?;
        self.file.sync_data()?;

        self.head_seq = seq;
        Ok(())
    }

    /// Current highest sequence number committed to the WAL.
    pub fn head_seq(&self) -> SequenceNumber {
        self.head_seq
    }

    /// Path to the backing WAL file.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Internal recovery function that scans file content, validates CRCs, and handles torn writes at EOF.
    fn recover(file: &mut File, path: &Path) -> Result<MicroWalRecovery, ServerError> {
        file.seek(SeekFrom::Start(0))?;
        let file_len = file.metadata()?.len() as usize;
        if file_len == 0 {
            return Ok(MicroWalRecovery {
                head_seq: SequenceNumber::new(0),
                entries: Vec::new(),
                truncated_bytes: 0,
            });
        }

        let mut data = Vec::with_capacity(file_len);
        std::io::Read::read_to_end(file, &mut data)?;

        let mut offset = 0;
        let mut head_seq = SequenceNumber::new(0);
        let mut entries = Vec::new();
        let mut truncated_bytes = 0;

        while offset < data.len() {
            let remaining = &data[offset..];

            // Check if remaining bytes are incomplete header
            if remaining.len() < RECORD_HEADER_SIZE {
                // If it's trailing zeros or incomplete write at EOF -> Torn write
                truncated_bytes = remaining.len();
                tracing::warn!(
                    path = ?path,
                    offset = offset,
                    truncated_bytes = truncated_bytes,
                    "Incomplete Micro-WAL record header at EOF, truncating torn write"
                );
                break;
            }

            let magic = [remaining[0], remaining[1]];
            if magic != MICRO_WAL_MAGIC {
                // Check if remaining slice is completely zero-padded
                if remaining.iter().all(|&b| b == 0) {
                    truncated_bytes = remaining.len();
                    tracing::warn!(
                        path = ?path,
                        offset = offset,
                        truncated_bytes = truncated_bytes,
                        "Zero-filled tail at EOF in Micro-WAL, truncating torn write"
                    );
                    break;
                }
                return Err(ServerError::WalCorruption(format!(
                    "Invalid Micro-WAL magic at offset {}: expected {:?}, got {:?}",
                    offset, MICRO_WAL_MAGIC, magic
                )));
            }

            let seq_num = u64::from_le_bytes(remaining[2..10].try_into().unwrap());
            let mut mutation_bytes = [0u8; 16];
            mutation_bytes.copy_from_slice(&remaining[10..26]);
            let mutation_id = MutationId::new(mutation_bytes);
            let client_len = u16::from_le_bytes(remaining[26..28].try_into().unwrap()) as usize;

            let record_len = RECORD_HEADER_SIZE + client_len + RECORD_CRC_SIZE;
            if remaining.len() < record_len {
                // Incomplete payload at EOF -> Torn write
                truncated_bytes = remaining.len();
                tracing::warn!(
                    path = ?path,
                    offset = offset,
                    truncated_bytes = truncated_bytes,
                    expected_len = record_len,
                    available_len = remaining.len(),
                    "Incomplete Micro-WAL record payload at EOF, truncating torn write"
                );
                break;
            }

            let client_bytes = &remaining[28..28 + client_len];
            let client_id_str = match std::str::from_utf8(client_bytes) {
                Ok(s) => s,
                Err(e) => {
                    return Err(ServerError::WalCorruption(format!(
                        "Invalid UTF-8 ClientId in Micro-WAL at offset {}: {}",
                        offset, e
                    )));
                }
            };
            let client_id = ClientId::new(client_id_str);

            let expected_crc = u32::from_le_bytes(
                remaining[28 + client_len..record_len]
                    .try_into()
                    .unwrap(),
            );
            let computed_crc = crc32fast::hash(&remaining[2..28 + client_len]);

            if computed_crc != expected_crc {
                return Err(ServerError::WalCorruption(format!(
                    "Micro-WAL CRC32 mismatch at offset {}: expected {:#010X}, got {:#010X}",
                    offset, expected_crc, computed_crc
                )));
            }

            let seq = SequenceNumber::new(seq_num);
            if seq.get() <= head_seq.get() && head_seq.get() != 0 {
                return Err(ServerError::WalCorruption(format!(
                    "Micro-WAL sequence non-monotonic at offset {}: {} <= head {}",
                    offset,
                    seq.get(),
                    head_seq.get()
                )));
            }

            head_seq = seq;
            entries.push(MicroWalEntry {
                seq,
                mutation_id,
                client_id,
            });

            offset += record_len;
        }

        // If torn write detected at EOF, truncate file in-place
        if truncated_bytes > 0 {
            file.set_len(offset as u64)?;
            file.sync_all()?;
        }

        Ok(MicroWalRecovery {
            head_seq,
            entries,
            truncated_bytes,
        })
    }
}
