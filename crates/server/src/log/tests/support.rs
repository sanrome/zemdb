//! Builders shared by the unit tests of the log modules.

use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};
use zemdb_core::id::SequenceNumber;
use zemdb_core::mutation::Operation;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::protocol::wal_frame::encode_wal_record;
use zemdb_core::value::{PrimaryKey, Value};

pub(super) use crate::test_support::TestRng as Rng;

pub(super) fn seq(n: u64) -> SequenceNumber {
    SequenceNumber::new(n)
}

/// The deletion of row `seq` at sequence `seq`.
pub(super) fn make_op(seq: u64) -> SequencedOperation {
    let pk = PrimaryKey::single(Value::Int(seq as i64));
    SequencedOperation::new(
        SequenceNumber::new(seq),
        Operation::delete(1, pk, seq * 1000),
    )
}

pub(super) fn seqs(ops: &[SequencedOperation]) -> Vec<u64> {
    ops.iter().map(|op| op.seq.get()).collect()
}

/// One WAL frame per operation in `seqs`, without mutation ids, and the offset of each frame.
pub(super) fn frames(seqs: RangeInclusive<u64>) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = Vec::new();
    let mut offsets = Vec::new();
    for seq in seqs {
        offsets.push(bytes.len());
        bytes.extend_from_slice(&encode_wal_record(&make_op(seq), None).unwrap());
    }
    (bytes, offsets)
}

/// The file of segment `first..=last` in `dir`, spelled out rather than built by the code under
/// test; `extension` is `wal` or `wal.zst`.
pub(super) fn segment_file(dir: &Path, first: u64, last: u64, extension: &str) -> PathBuf {
    dir.join(format!("segment_{first:016}_{last:016}.{extension}"))
}
