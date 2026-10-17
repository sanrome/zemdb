use zemdb_core::mutation::OperationKind;
use zemdb_core::protocol::codec::encoded_len;
use zemdb_core::protocol::messages::SequencedOperation;
use zemdb_core::value::Value;

/// What `op` is charged against a read budget and the RAM window: its encoded size, or what
/// its values take in memory if that is more (a `Null` takes 1 byte on the wire and 24 in
/// memory).
pub(crate) fn op_bytes(op: &SequencedOperation) -> u64 {
    let values = op.op.pk().len()
        + match op.op.kind() {
            OperationKind::Insert { row } => row.len(),
            OperationKind::Update { updates } => updates.len(),
            OperationKind::Delete => 0,
        };
    let in_memory = (values * std::mem::size_of::<Value>()) as u64;
    encoded_len(op).unwrap_or(u64::MAX).max(in_memory)
}

/// What one read of the log may still return: at most a number of operations and of bytes,
/// as [`op_bytes`] charges them. The first operation is always admitted, so a read makes
/// progress even when one
/// operation is larger than the byte budget (every accepted operation fits in a response on
/// its own).
///
/// Once an operation is refused the budget is exhausted for the rest of the read, so a read
/// that spans several tiers stops where the refused operation is instead of skipping it.
#[derive(Debug, Clone)]
pub struct ReadBudget {
    max_ops: usize,
    max_bytes: u64,
    ops: usize,
    bytes: u64,
    exhausted: bool,
}

impl ReadBudget {
    /// A budget of `max_ops` operations and `max_bytes` bytes.
    pub fn new(max_ops: usize, max_bytes: u64) -> Self {
        Self {
            max_ops,
            max_bytes,
            ops: 0,
            bytes: 0,
            exhausted: max_ops == 0,
        }
    }

    /// A budget of `max_ops` operations, of any size.
    pub fn ops(max_ops: usize) -> Self {
        Self::new(max_ops, u64::MAX)
    }

    /// Takes an operation of `bytes` from the budget if it fits, and returns whether it did.
    pub fn admit(&mut self, bytes: u64) -> bool {
        let fits = !self.exhausted
            && self.ops < self.max_ops
            && (self.ops == 0 || self.bytes.saturating_add(bytes) <= self.max_bytes);
        if fits {
            self.ops += 1;
            self.bytes = self.bytes.saturating_add(bytes);
            self.exhausted = self.ops == self.max_ops;
        } else {
            self.exhausted = true;
        }
        fits
    }

    /// Whether the read must stop: the operation count is reached or an operation was refused.
    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

#[cfg(test)]
#[path = "tests/budget.rs"]
mod tests;
