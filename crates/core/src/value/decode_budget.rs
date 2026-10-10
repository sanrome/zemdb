//! Per-thread budget of the [`Value`](super::Value)s a decode may materialize.
//!
//! A `Value` takes 24 bytes in memory but as little as one byte on the wire (`Null`), so the
//! byte limit of a frame does not bound the memory its decode reserves. A decoder that must
//! bound it opens a [`ValueBudget`] for the duration of a synchronous decode. Meanwhile, the
//! sequences of column values deserialized on the thread (rows, primary keys and update
//! deltas, see [`deserialize_columns`](super::row::deserialize_columns)) charge their values
//! against it before decoding them. Outside a budget (snapshots, log records, responses
//! decoded by clients) nothing is counted, and charging costs one thread-local read per
//! sequence.

use std::cell::Cell;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Budget {
    /// No budgeted decode is running on this thread.
    Unlimited,
    /// Values the running decode may still materialize.
    Remaining(u64),
    /// The running decode tried to materialize more values than its budget.
    Exhausted,
}

thread_local! {
    static BUDGET: Cell<Budget> = const { Cell::new(Budget::Unlimited) };
}

/// The budget was exhausted: the value must not be materialized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetExhausted;

/// Charges `count` values against the budget of the decode running on this thread, if any.
pub(crate) fn charge_values(count: u64) -> Result<(), BudgetExhausted> {
    BUDGET.with(|budget| match budget.get() {
        Budget::Unlimited => Ok(()),
        Budget::Remaining(left) if count <= left => {
            budget.set(Budget::Remaining(left - count));
            Ok(())
        }
        Budget::Remaining(_) | Budget::Exhausted => {
            budget.set(Budget::Exhausted);
            Err(BudgetExhausted)
        }
    })
}

/// Guard of a budgeted decode: while it lives, the values deserialized on this thread are
/// limited to `max_values`. Dropping it (also on an early return or a panic) restores the
/// previous state, so the budget never leaks into unrelated decodes on the same thread.
#[must_use]
pub(crate) struct ValueBudget {
    previous: Budget,
}

impl ValueBudget {
    pub(crate) fn start(max_values: u64) -> Self {
        let previous = BUDGET.with(|budget| budget.replace(Budget::Remaining(max_values)));
        Self { previous }
    }

    /// Whether the decode tried to materialize more values than the budget allows.
    pub(crate) fn exhausted(&self) -> bool {
        BUDGET.with(|budget| budget.get() == Budget::Exhausted)
    }
}

impl Drop for ValueBudget {
    fn drop(&mut self) {
        BUDGET.with(|budget| budget.set(self.previous));
    }
}

#[cfg(test)]
#[path = "tests/decode_budget.rs"]
mod tests;
