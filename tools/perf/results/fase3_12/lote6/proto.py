#!/usr/bin/env python3
"""A diagnostic prototype, outside the repo: on a copy of the result tree, (1) fit_in_response does
not measure the operations again, and (2) a read grows its response's reservation by chunks of
256 KiB instead of once per operation, giving back what it did not use when it ends."""
import sys
T=sys.argv[1]
PARTS=sys.argv[2] if len(sys.argv)>2 else 'RF'
def edit(p, pairs):
    s=open(p).read()
    for a,b in pairs:
        assert s.count(a)==1,(p,a,s.count(a))
        s=s.replace(a,b)
    open(p,'w').write(s)
if 'F' in PARTS: edit(T+'/crates/server/src/actor/room.rs',[(
"""    let mut total: u64 = 0;
    let fitting = ops""",
"""    if !ops.is_empty() {
        return (ops, has_more);
    }
    let mut total: u64 = 0;
    let fitting = ops""")])
if 'R' in PARTS: edit(T+'/crates/server/src/response_budget.rs',[(
"""    /// Returns what the reservation holds beyond `bytes`.""",
"""    pub(crate) fn held(&self) -> u64 {
        self.permit.num_permits() as u64
    }

    /// Returns what the reservation holds beyond `bytes`.""")])
if 'R' in PARTS: edit(T+'/crates/server/src/log/budget.rs',[
("""    /// Whether the reservation had no room for an operation, which ended the read.
    out_of_room: bool,
}""","""    /// Whether the reservation had no room for an operation, which ended the read.
    out_of_room: bool,
    ahead: u64,
}

impl Drop for ReadBudget<'_> {
    fn drop(&mut self) {
        if let Some(reservation) = self.reservation.as_deref_mut() {
            let held = reservation.held();
            reservation.shrink_to(held.saturating_sub(self.ahead));
        }
    }
}"""),
("""            reservation: None,
            out_of_room: false,
        }""","""            reservation: None,
            out_of_room: false,
            ahead: 0,
        }"""),
("""    pub(crate) fn reserving(self, reservation: &'r mut Reservation) -> Self {
        Self {
            reservation: Some(reservation),
            ..self
        }
    }""","""    pub(crate) fn reserving(mut self, reservation: &'r mut Reservation) -> Self {
        self.reservation = Some(reservation);
        self
    }"""),
("""    pub(crate) fn window(max_ops: usize, max_bytes: u64) -> Self {
        Self {
            window: Some((max_ops, max_bytes)),
            ..Self::ops(usize::MAX)
        }
    }""","""    pub(crate) fn window(max_ops: usize, max_bytes: u64) -> Self {
        let mut budget = Self::ops(usize::MAX);
        budget.window = Some((max_ops, max_bytes));
        budget
    }"""),
("""            if let Some(reservation) = self.reservation.as_deref_mut() {
                fits = reservation.grow(size.in_response());
                self.out_of_room = !fits;
            }""","""            if let Some(reservation) = self.reservation.as_deref_mut() {
                let need = size.in_response();
                if self.ahead >= need {
                    self.ahead -= need;
                } else {
                    let missing = need - self.ahead;
                    let chunk = missing.max(256 * 1024);
                    if reservation.grow(chunk) {
                        self.ahead = self.ahead + chunk - need;
                    } else if reservation.grow(missing) {
                        self.ahead = 0;
                    } else {
                        fits = false;
                    }
                }
                self.out_of_room = !fits;
            }"""),
])
print("patched")
