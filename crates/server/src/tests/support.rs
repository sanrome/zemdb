//! Helpers shared by the unit tests of every module of the crate.

/// Deterministic pseudo-random generator (SplitMix64) for randomized tests. A fixed seed always
/// yields the same sequence, so a failing case replays exactly from its seed.
pub(crate) struct TestRng(u64);

impl TestRng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A value in `0..n`, or 0 when `n` is 0.
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    /// True with probability `p` per thousand.
    pub(crate) fn permille(&mut self, p: u64) -> bool {
        self.below(1000) < p
    }

    /// A position in a buffer of `len` bytes, or 0 if it is empty.
    pub(crate) fn index(&mut self, len: usize) -> usize {
        self.below(len as u64) as usize
    }
}
