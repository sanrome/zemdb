//! A small deterministic pseudo-random generator for the randomized tests, so that a seed always
//! reproduces the same run. Shared with the unit tests of the crate, which include this file.

/// SplitMix64: a 64-bit state advanced by a constant and mixed on output. Fast, and good enough
/// to pick test inputs; never use it for anything else.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number in `0..n`, or 0 if `n` is 0.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    /// True with a probability of `percent` in 100.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}
