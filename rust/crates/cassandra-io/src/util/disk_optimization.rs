// Licensed under Apache License, Version 2.0.

//! Disk-type-aware read buffer sizing.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.DiskOptimizationStrategy`

/// Minimum buffer size returned by any strategy (4 KiB).
const MIN_BUFFER_SIZE: usize = 4 * 1024;

/// Page alignment boundary (4 KiB).
const PAGE_SIZE: usize = 4 * 1024;

/// Maximum read buffer for spinning disks (128 KiB).
const SPINNING_MAX: usize = 128 * 1024;

/// Maximum read buffer for SSDs (8 KiB).
const SSD_MAX: usize = 8 * 1024;

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

/// Selects read-buffer sizes based on the underlying storage medium.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiskOptimizationStrategy {
    /// Optimise for rotational media — larger sequential reads amortise seek
    /// latency.
    Spinning,
    /// Optimise for solid-state drives — smaller reads avoid read
    /// amplification.
    Ssd,
    /// Platform default (currently equivalent to [`Ssd`](Self::Ssd)).
    Default,
}

impl DiskOptimizationStrategy {
    /// Returns the ideal read-buffer size for a read of `expected_read` bytes.
    ///
    /// The returned value is:
    /// 1. Capped to a strategy-specific maximum.
    /// 2. Rounded **up** to the nearest 4 KiB page boundary.
    /// 3. Never smaller than 4 KiB.
    pub fn buffer_size_for_read(&self, expected_read: u64) -> usize {
        let max = match self {
            Self::Spinning => SPINNING_MAX,
            Self::Ssd | Self::Default => SSD_MAX,
        };

        let capped = (expected_read as usize).min(max);
        let rounded = round_up_to_page(capped);
        rounded.max(MIN_BUFFER_SIZE)
    }
}

/// Rounds `n` up to the next multiple of [`PAGE_SIZE`].
#[inline]
fn round_up_to_page(n: usize) -> usize {
    (n + PAGE_SIZE - 1) & !(PAGE_SIZE - 1)
}

impl std::fmt::Display for DiskOptimizationStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spinning => write!(f, "spinning"),
            Self::Ssd => write!(f, "ssd"),
            Self::Default => write!(f, "default"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Spinning ---------------------------------------------------------

    #[test]
    fn test_spinning_small_read() {
        // A 100-byte read should still yield the 4 KiB minimum.
        assert_eq!(
            DiskOptimizationStrategy::Spinning.buffer_size_for_read(100),
            4096,
        );
    }

    #[test]
    fn test_spinning_medium_read() {
        // 50 KiB → rounded up to 52 KiB (next 4 KiB boundary).
        let size = DiskOptimizationStrategy::Spinning.buffer_size_for_read(50 * 1024);
        assert_eq!(size, 52 * 1024);
    }

    #[test]
    fn test_spinning_capped_at_128k() {
        let size = DiskOptimizationStrategy::Spinning.buffer_size_for_read(1_000_000);
        assert_eq!(size, 128 * 1024);
    }

    #[test]
    fn test_spinning_exact_page_boundary() {
        let size = DiskOptimizationStrategy::Spinning.buffer_size_for_read(8192);
        assert_eq!(size, 8192);
    }

    // -- SSD --------------------------------------------------------------

    #[test]
    fn test_ssd_small_read() {
        assert_eq!(
            DiskOptimizationStrategy::Ssd.buffer_size_for_read(100),
            4096,
        );
    }

    #[test]
    fn test_ssd_capped_at_8k() {
        let size = DiskOptimizationStrategy::Ssd.buffer_size_for_read(1_000_000);
        assert_eq!(size, 8 * 1024);
    }

    #[test]
    fn test_ssd_exact_page() {
        let size = DiskOptimizationStrategy::Ssd.buffer_size_for_read(4096);
        assert_eq!(size, 4096);
    }

    // -- Default ----------------------------------------------------------

    #[test]
    fn test_default_matches_ssd() {
        for read in [0, 100, 4096, 8192, 1_000_000u64] {
            assert_eq!(
                DiskOptimizationStrategy::Default.buffer_size_for_read(read),
                DiskOptimizationStrategy::Ssd.buffer_size_for_read(read),
            );
        }
    }

    // -- Round-up helper --------------------------------------------------

    #[test]
    fn test_round_up_to_page() {
        assert_eq!(round_up_to_page(0), 0);
        assert_eq!(round_up_to_page(1), 4096);
        assert_eq!(round_up_to_page(4096), 4096);
        assert_eq!(round_up_to_page(4097), 8192);
    }

    // -- Zero read --------------------------------------------------------

    #[test]
    fn test_zero_read_returns_min() {
        assert_eq!(
            DiskOptimizationStrategy::Spinning.buffer_size_for_read(0),
            4096,
        );
    }
}
