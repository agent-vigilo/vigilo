//! Admission limits for individual encoded objects and their resident budgets.
//!
//! These are format limits, independent of execution chunk size and remote I/O
//! policy. Validate configuration before accepting objects. Finite ceilings
//! prevent overrides from turning bounded codecs into dataset-wide buffers.

use anyhow::{
    Result,
    ensure,
};
use serde::{
    Deserialize,
    Serialize,
};

pub(crate) const MIB: usize = 1_048_576;

/// Admitted storage batching policy, recorded in each dataset manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BatchLimits {
    pub(crate) max_cases: usize,
    pub(crate) max_bytes: usize,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_cases: 100,
            max_bytes: 8 * MIB,
        }
    }
}

impl BatchLimits {
    /// Rejects zero and excessive limits before any allocation or I/O.
    pub(crate) fn validate(self) -> Result<()> {
        ensure!(
            (1..=65_536).contains(&self.max_cases),
            "batch max_cases must be in 1..=65536"
        );
        ensure!(
            (256..=64 * MIB).contains(&self.max_bytes),
            "batch max_bytes must be in 256..=67108864"
        );
        Ok(())
    }
}

/// Reader/writer bounds. Published batching policy remains independent of defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FormatLimits {
    pub(crate) batch: BatchLimits,
    pub(crate) page_bytes: usize,
    pub(crate) page_entries: usize,
    pub(crate) root_bytes: usize,
    pub(crate) source_bytes: usize,
    pub(crate) index_height: u8,
    pub(crate) resident_batches: usize,
    pub(crate) resident_batch_bytes: usize,
    pub(crate) staging_spool_bytes: usize,
}

impl Default for FormatLimits {
    fn default() -> Self {
        Self {
            batch: BatchLimits::default(),
            page_bytes: MIB,
            page_entries: 256,
            root_bytes: MIB / 4,
            source_bytes: 8 * MIB,
            index_height: 8,
            resident_batches: 4,
            resident_batch_bytes: 32 * MIB,
            staging_spool_bytes: 256 * MIB,
        }
    }
}

impl FormatLimits {
    /// Checks finite maxima and aggregate encoded-byte reservations with checked arithmetic.
    /// Remote stream, retry, and disk-validation quotas belong to their consumers.
    pub(crate) fn validate(&self) -> Result<()> {
        self.batch.validate()?;
        ensure!(
            (256..=8 * MIB).contains(&self.page_bytes),
            "page_bytes must be in 256..=8388608"
        );
        ensure!(
            (1..=4096).contains(&self.page_entries),
            "page_entries must be in 1..=4096"
        );
        ensure!(
            (256..=MIB).contains(&self.root_bytes),
            "root_bytes must be in 256..=1048576"
        );
        ensure!(
            (256..=64 * MIB).contains(&self.source_bytes),
            "source_bytes must be in 256..=67108864"
        );
        ensure!(
            (1..=8).contains(&self.index_height),
            "index_height must be in 1..=8"
        );
        ensure!(
            (1..=16).contains(&self.resident_batches),
            "resident_batches must be in 1..=16"
        );
        ensure!(
            self.resident_batch_bytes <= 1024 * MIB,
            "resident batch budget exceeds 1 GiB"
        );
        ensure!(
            self.staging_spool_bytes <= 4096_usize.saturating_mul(MIB),
            "staging spool exceeds 4 GiB"
        );
        let resident = self
            .batch
            .max_bytes
            .checked_mul(self.resident_batches)
            .ok_or_else(|| anyhow::anyhow!("resident batch reservation overflow"))?;
        ensure!(
            resident <= self.resident_batch_bytes,
            "resident batches exceed encoded memory budget"
        );
        ensure!(
            self.batch.max_bytes <= self.staging_spool_bytes,
            "batch exceeds staging spool budget"
        );
        Ok(())
    }
}
