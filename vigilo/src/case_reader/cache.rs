//! Worker-local reuse of immutable, byte-verified files across shard contexts.
//!
//! One worker command owns the cache. Binding identity and the entire reference
//! isolate owners, revisions, formats and digests. Only small complete files are
//! retained; readers still decode, validate every touched file and prove chunk
//! membership on every use. No leases, routes or mutable permissions are cached.
//! The mutex covers bounded memory operations only, never remote I/O. Concurrent
//! misses may download independently; failed/cancelled reads are never inserted.
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        Mutex,
    },
};

use anyhow::{
    Result,
    ensure,
};
use tokio_util::sync::CancellationToken;

use super::*;

const CAPACITY: usize = 8 * 1024 * 1024;
const MAX_FILE: usize = 1024 * 1024;
const MAX_ENTRIES: usize = 256;

/// One command shares at most 8 MiB of charged bytes and 256 file identities.
/// Readers own their returned copies; eviction releases the retained allocation.
#[derive(Default)]
pub(crate) struct ReadCache(Mutex<State>);
#[derive(Default)]
struct State {
    entries: VecDeque<Entry>,
    charged: usize,
}
struct Entry {
    binding: String,
    reference: ObjectRef,
    bytes: Box<[u8]>,
    charge: usize,
}

/// Each reader binds the shared worker cache to its verified physical owner.
#[derive(Clone)]
pub(super) struct Scope {
    pub cache: Arc<ReadCache>,
    pub binding: String,
}
impl Scope {
    pub async fn read(
        &self,
        store: &dyn BlobStorage,
        reference: &ObjectRef,
        limits: &FormatLimits,
        budget: Budget,
        cancel: &CancellationToken,
    ) -> Result<Vec<u8>> {
        budget.validate()?;
        reference.validate(limits)?;
        ensure!(!cancel.is_cancelled(), "case read cancelled");
        let hit = {
            let mut state = self.cache.0.lock().expect("read cache poisoned");
            state
                .entries
                .iter()
                .position(|e| e.binding == self.binding && e.reference == *reference)
                .map(|index| {
                    let entry = state.entries.remove(index).expect("present cache entry");
                    let bytes = entry.bytes.to_vec();
                    state.entries.push_back(entry);
                    bytes
                })
        };
        let bytes = match hit {
            Some(bytes) => bytes,
            None => crate::dataset_ingestion::read_bytes(store, reference, budget, cancel).await?,
        };
        // Enforce the caller's current limits and complete byte proof on hits as
        // well as misses. Semantic validation stays at each consuming boundary.
        reference.verify(&bytes, limits)?;
        ensure!(!cancel.is_cancelled(), "case read cancelled");
        ensure!(
            tokio::time::Instant::now() < budget.deadline,
            "case read deadline exceeded"
        );
        if bytes.len() <= MAX_FILE {
            let charge = bytes.len()
                + serde_json::to_vec(reference)?.len()
                + self.binding.len()
                + std::mem::size_of::<Entry>();
            let mut state = self.cache.0.lock().expect("read cache poisoned");
            if charge <= CAPACITY
                && !state
                    .entries
                    .iter()
                    .any(|e| e.binding == self.binding && e.reference == *reference)
            {
                while state.charged + charge > CAPACITY || state.entries.len() >= MAX_ENTRIES {
                    let old = state.entries.pop_front().expect("nonempty bounded cache");
                    state.charged -= old.charge;
                }
                state.charged += charge;
                state.entries.push_back(Entry {
                    binding: self.binding.clone(),
                    reference: reference.clone(),
                    bytes: bytes.clone().into_boxed_slice(),
                    charge,
                });
            }
        }
        Ok(bytes)
    }
}
