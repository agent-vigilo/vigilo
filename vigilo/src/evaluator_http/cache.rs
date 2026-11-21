//! Bounded policy cache. One refresh gate coalesces overlapping batch loads;
//! errors never renew entries. Snapshots are immutable and survive cache expiry.
use std::{
    collections::{
        BTreeMap,
        BTreeSet,
    },
    future::Future,
    sync::Arc,
    time::Duration,
};

use anyhow::{
    Result,
    ensure,
};
use tokio::{
    sync::Mutex,
    time::Instant,
};
use uuid::Uuid;

use super::{
    Rule,
    Snapshot,
};

/// Worker-owned cache; loader is supplied by the host, keeping SQL outside policy.
/// Entries are partitioned by a trusted owner key; a cache hit grants no authority
/// beyond the policy snapshot itself. Capacity is shared across all partitions.
pub struct Cache {
    age: Duration,
    capacity: usize,
    entries: Mutex<BTreeMap<(Uuid, Uuid), Entry>>,
}
struct Entry {
    read_at: Instant,
    snapshot: Arc<Snapshot>,
}
impl Default for Cache {
    fn default() -> Self {
        Self::new(Duration::from_secs(30), 256).expect("valid default policy cache")
    }
}

impl Cache {
    /// Positive maximum age; no cache hit or failed refresh extends freshness.
    pub fn new(age: Duration, capacity: usize) -> Result<Self> {
        ensure!(
            !age.is_zero() && age <= Duration::from_secs(3600),
            "policy freshness must be 1ns..1h"
        );
        ensure!(
            (1..=4096).contains(&capacity),
            "invalid policy cache capacity"
        );
        Ok(Self {
            age,
            capacity,
            entries: Mutex::new(BTreeMap::new()),
        })
    }
    /// Fetch at most one bounded batch. Missing rows become cached deny-all snapshots.
    /// The partition comes from verified host context, never evaluator input.
    /// The load and wait share a bounded deadline. Failed loads leave expired entries
    /// expired. Callers freeze the returned snapshot only for their allocated attempt.
    pub async fn snapshots<F, Fut>(
        &self,
        partition: Uuid,
        ids: &[Uuid],
        load: F,
    ) -> Result<BTreeMap<Uuid, Arc<Snapshot>>>
    where
        F: FnOnce(Vec<Uuid>) -> Fut,
        Fut: Future<Output = Result<BTreeMap<Uuid, Vec<Rule>>>>,
    {
        ensure!(
            ids.len() <= self.capacity,
            "policy batch exceeds cache capacity"
        );
        let ids: BTreeSet<_> = ids.iter().copied().collect();
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            let mut entries = self.entries.lock().await;
            let missing: Vec<_> = ids
                .iter()
                .filter(|id| {
                    entries
                        .get(&(partition, **id))
                        .is_none_or(|entry| entry.read_at.elapsed() >= self.age)
                })
                .copied()
                .collect();
            if !missing.is_empty() {
                let read_at = Instant::now();
                let mut loaded = load(missing.clone()).await?;
                ensure!(
                    loaded.keys().all(|id| missing.contains(id)),
                    "unexpected evaluator policy identity"
                );
                let fresh = missing
                    .iter()
                    .map(|id| {
                        Ok((
                            *id,
                            Arc::new(Snapshot::new(*id, loaded.remove(id).unwrap_or_default())?),
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                ensure!(
                    read_at.elapsed() < self.age,
                    "policy expired during refresh"
                );
                // Evict entries outside this requested batch before insertion. Held
                // attempt snapshots remain valid through Arc ownership.
                entries.retain(|(owner, id), entry| {
                    (*owner == partition && ids.contains(id)) || entry.read_at.elapsed() < self.age
                });
                while entries.len()
                    + fresh
                        .iter()
                        .filter(|(id, _)| !entries.contains_key(&(partition, *id)))
                        .count()
                    > self.capacity
                {
                    let key = entries
                        .keys()
                        .find(|(owner, id)| *owner != partition || !ids.contains(id))
                        .copied()
                        .unwrap();
                    entries.remove(&key);
                }
                for (id, snapshot) in fresh {
                    entries.insert((partition, id), Entry { read_at, snapshot });
                }
            }
            ensure!(
                ids.iter()
                    .all(|id| entries[&(partition, *id)].read_at.elapsed() < self.age),
                "policy expired while preparing batch"
            );
            Ok(ids
                .into_iter()
                .map(|id| (id, entries[&(partition, id)].snapshot.clone()))
                .collect())
        })
        .await
        .map_err(|_| anyhow::anyhow!("policy refresh deadline exceeded"))?
    }
}
