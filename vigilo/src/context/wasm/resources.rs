//! Per-invocation ownership and budgets for evaluator response resources.
//!
//! Tables share immutable captured allocations, but never handles or read budgets.
//! Process-local nonreused handle ranges prevent stale or foreign IDs from aliasing
//! a resource in another invocation. No global resource registry or I/O is used.
//! Dropping a store drops its table and references, including after a Wasm trap.

use std::{
    collections::BTreeMap,
    sync::atomic::{
        AtomicU64,
        Ordering,
    },
    time::{
        Duration,
        Instant,
    },
};

use vigilo::response::{
    CapturedBody,
    ResourceHandle,
    ResourceReadError,
};

pub(crate) const MAX_RESOURCES: u64 = 64;
pub(crate) const MAX_READ_BYTES: u32 = 64 * 1024;
pub(crate) const MAX_READ_CALLS: u32 = 4096;
static NEXT_RANGE: AtomicU64 = AtomicU64::new(1);

pub(crate) struct Resources {
    resources: BTreeMap<u64, CapturedBody>,
    next: u64,
    end: u64,
    remaining_bytes: u64,
    remaining_read_bytes: u64,
    remaining_calls: u32,
    deadline: Option<Instant>,
}

impl Resources {
    /// Reserve unique IDs once per invocation. Retained bytes share the configured
    /// Wasm-memory ceiling; read traffic is independently capped at four times it.
    pub(crate) fn new(
        max_bytes: u64,
        timeout: Option<Duration>,
    ) -> Result<Self, ResourceReadError> {
        if max_bytes == 0 {
            return Err(ResourceReadError::LimitExceeded);
        }
        let base = NEXT_RANGE
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(MAX_RESOURCES)
            })
            .map_err(|_| ResourceReadError::LimitExceeded)?;
        Ok(Self {
            resources: BTreeMap::new(),
            next: base,
            end: base + MAX_RESOURCES,
            remaining_bytes: max_bytes,
            remaining_read_bytes: max_bytes.saturating_mul(4),
            remaining_calls: MAX_READ_CALLS,
            deadline: timeout
                .map(|duration| {
                    Instant::now()
                        .checked_add(duration)
                        .ok_or(ResourceReadError::LimitExceeded)
                })
                .transpose()?,
        })
    }

    /// Insert only after all bounds pass. Failed insertions consume no identity,
    /// memory quota, or resource slot. Captured bytes remain shared and immutable.
    pub(crate) fn insert(
        &mut self,
        body: CapturedBody,
    ) -> Result<ResourceHandle, ResourceReadError> {
        self.check_deadline()?;
        let bytes = body.bytes().len() as u64;
        if self.next == self.end || bytes > self.remaining_bytes {
            return Err(ResourceReadError::LimitExceeded);
        }
        let handle = self.next;
        self.next += 1;
        self.remaining_bytes -= bytes;
        self.resources.insert(handle, body);
        Ok(ResourceHandle(handle))
    }

    fn check_deadline(&self) -> Result<(), ResourceReadError> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            Err(ResourceReadError::LimitExceeded)
        } else {
            Ok(())
        }
    }

    /// Validate authorization and budgets before allocating a result. Every call,
    /// including invalid and EOF reads, consumes a call slot. Only returned bytes
    /// consume byte quota; repeated reads count again. Errors disclose no bytes.
    pub(crate) fn read(
        &mut self,
        handle: ResourceHandle,
        offset: u64,
        max_bytes: u32,
    ) -> Result<Vec<u8>, ResourceReadError> {
        self.check_deadline()?;
        self.remaining_calls = self
            .remaining_calls
            .checked_sub(1)
            .ok_or(ResourceReadError::LimitExceeded)?;
        let body = self
            .resources
            .get(&handle.0)
            .ok_or(ResourceReadError::InvalidHandle)?;
        if max_bytes == 0 || max_bytes > MAX_READ_BYTES {
            return Err(ResourceReadError::LimitExceeded);
        }
        if offset > body.bytes().len() as u64 {
            return Err(ResourceReadError::OutOfBounds);
        }
        let offset = usize::try_from(offset).map_err(|_| ResourceReadError::OutOfBounds)?;
        let length = (body.bytes().len() - offset).min(max_bytes as usize);
        if length as u64 > self.remaining_read_bytes {
            return Err(ResourceReadError::LimitExceeded);
        }
        self.remaining_read_bytes -= length as u64;
        Ok(body.bytes()[offset..offset + length].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn body(bytes: &[u8]) -> CapturedBody {
        CapturedBody::new(bytes.to_vec(), None)
    }

    #[test]
    fn reads_preserve_bytes_boundaries_and_charge_repeated_access() {
        let mut table = Resources::new(4, None).unwrap();
        let handle = table.insert(body(&[0, 255, 3, 4])).unwrap();
        assert_eq!(table.read(handle, 1, 10).unwrap(), [255, 3, 4]);
        assert!(table.read(handle, 4, 1).unwrap().is_empty());
        assert_eq!(
            table.read(handle, u64::MAX, 1),
            Err(ResourceReadError::OutOfBounds)
        );
        assert_eq!(
            table.read(handle, 0, 0),
            Err(ResourceReadError::LimitExceeded)
        );
        assert_eq!(
            table.read(handle, 0, MAX_READ_BYTES + 1),
            Err(ResourceReadError::LimitExceeded)
        );
        for _ in 0..3 {
            assert_eq!(table.read(handle, 0, 4).unwrap().len(), 4);
        }
        assert_eq!(
            table.read(handle, 0, 2),
            Err(ResourceReadError::LimitExceeded)
        );
        assert_eq!(table.read(handle, 0, 1).unwrap(), [0]);
        assert_eq!(
            table.read(handle, 0, 1),
            Err(ResourceReadError::LimitExceeded)
        );
    }

    #[test]
    fn stores_cannot_reuse_foreign_or_expired_handles() {
        let mut first = Resources::new(4, None).unwrap();
        let old = first.insert(body(b"a")).unwrap();
        let mut second = Resources::new(4, None).unwrap();
        let own = second.insert(body(b"b")).unwrap();
        assert_ne!(old, own);
        assert_eq!(first.read(own, 0, 1), Err(ResourceReadError::InvalidHandle));
        drop(first);
        assert_eq!(
            second.read(old, 0, 1),
            Err(ResourceReadError::InvalidHandle)
        );
        assert_eq!(second.read(own, 0, 1).unwrap(), b"b");
    }

    #[test]
    fn insertion_is_atomic_and_empty_resources_still_count() {
        let mut table = Resources::new(2, None).unwrap();
        assert_eq!(
            table.insert(body(b"bad")),
            Err(ResourceReadError::LimitExceeded)
        );
        assert!(table.insert(body(b"ok")).is_ok());
        assert_eq!(
            table.insert(body(b"x")),
            Err(ResourceReadError::LimitExceeded)
        );
        for _ in 1..MAX_RESOURCES {
            assert!(table.insert(body(b"")).is_ok());
        }
        assert_eq!(
            table.insert(body(b"")),
            Err(ResourceReadError::LimitExceeded)
        );
    }

    #[test]
    fn eof_and_invalid_calls_cannot_bypass_budgets_and_deadlines() {
        let mut table = Resources::new(1, None).unwrap();
        let handle = table.insert(body(b"")).unwrap();
        for _ in 0..MAX_READ_CALLS {
            assert!(table.read(handle, 0, 1).unwrap().is_empty());
        }
        assert_eq!(
            table.read(handle, 0, 1),
            Err(ResourceReadError::LimitExceeded)
        );
        let mut expired = Resources::new(1, Some(Duration::ZERO)).unwrap();
        assert_eq!(
            expired.insert(body(b"")),
            Err(ResourceReadError::LimitExceeded)
        );
        assert_eq!(
            expired.read(handle, 0, 1),
            Err(ResourceReadError::LimitExceeded)
        );
    }

    #[test]
    fn resource_tables_share_allocations_and_release_them_on_drop() {
        let original = body(b"secret");
        let pointer = original.bytes().as_ptr();
        let clone = original.clone();
        assert_eq!(pointer, clone.bytes().as_ptr());
        let mut table = Resources::new(6, None).unwrap();
        let handle = table.insert(clone).unwrap();
        assert_eq!(table.resources[&handle.0].bytes().as_ptr(), pointer);
        drop(table);
        assert_eq!(original.bytes(), b"secret");
        assert!(!format!("{original:?}").contains("secret"));
    }
}
