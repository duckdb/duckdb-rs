use std::{
    collections::HashSet,
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
};

use crate::{Result, error::duckdb_failure_from_message, ffi::duckdb_vector};

/// `Arc<Mutex<_>>` preserves `RefUnwindSafe`; guards use `Weak` so leaked
/// guards cannot keep a stale pointer registry alive after its owner drops.
#[derive(Default)]
pub(in crate::core) struct BorrowRegistry {
    active: Mutex<HashSet<usize>>,
}

impl BorrowRegistry {
    fn active_vector_views(&self) -> MutexGuard<'_, HashSet<usize>> {
        self.active
            .lock()
            // The registry only inserts and removes `usize` keys; neither
            // operation invokes user code. Continuing with a poisoned guard is
            // therefore sufficient to preserve alias rejection.
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(in crate::core) fn acquire(self: &Arc<Self>, ptr: duckdb_vector) -> Result<BorrowGuard> {
        let key = ptr as usize;
        let mut active = self.active_vector_views();
        if active.insert(key) {
            Ok(BorrowGuard {
                registry: Arc::downgrade(self),
                key,
            })
        } else {
            Err(duckdb_failure_from_message(
                "DuckDB vector already has a live view; drop it before requesting another",
            ))
        }
    }
}

pub(in crate::core) struct BorrowGuard {
    registry: Weak<BorrowRegistry>,
    key: usize,
}

impl Drop for BorrowGuard {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            registry.active_vector_views().remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::BorrowRegistry;

    #[test]
    fn forgotten_lease_does_not_poison_a_reused_vector_address() {
        let reused_address: crate::ffi::duckdb_vector = std::ptr::dangling_mut();
        let first_owner = Arc::new(BorrowRegistry::default());
        let guard = first_owner.acquire(reused_address).unwrap();
        std::mem::forget(guard);
        drop(first_owner);

        let second_owner = Arc::new(BorrowRegistry::default());
        second_owner.acquire(reused_address).unwrap();
    }
}
