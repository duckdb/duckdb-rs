use std::{
    collections::HashSet,
    sync::{Mutex, MutexGuard, OnceLock, PoisonError},
};

use crate::{Result, error::duckdb_failure_from_message, ffi::duckdb_vector};

static ACTIVE_VECTOR_VIEWS: OnceLock<Mutex<HashSet<usize>>> = OnceLock::new();

fn active_vector_views() -> MutexGuard<'static, HashSet<usize>> {
    ACTIVE_VECTOR_VIEWS
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        // The registry only inserts and removes `usize` keys; neither
        // operation invokes user code. A panic can conservatively leave an
        // active key behind, but cannot invalidate the HashSet's structure, so
        // continuing with the poisoned guard preserves alias rejection.
        .unwrap_or_else(PoisonError::into_inner)
}

pub(in crate::core) struct BorrowGuard {
    key: usize,
}

impl BorrowGuard {
    pub(in crate::core) fn acquire(ptr: duckdb_vector) -> Result<Self> {
        let key = ptr as usize;
        let mut active = active_vector_views();
        if active.insert(key) {
            Ok(Self { key })
        } else {
            Err(duckdb_failure_from_message(
                "DuckDB vector already has an active compatibility view",
            ))
        }
    }
}

impl Drop for BorrowGuard {
    fn drop(&mut self) {
        active_vector_views().remove(&self.key);
    }
}
