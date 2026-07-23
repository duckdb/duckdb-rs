use std::{cell::RefCell, collections::HashSet};

use crate::{Result, error::duckdb_failure_from_message, ffi::duckdb_vector};

#[derive(Default)]
pub(in crate::core) struct BorrowRegistry {
    active: RefCell<HashSet<usize>>,
}

impl BorrowRegistry {
    pub(in crate::core) fn acquire(&self, ptr: duckdb_vector) -> Result<BorrowGuard<'_>> {
        let key = ptr as usize;
        let mut active = self.active.borrow_mut();
        if active.insert(key) {
            Ok(BorrowGuard { registry: self, key })
        } else {
            Err(duckdb_failure_from_message(
                "DuckDB vector already has a live view; drop it before requesting another",
            ))
        }
    }
}

pub(in crate::core) struct BorrowGuard<'a> {
    registry: &'a BorrowRegistry,
    key: usize,
}

impl Drop for BorrowGuard<'_> {
    fn drop(&mut self) {
        self.registry.active.borrow_mut().remove(&self.key);
    }
}
