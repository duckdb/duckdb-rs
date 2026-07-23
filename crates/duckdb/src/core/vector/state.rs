use std::cell::Cell;

use crate::{Result, error::duckdb_failure_from_message};

pub(in crate::core) const UNDER_CONSTRUCTION_MESSAGE: &str = "DuckDB payload is still under construction; finish writable views, then read through an initialized owner; chunk writers must call DataChunkHandle::assume_initialized";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::core) enum VectorState {
    /// DuckDB supplied initialized callback input that must remain read-only.
    ///
    /// This state is staged for the native callback adapters. The temporary
    /// callback transport does not select it yet.
    // TODO(native-vector-callback-adapters): Select this state when callback
    // input chunks are wrapped instead of treating `new_unowned` as initialized.
    #[cfg_attr(not(feature = "vtab"), allow(dead_code))]
    CallbackInput { readable_len: usize },
    /// Every non-null payload in the committed top-level and nested child spans
    /// is initialized.
    Initialized { readable_len: usize },
    /// The vector is being filled. Validity remains readable, but payloads do
    /// not cross the safe native read seam.
    UnderConstruction,
}

impl VectorState {
    fn readable_len(self) -> Option<usize> {
        match self {
            Self::CallbackInput { readable_len } | Self::Initialized { readable_len } => Some(readable_len),
            Self::UnderConstruction => None,
        }
    }
}

/// Shared initialization state for every view derived from one data chunk.
pub(in crate::core) struct VectorStateCell(Cell<VectorState>);

impl VectorStateCell {
    pub(in crate::core) fn new(state: VectorState) -> Self {
        Self(Cell::new(state))
    }

    pub(in crate::core) fn get(&self) -> VectorState {
        self.0.get()
    }

    pub(in crate::core) fn set(&self, state: VectorState) {
        self.0.set(state);
    }

    pub(in crate::core) fn readable_len_or_err(&self) -> Result<usize> {
        self.get()
            .readable_len()
            .ok_or_else(|| duckdb_failure_from_message(UNDER_CONSTRUCTION_MESSAGE))
    }

    #[cfg(test)]
    pub(super) fn is_initialized(&self) -> bool {
        self.readable_len_or_err().is_ok()
    }
}

pub(super) enum StateRef<'a> {
    Borrowed(&'a VectorStateCell),
    Owned(VectorStateCell),
}

impl StateRef<'_> {
    pub(super) fn shared(&self) -> &VectorStateCell {
        match self {
            Self::Borrowed(state) => state,
            Self::Owned(state) => state,
        }
    }

    pub(super) fn reborrow(&self) -> StateRef<'_> {
        StateRef::Borrowed(self.shared())
    }

    pub(super) fn readable_len_or_err(&self) -> Result<usize> {
        self.shared().readable_len_or_err()
    }
}

#[derive(Clone, Copy)]
pub(super) enum ReadableSpan {
    Chunk { multiplier: usize },
    Fixed(usize),
}

impl ReadableSpan {
    pub(super) fn scaled(self, multiplier: usize) -> Result<Self> {
        let scale = |len: usize| {
            len.checked_mul(multiplier)
                .ok_or_else(|| duckdb_failure_from_message("array child readable span overflows usize"))
        };
        Ok(match self {
            Self::Chunk { multiplier } => Self::Chunk {
                multiplier: scale(multiplier)?,
            },
            Self::Fixed(len) => Self::Fixed(scale(len)?),
        })
    }
}
