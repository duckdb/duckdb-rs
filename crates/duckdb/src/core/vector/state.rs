use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use crate::{Result, error::duckdb_failure_from_message};

pub(in crate::core) const UNDER_CONSTRUCTION_MESSAGE: &str = "DuckDB payload is still under construction; finish writable views, then read through an initialized owner; chunk writers must call DataChunkHandle::assume_initialized";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::core) enum VectorState {
    /// DuckDB supplied initialized callback input that must remain read-only.
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
///
/// Atomics preserve `RefUnwindSafe` on the public vector wrappers. The wrappers
/// are not `Send` or `Sync`, so safe code cannot use this as a cross-thread
/// synchronization protocol and relaxed ordering is sufficient.
pub(in crate::core) struct VectorStateCell {
    mode: AtomicU8,
    readable_len: AtomicUsize,
}

impl VectorStateCell {
    const CALLBACK_INPUT: u8 = 0;
    const INITIALIZED: u8 = 1;
    const UNDER_CONSTRUCTION: u8 = 2;

    pub(in crate::core) fn new(state: VectorState) -> Self {
        let cell = Self {
            mode: AtomicU8::new(Self::UNDER_CONSTRUCTION),
            readable_len: AtomicUsize::new(0),
        };
        cell.set(state);
        cell
    }

    pub(in crate::core) fn get(&self) -> VectorState {
        match self.mode.load(Ordering::Relaxed) {
            Self::CALLBACK_INPUT => VectorState::CallbackInput {
                readable_len: self.readable_len.load(Ordering::Relaxed),
            },
            Self::INITIALIZED => VectorState::Initialized {
                readable_len: self.readable_len.load(Ordering::Relaxed),
            },
            Self::UNDER_CONSTRUCTION => VectorState::UnderConstruction,
            mode => unreachable!("invalid DuckDB vector state mode {mode}"),
        }
    }

    pub(in crate::core) fn set(&self, state: VectorState) {
        let (mode, readable_len) = match state {
            VectorState::CallbackInput { readable_len } => (Self::CALLBACK_INPUT, readable_len),
            VectorState::Initialized { readable_len } => (Self::INITIALIZED, readable_len),
            VectorState::UnderConstruction => (Self::UNDER_CONSTRUCTION, 0),
        };
        self.readable_len.store(readable_len, Ordering::Relaxed);
        self.mode.store(mode, Ordering::Relaxed);
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
        match self {
            Self::Chunk { multiplier: current } => current
                .checked_mul(multiplier)
                .map(|multiplier| Self::Chunk { multiplier })
                .ok_or_else(|| duckdb_failure_from_message("array child readable span overflows usize")),
            Self::Fixed(len) => len
                .checked_mul(multiplier)
                .map(Self::Fixed)
                .ok_or_else(|| duckdb_failure_from_message("array child readable span overflows usize")),
        }
    }
}
