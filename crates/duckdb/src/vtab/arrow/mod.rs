#[cfg(test)]
mod tests;

use super::{BindInfo, DataChunkHandle, InitInfo, LogicalTypeHandle, TableFunctionInfo, VTab};
use std::{
    collections::HashMap,
    sync::{
        LazyLock, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{ArrayData, StructArray},
    datatypes::DataType,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};

pub use crate::arrow_interop::*;
use crate::{ToSql, core::LogicalTypeId, types::ToSqlOutput};

/// The registered Arrow record batch resolved during bind.
///
/// Keeping the batch in bind data lets a bound scan finish after its
/// registration is dropped. Scan position remains isolated in
/// [`ArrowInitData`].
pub struct ArrowBindData {
    rb: RecordBatch,
}

/// Tracks how many rows of the Arrow record batch have been emitted so far.
///
/// DuckDB drives table functions with a pull model: [`ArrowVTab::func`] is
/// called repeatedly and may emit at most one `DataChunk` per call. DuckDB
/// reports that chunk capacity through `duckdb_vector_size()`, which returns
/// DuckDB's compile-time `STANDARD_VECTOR_SIZE`. Capturing that capacity here
/// lets each call slice the next window of rows, so batches larger than the
/// vector size are streamed across multiple calls instead of overflowing a
/// single chunk.
pub struct ArrowInitData {
    offset: AtomicUsize,
    vector_size: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RowSlice {
    offset: usize,
    len: usize,
}

impl ArrowInitData {
    fn new(vector_size: usize) -> Self {
        Self {
            offset: AtomicUsize::new(0),
            vector_size,
        }
    }

    fn take_slice(&self, num_rows: usize) -> Option<RowSlice> {
        let vector_size = self.vector_size;
        // DuckDB currently drives this scan serially, but if parallel scans are
        // enabled later, this atomic read-modify-write makes each claimed row
        // window disjoint. Relaxed ordering is enough because each RowSlice is
        // self-contained and no other shared state is published through the atomic.
        let offset = self
            .offset
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |offset| {
                if offset >= num_rows {
                    None
                } else {
                    Some(offset.saturating_add(vector_size).min(num_rows))
                }
            })
            // `fetch_update` retries compare-exchange contention internally.
            // Err only means the closure observed completion and declined to update.
            .ok()?;
        Some(RowSlice {
            offset,
            len: (num_rows - offset).min(vector_size),
        })
    }
}

/// The Arrow table function.
pub struct ArrowVTab;

// Registrations are not associated with a connection, so batches share a
// process-global registry. A registration owns its map entry, and binding a
// scan clones the cheap, Arc-backed RecordBatch into bind data.
static BATCHES: LazyLock<Mutex<HashMap<u64, RecordBatch>>> = LazyLock::new(|| Mutex::new(HashMap::new()));
// Start tokens in the upper half so ordinary small UBIGINT literals do not
// accidentally select registrations during any realistic process lifetime.
// Tokens are registry identifiers, not secrets.
const ARROW_BATCH_TOKEN_START: u64 = 1 << 63;
static NEXT_TOKEN: AtomicU64 = AtomicU64::new(ARROW_BATCH_TOKEN_START);

fn lock_batches() -> MutexGuard<'static, HashMap<u64, RecordBatch>> {
    BATCHES.lock().unwrap_or_else(PoisonError::into_inner)
}

impl VTab for ArrowVTab {
    type BindData = ArrowBindData;
    type InitData = ArrowInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let value = bind.get_parameter(0);
        if value.is_null() {
            return Err("ArrowVTab record batch token parameter must not be NULL".into());
        }
        let logical_type = value.logical_type_id();
        if logical_type != LogicalTypeId::UBigint {
            return Err(format!("ArrowVTab record batch token parameter must be UBIGINT, got {logical_type:?}").into());
        }
        let token = value.to_uint64();
        let rb = lock_batches()
            .get(&token)
            .cloned()
            .ok_or_else(|| format!("ArrowVTab record batch token {token} is not registered"))?;
        for f in rb.schema().fields() {
            let name = f.name();
            let logical_type = to_duckdb_logical_type_for_field(f)?;
            bind.add_result_column(name, logical_type);
        }

        Ok(ArrowBindData { rb })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let vector_size = unsafe { crate::ffi::duckdb_vector_size() } as usize;
        if vector_size == 0 {
            return Err("DuckDB vector size must be greater than zero".into());
        }

        Ok(ArrowInitData::new(vector_size))
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data();
        let rb = &func.get_bind_data().rb;
        let num_rows = rb.num_rows();
        let Some(slice) = init_info.take_slice(num_rows) else {
            output.set_len(0);
            return Ok(());
        };

        // Emit at most one vector's worth of rows per call (slicing is zero-copy)
        record_batch_to_duckdb_data_chunk(&rb.slice(slice.offset, slice.len), output)?;

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::UBigint)])
    }
}

/// An Arrow [`RecordBatch`] registration for [`ArrowVTab`].
///
/// Pass a reference as one element of a parameter list, such as
/// `statement.query_arrow([&registration])`. A registration can back multiple
/// scans and executions while it is alive. Each bound scan retains the batch,
/// so it can execute after the registration is dropped. Dropping the
/// registration releases its batch from the registry and prevents future
/// scans, including stored views, from binding it. Registrations use a
/// process-global registry and are not scoped to a connection.
///
/// Registrations must not be passed by value:
///
/// ```compile_fail
/// use duckdb::{Statement, vtab::arrow::ArrowBatchRegistration};
///
/// fn query_with_owned_registration(
///     statement: &mut Statement<'_>,
///     registration: ArrowBatchRegistration,
/// ) {
///     let _ = statement.query_arrow([registration]);
/// }
/// ```
///
/// A reference to this type implements [`ToSql`] only to pass its opaque token
/// to `arrow(...)`. Using it in another SQL position binds that implementation
/// detail as a `UBIGINT`; the value has no stable meaning and should not be
/// stored or otherwise used as data.
///
/// With DuckDB 1.5.5, dropping a stream returned by
/// [`crate::Statement::stream_arrow`]
/// before consuming it can retain the batch even after the statement and
/// registration are dropped. Another query on the same connection, or closing
/// the connection, releases it.
#[derive(Debug)]
#[must_use = "dropping the registration releases its Arrow record batch"]
pub struct ArrowBatchRegistration {
    token: u64,
}

impl ArrowBatchRegistration {
    /// Registers `rb` until this value is dropped.
    pub fn new(rb: RecordBatch) -> Self {
        let token = NEXT_TOKEN.fetch_add(1, Ordering::Relaxed);
        lock_batches().insert(token, rb);
        Self { token }
    }

    /// Converts a struct [`ArrayData`] into a registration.
    ///
    /// # Panics
    ///
    /// Panics if `data` does not describe a struct array or if the struct array
    /// contains top-level nulls, which a [`RecordBatch`] cannot represent.
    pub fn from_array_data(data: ArrayData) -> Self {
        // StructArray::from does not check the data type, so a non-struct input
        // would otherwise reach an unreachable!() inside StructArray::into_parts.
        assert!(
            matches!(data.data_type(), DataType::Struct(_)),
            "ArrowVTab registration requires a struct array, got {:?}",
            data.data_type()
        );
        let struct_array = StructArray::from(data);
        Self::new(RecordBatch::from(&struct_array))
    }

    /// Imports an Arrow FFI struct array into a registration.
    ///
    /// # Safety
    ///
    /// `array` and `schema` must describe the same valid, unreleased Arrow
    /// struct array and satisfy [`arrow::ffi::from_ffi`]'s C Data Interface
    /// contract. Ownership is transferred to this function. The backing memory
    /// and release callback must be safe to use from any thread.
    ///
    /// # Panics
    ///
    /// Panics if the FFI values cannot be imported, if they do not describe a
    /// struct array, or if that array contains top-level nulls, which a
    /// [`RecordBatch`] cannot represent.
    pub unsafe fn from_ffi(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> Self {
        // SAFETY: The caller guarantees matching, valid C Data Interface values.
        let data = unsafe { arrow::ffi::from_ffi(array, &schema) }.expect("failed to import Arrow FFI data");
        Self::from_array_data(data)
    }
}

impl ToSql for &ArrowBatchRegistration {
    fn to_sql(&self) -> crate::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.token))
    }
}

impl Drop for ArrowBatchRegistration {
    fn drop(&mut self) {
        // Keep the removed batch alive until the registry guard has dropped so
        // foreign Arrow release callbacks do not run while holding the mutex.
        let _removed = lock_batches().remove(&self.token);
    }
}
