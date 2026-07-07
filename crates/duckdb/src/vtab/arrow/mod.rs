#[cfg(test)]
mod tests;

use super::{BindInfo, DataChunkHandle, InitInfo, LogicalTypeHandle, TableFunctionInfo, VTab};
use std::sync::{Arc, Mutex, OnceLock, atomic::AtomicUsize};

use arrow::{
    array::{ArrayData, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    record_batch::RecordBatch,
};

pub use crate::arrow_interop::{
    WritableVector, data_chunk_to_arrow, flat_vector_to_arrow_array, record_batch_to_duckdb_data_chunk,
    to_duckdb_logical_type, to_duckdb_logical_type_for_field, to_duckdb_type_id, write_arrow_array_to_vector,
};
use crate::core::LogicalTypeId;

/// The Arrow record batch for the table function.
///
/// Bind data is shared across `func` calls and should be treated as read-only.
/// That is enough for `RecordBatch`: Arrow batches are immutable containers of
/// shared array data, and the `VTab::BindData: Send + Sync` bound requires this
/// value to be safe to share. The mutable scan position lives in
/// `ArrowInitData.offset`, so the batch itself does not need a `Mutex`.
#[repr(C)]
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
#[repr(C)]
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
    fn take_slice(&self, num_rows: usize) -> Option<RowSlice> {
        use std::sync::atomic::Ordering::Relaxed;

        let vector_size = self.vector_size;
        // DuckDB currently drives this scan serially, but if parallel scans are
        // enabled later, this atomic read-modify-write makes each claimed row
        // window disjoint. Relaxed ordering is enough because each RowSlice is
        // self-contained and no other shared state is published through the atomic.
        let offset = self
            .offset
            .fetch_update(Relaxed, Relaxed, |offset| {
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

const ARROW_QUERY_PARAMS_MARKER: usize = 0x4152_5257; // "ARRW"
fn arrow_record_batch_store() -> &'static Mutex<Vec<Arc<RecordBatch>>> {
    static STORE: OnceLock<Mutex<Vec<Arc<RecordBatch>>>> = OnceLock::new();
    STORE.get_or_init(|| Mutex::new(Vec::new()))
}

fn register_arrow_record_batch(rb: RecordBatch) -> [usize; 2] {
    // Views can rebind long after creation, so the RecordBatch allocation must
    // remain valid for the process lifetime. The Arc keeps the pointee stable
    // across Vec growth, while the static store keeps it visible to
    // LeakSanitizer as reachable process-lifetime storage.
    let mut store = arrow_record_batch_store()
        .lock()
        .expect("ArrowVTab record batch store poisoned");
    let rb = Arc::new(rb);
    let ptr = Arc::as_ptr(&rb);
    store.push(rb);
    [ptr as usize, ARROW_QUERY_PARAMS_MARKER]
}

fn arrow_query_param_usize(bind: &BindInfo, index: u64, name: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let value = bind.get_parameter(index);
    if value.is_null() {
        return Err(format!("ArrowVTab {name} parameter must not be NULL").into());
    }

    let logical_type = value.logical_type_id();
    if logical_type != LogicalTypeId::UBigint {
        return Err(format!("ArrowVTab {name} parameter must be UBIGINT, got {logical_type:?}").into());
    }

    usize::try_from(value.to_uint64()).map_err(|_| format!("ArrowVTab {name} parameter does not fit in usize").into())
}

/// Imports a record batch from the current opaque ArrowVTab query parameters.
///
/// # Safety
///
/// `address` must be a non-null pointer returned by
/// [`arrow_recordbatch_to_query_params`], and `marker` must be the matching
/// layout marker returned with it. The marker catches common misuse only and
/// does not make stale or forged pointers safe to dereference.
unsafe fn address_to_arrow_record_batch(
    address: usize,
    marker: usize,
) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let ptr = address as *const RecordBatch;
    if ptr.is_null() {
        return Err("invalid ArrowVTab record batch address".into());
    }

    if marker != ARROW_QUERY_PARAMS_MARKER {
        return Err("ArrowVTab query parameter marker mismatch; use arrow_recordbatch_to_query_params".into());
    }

    // SAFETY: The caller guarantees that `ptr` is the RecordBatch allocation
    // retained by `register_arrow_record_batch`.
    Ok(unsafe { (*ptr).clone() })
}

fn arrow_record_batch_from_ffi(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> RecordBatch {
    let array_data = unsafe { from_ffi(array, &schema) }.expect("failed to import Arrow FFI data");
    let struct_array = StructArray::from(array_data);
    RecordBatch::from(&struct_array)
}

impl VTab for ArrowVTab {
    type BindData = ArrowBindData;
    type InitData = ArrowInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let param_count = bind.get_parameter_count();
        if param_count != 2 {
            return Err(format!("Bad param count: {param_count}, expected 2").into());
        }
        let address = arrow_query_param_usize(bind, 0, "record batch address")?;
        let marker = arrow_query_param_usize(bind, 1, "marker")?;

        // SAFETY: ArrowVTab's raw-parameter API relies on callers passing
        // values returned unchanged by `arrow_recordbatch_to_query_params`.
        // Validation above catches nulls, type mismatches, and layout marker
        // mismatches, but cannot validate forged addresses.
        let rb = unsafe { address_to_arrow_record_batch(address, marker)? };
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

        Ok(ArrowInitData {
            offset: AtomicUsize::new(0),
            vector_size,
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data();
        let bind_info = func.get_bind_data();

        let rb = &bind_info.rb;
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
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::UBigint), // record batch address
            LogicalTypeHandle::from(LogicalTypeId::UBigint), // query parameter marker
        ])
    }
}

/// Pass a RecordBatch to DuckDB.
///
/// This returns opaque query parameters for [`ArrowVTab`].
///
/// Each call permanently retains one [`RecordBatch`] allocation in a
/// process-global arena that is never freed, so stored views can rebind the same
/// parameters later. Memory grows monotonically. Do not call this per row or
/// per query. Create these parameters once per logical table or view.
///
/// # Panics
///
/// Panics if the process-global ArrowVTab record batch store mutex is poisoned.
pub fn arrow_recordbatch_to_query_params(rb: RecordBatch) -> [usize; 2] {
    register_arrow_record_batch(rb)
}

/// Pass ArrayData to DuckDB.
///
/// This converts the [`ArrayData`] to a [`RecordBatch`] immediately. Like
/// [`arrow_recordbatch_to_query_params`], each call permanently retains one
/// [`RecordBatch`] allocation in a process-global arena that is never freed.
///
/// # Panics
///
/// Panics if the process-global ArrowVTab record batch store mutex is poisoned.
pub fn arrow_arraydata_to_query_params(data: ArrayData) -> [usize; 2] {
    let struct_array = StructArray::from(data);
    arrow_recordbatch_to_query_params(RecordBatch::from(&struct_array))
}

/// Pass array and schema to DuckDB.
///
/// This imports the FFI values immediately. Like
/// [`arrow_recordbatch_to_query_params`], each call permanently retains one
/// [`RecordBatch`] allocation in a process-global arena that is never freed.
///
/// # Panics
///
/// Panics if the FFI values cannot be imported as Arrow data, or if the
/// process-global ArrowVTab record batch store mutex is poisoned.
pub fn arrow_ffi_to_query_params(array: FFI_ArrowArray, schema: FFI_ArrowSchema) -> [usize; 2] {
    arrow_recordbatch_to_query_params(arrow_record_batch_from_ffi(array, schema))
}
