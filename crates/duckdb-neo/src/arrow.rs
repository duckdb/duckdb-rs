//! Arrow C Data Interface conversion.
//!
//! [`ArrowSchema`], [`ArrowArray`] and [`ArrowStream`] own their C struct and
//! call its `release` callback when dropped. `into_raw` hands the struct, and
//! the duty to release it, to another Arrow consumer.
//!
//! The raw structs follow the C Data Interface layout, so they can be
//! transmuted to the FFI types of an Arrow library such as arrow-rs.

use std::mem::ManuallyDrop;

use crate::{
    DropHandle, Result, check_api_call,
    connection::{Connection, Context},
    data_chunk::{DataChunk, DataChunkRef},
    ffi,
    logical_type::LogicalType,
    raw::RawExt,
    schema::Schema,
    statement::BoundCollections,
};

/// An Arrow C stream over a query result, from [`ArrowQueryResult::to_arrow_stream`](crate::query_result::ArrowQueryResult::to_arrow_stream).
///
/// The query keeps running while the stream is read, so the stream borrows the
/// connection and is released when dropped.
#[derive(Debug)]
pub struct ArrowStream<'conn> {
    stream: ffi::ArrowArrayStream,
    // Dropped after `Drop` has released the stream that reads them.
    collections: BoundCollections<'conn>,
    _conn: std::marker::PhantomData<&'conn mut Connection>,
}

impl<'conn> ArrowStream<'conn> {
    pub(crate) fn new(stream: ffi::ArrowArrayStream, collections: BoundCollections<'conn>) -> Self {
        Self {
            stream,
            collections,
            _conn: std::marker::PhantomData,
        }
    }

    /// Hand the stream to an Arrow consumer, which then owns releasing it.
    ///
    /// The returned [`BoundCollections`] are the collections the query reads.
    ///
    /// # Safety
    ///
    /// The stream no longer borrows the connection, so the caller must release it
    /// before the connection is used again or dropped, and must keep the returned
    /// collections alive until then.
    pub unsafe fn into_raw(self) -> (ffi::ArrowArrayStream, BoundCollections<'conn>) {
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is never dropped, so each field is moved out exactly once.
        unsafe { (std::ptr::read(&this.stream), std::ptr::read(&this.collections)) }
    }
}

impl Drop for ArrowStream<'_> {
    fn drop(&mut self) {
        if let Some(release) = self.stream.release {
            unsafe { release(&mut self.stream) };
        }
        self.stream.release = None;
    }
}

/// An owned Arrow C schema, released when dropped.
#[derive(Debug)]
pub struct ArrowSchema {
    raw: ffi::ArrowSchema,
}

impl ArrowSchema {
    /// Take ownership of a schema from another Arrow producer.
    ///
    /// # Safety
    ///
    /// `raw` must be a valid Arrow C schema, or have a null `release`. Its
    /// `release` callback is called on drop.
    pub unsafe fn from_raw(raw: ffi::ArrowSchema) -> Self {
        Self { raw }
    }

    /// Hand the schema to an Arrow consumer, which then owns releasing it.
    pub fn into_raw(mut self) -> ffi::ArrowSchema {
        std::mem::replace(&mut self.raw, ffi::ArrowSchema::empty())
    }
}

impl Drop for ArrowSchema {
    fn drop(&mut self) {
        if let Some(release) = self.raw.release {
            unsafe { release(&mut self.raw) };
        }
        self.raw.release = None;
    }
}

/// An owned Arrow C array, released when dropped.
#[derive(Debug)]
pub struct ArrowArray {
    raw: ffi::ArrowArray,
}

impl ArrowArray {
    /// Take ownership of an array from another Arrow producer.
    ///
    /// # Safety
    ///
    /// `raw` must be a valid Arrow C array, or have a null `release`. Its
    /// `release` callback is called on drop.
    pub unsafe fn from_raw(raw: ffi::ArrowArray) -> Self {
        Self { raw }
    }

    /// Hand the array to an Arrow consumer, which then owns releasing it.
    pub fn into_raw(mut self) -> ffi::ArrowArray {
        std::mem::replace(&mut self.raw, ffi::ArrowArray::empty())
    }

    /// Return the number of rows.
    pub fn len(&self) -> usize {
        self.raw.length as usize
    }

    /// Return whether the array has no rows.
    pub fn is_empty(&self) -> bool {
        self.raw.length == 0
    }
}

impl Drop for ArrowArray {
    fn drop(&mut self) {
        if let Some(release) = self.raw.release {
            unsafe { release(&mut self.raw) };
        }
        self.raw.release = None;
    }
}

/// Converts DuckDB chunks into Arrow arrays using a fixed schema.
///
/// Feed chunks with [`Self::append`] and drain completed arrays with [`Self::next_array`].
///
/// The context's Arrow settings are copied at creation, but the copy keeps a
/// raw pointer to the client context, so the exporter borrows the context for
/// `'ctx` and must not outlive it.
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_arrow_exporter_destroy)]
pub struct ArrowExporter<'ctx> {
    handle: ffi::duckdb_v2_arrow_exporter_handle,
    _context: std::marker::PhantomData<&'ctx Context>,
}

impl<'ctx> ArrowExporter<'ctx> {
    /// Capture the column schema and Arrow settings from an active transaction.
    ///
    /// `None` or `Some(0)` imposes no batch-size limit.
    pub fn new(context: &'ctx Context, columns: &[(&str, LogicalType)], batch_size: Option<usize>) -> Result<Self> {
        let converted = columns
            .iter()
            .map(|(name, lt)| ((*name).into(), lt.handle))
            .collect::<Vec<(ffi::duckdb_v2_str, ffi::duckdb_v2_logical_type_handle)>>();
        let name_ptrs = converted.iter().map(|x| x.0).collect::<Vec<_>>();
        let logical_type_handles = converted.iter().map(|x| x.1).collect::<Vec<_>>();

        check_api_call!(
            ffi::duckdb_v2_arrow_exporter_create,
            context.raw(),
            logical_type_handles.as_ptr(),
            name_ptrs.as_ptr(),
            logical_type_handles.len() as u64,
            batch_size.unwrap_or(0) as u64,
            RET
        )
        .map(|handle| ArrowExporter {
            handle,
            _context: std::marker::PhantomData,
        })
    }

    /// Copy a chunk into Arrow buffers, flushing any partial batch when `flush` is true.
    ///
    /// Accepts an owned [`DataChunk`] or a chunk borrowed from a callback; the
    /// chunk is only read.
    pub fn append(&mut self, chunk: &DataChunkRef<'_>, flush: bool) -> Result<()> {
        // With `consume` false DuckDB leaves the handle untouched, so a local copy suffices.
        let mut handle = chunk.handle;
        check_api_call!(
            ffi::duckdb_v2_arrow_exporter_append,
            self.handle,
            &mut handle,
            false,
            flush
        )
    }

    /// Flush any partial batch without appending another chunk.
    pub fn flush(&mut self) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_arrow_exporter_append,
            self.handle,
            std::ptr::null_mut(),
            false,
            true
        )
    }

    /// Return the Arrow schema.
    pub fn schema(&self) -> Result<ArrowSchema> {
        check_api_call!(ffi::duckdb_v2_arrow_exporter_get_schema, self.handle, RET).map(|raw| ArrowSchema { raw })
    }

    /// Take the next completed array, or `None` when none is ready.
    pub fn next_array(&mut self) -> Result<Option<ArrowArray>> {
        // DuckDB only clears `release` when nothing is ready, so start from a fully initialized array.
        let mut raw = ffi::ArrowArray::empty();
        check_api_call!(ffi::duckdb_v2_arrow_exporter_next_array, self.handle, &mut raw)?;
        // DuckDB signals "nothing ready" with a released array.
        Ok(raw.release.is_some().then_some(ArrowArray { raw }))
    }
}

/// Converts Arrow arrays of a fixed schema into DuckDB chunks.
///
/// The importer runs every conversion under the context it was created with,
/// so it borrows that context and cannot outlive it.
#[derive(Debug, DropHandle)]
#[drop_handle(ffi::duckdb_v2_arrow_importer_destroy)]
pub struct ArrowImporter<'ctx> {
    handle: ffi::duckdb_v2_arrow_importer_handle,
    _context: std::marker::PhantomData<&'ctx Context>,
}

impl<'ctx> ArrowImporter<'ctx> {
    /// Resolve an Arrow schema using the context's active transaction.
    ///
    /// Takes the schema and releases it once resolved. `None` or `Some(0)`
    /// imposes no chunk-size limit.
    pub fn new(context: &'ctx Context, schema: impl Into<ArrowSchema>, batch_size: Option<usize>) -> Result<Self> {
        // DuckDB only reads the schema while resolving it, so it is released on return.
        let mut schema = schema.into();
        check_api_call!(
            ffi::duckdb_v2_arrow_importer_create,
            context.raw(),
            &mut schema.raw,
            batch_size.unwrap_or(0) as u64,
            RET
        )
        .map(|handle| ArrowImporter {
            handle,
            _context: std::marker::PhantomData,
        })
    }

    /// Queue an array for copying into chunks; `flush` releases any final partial batch.
    ///
    /// The caller keeps ownership of the array, and every produced chunk is a
    /// copy that does not depend on it. Prefer [`Self::append_consumed`], which
    /// avoids the copy and has no safety requirements.
    ///
    /// # Safety
    ///
    /// The importer keeps a pointer to `array` until it is drained, meaning
    /// [`Self::next_chunk`] has returned `Ok(None)`. Until then the array must not be
    /// released, mutated, or moved. If draining fails, those requirements hold
    /// until the importer is dropped.
    pub unsafe fn append_referenced(&mut self, array: &mut ArrowArray, flush: bool) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_arrow_importer_append,
            self.handle,
            &mut array.raw,
            false,
            flush
        )
    }

    /// Hand an array over for zero-copy conversion; `flush` releases any final partial batch.
    ///
    /// Produced chunks reference the array's buffers and keep them alive, so
    /// they stay valid after the importer is dropped. On error the array is
    /// released.
    pub fn append_consumed(&mut self, array: impl Into<ArrowArray>, flush: bool) -> Result<()> {
        // DuckDB takes the array on success; on error `array` still owns it and releases it on drop.
        let mut array = array.into();
        check_api_call!(
            ffi::duckdb_v2_arrow_importer_append,
            self.handle,
            &mut array.raw,
            true,
            flush
        )
    }

    /// Flush any partial batch after draining the current array with [`Self::next_chunk`].
    pub fn flush(&mut self) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_arrow_importer_append,
            self.handle,
            std::ptr::null_mut(),
            false,
            true
        )
    }

    /// Return the owned DuckDB schema resolved from the Arrow schema.
    pub fn schema(&self) -> Result<Schema> {
        check_api_call!(ffi::duckdb_v2_arrow_importer_get_schema, self.handle, RET).map(|handle| Schema { handle })
    }

    /// Take the next chunk, or return `None` when the current array is drained.
    pub fn next_chunk(&mut self) -> Result<Option<DataChunk<'static>>> {
        let handle = check_api_call!(ffi::duckdb_v2_arrow_importer_next_chunk, self.handle, RET)?;

        if handle.is_null() {
            Ok(None)
        } else {
            Ok(Some(DataChunk::new(handle, false)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::mem::transmute;

    use arrow_array::ffi::FFI_ArrowSchema;
    use libduckdb_sys::v2 as ffi;

    use crate::{
        Parameters,
        arrow::{ArrowExporter, ArrowImporter, ArrowSchema},
        builder_helpers::scalar_callback,
        environment::{Environment, StorageLocation},
        scalar::ScalarFunctionBuilder,
        signature::{FunctionParameter, SignatureBuilder},
        types::DuckDBType,
    };

    scalar_callback!(ToArrowTest, i64, |input, result, ctx, user_data| {
        let columns = [
            ("val1", input.get_untyped_vector_at(0)?.logical_type().clone()),
            ("val2", input.get_untyped_vector_at(1)?.logical_type().clone()),
            ("val3", input.get_untyped_vector_at(2)?.logical_type().clone()),
        ];

        let mut result = result;

        let data_chunk = input.to_data_chunk(&ctx)?;

        let mut arrow_exporter = ArrowExporter::new(ctx, &columns, Some(3))?;

        let schema = arrow_exporter.schema()?;

        // Round-trips through arrow-rs, as a caller using its FFI types would.
        let schema = unsafe { transmute::<ffi::ArrowSchema, FFI_ArrowSchema>(schema.into_raw()) };
        assert_eq!(schema.child(0).name(), Some("val1"));
        assert_eq!(schema.child(1).name(), Some("val2"));
        assert_eq!(schema.child(2).name(), Some("val3"));
        let schema = unsafe { ArrowSchema::from_raw(transmute::<FFI_ArrowSchema, ffi::ArrowSchema>(schema)) };

        assert!(arrow_exporter.next_array()?.is_none());
        arrow_exporter.append(&data_chunk, false)?;
        assert!(arrow_exporter.next_array()?.is_none());
        arrow_exporter.flush()?;

        let mut importer = ArrowImporter::new(ctx, schema, Some(3))?;

        let import_schema = importer.schema()?;

        assert_eq!(import_schema.get(0)?, ("val1", i64::logical_type(&ctx)?));
        assert_eq!(import_schema.get(1)?, ("val2", bool::logical_type(&ctx)?));
        assert_eq!(import_schema.get(2)?, ("val3", String::logical_type(&ctx)?));

        let array = arrow_exporter.next_array()?.expect("the flushed chunk is ready");
        assert_eq!(array.len(), 2);
        importer.append_consumed(array, false)?;
        assert!(importer.next_chunk()?.is_none());
        importer.flush()?;

        let chunk = importer.next_chunk()?;

        assert!(chunk.is_some());
        let mut chunk = chunk.unwrap();

        assert_eq!(chunk.row_count()?, 2);

        // Imported vectors may share Arrow or dictionary buffers, so they are read-only.
        let mut imported = chunk.get_vector_at_mut::<i64>(0)?;
        assert!(!imported.is_writable());
        assert!(imported.writer().is_err());

        let val1 = chunk.get_vector_at::<i64>(0)?.reader()?;
        let val2 = chunk.get_vector_at::<bool>(1)?.reader()?;
        let val3 = chunk.get_vector_at::<String>(2)?.reader()?;

        result.set_size(chunk.row_count()?)?;
        let mut writer = result.writer()?;

        for row in 0..chunk.row_count()? {
            let v1 = val1.get(row)?;
            let v2 = val2.get(row)?;
            let v3 = val3.get(row)?;

            let sum = *v1.unwrap_or(&0)
                + v2.map_or(0, |x| if *x { 2 } else { 1 }) as i64
                + v3.unwrap_or_default().len() as i64;

            writer.set(row, Some(sum))?;
        }

        Ok(())
    });

    #[test]
    fn test_arrow_conversion() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        ScalarFunctionBuilder::new(
            "to_arrow",
            SignatureBuilder::new()
                .param(FunctionParameter::new::<i64>("val1"))
                .param(FunctionParameter::new::<bool>("val2"))
                .param(FunctionParameter::new::<String>("val3"))
                .return_type(i64::logical_type(&conn)?),
            ToArrowTest,
        )
        .register(&conn)?;

        let mut result = conn.query(
            "SELECT to_arrow(a, b, c) FROM (VALUES (2, true, 'hello'), (1, false, 'world')) AS t(a, b, c)",
            Parameters::None,
        )?;

        let chunk = result.next_chunk()?.expect("conversion must produce a result chunk");
        assert_eq!(chunk.row_count()?, 2);
        let res = chunk.get_vector_at::<i64>(0)?;
        let res_reader = res.reader()?;
        assert_eq!(*res_reader.get(0)?.unwrap_or(&0), 2 + 2 + 5);
        assert_eq!(*res_reader.get(1)?.unwrap_or(&0), 1 + 1 + 5);
        drop(chunk);
        assert!(result.next_chunk()?.is_none());

        Ok(())
    }
}
