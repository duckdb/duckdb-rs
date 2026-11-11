use crate::data_chunk::VectorCollection;
use crate::logical_type::{LogicalType, LogicalTypeID};
use crate::{
    Result,
    vector::{VectorReader, VectorWriter},
};

/// Describes the physical storage of a DuckDB logical type.
///
/// Every element type is native: it reads and writes its own vector's slots. Nested types read
/// their slot only, e.g. a `LIST` row's [`ListIndex`](crate::types::ListIndex) or a `STRUCT` row's
/// validity; their children are read and written through [`WithReader`](crate::vector::WithReader)
/// and [`WithWriter`](crate::vector::WithWriter), e.g. a [`ListReader`](crate::vector::ListReader).
/// Validation establishes the storage layout for both.
///
/// Derive `AsDuckDB` with `#[duckdb(reader, writer)]` for named struct readers and writers:
///
/// ```
/// use duckdb_neo::{AsDuckDB, Result, vector::{RowReader, Vector}};
///
/// #[derive(AsDuckDB)]
/// #[duckdb(reader)]
/// struct Record {
///     id: Option<i32>,
/// }
///
/// fn id(vector: &Vector<'_, Record>) -> Result<Option<i32>> {
///     Ok(vector.row_reader()?.get(0)?.and_then(|row| row.id).copied())
/// }
/// ```
///
/// The borrowed projection keeps every field nullable, even when the Rust input field is required.
/// Readers borrow the vector and prevent mutation while in use:
///
/// ```compile_fail,E0502
/// use duckdb_neo::{AsDuckDB, Result, vector::{RowReader, Vector}};
///
/// #[derive(AsDuckDB)]
/// #[duckdb(reader)]
/// struct Record {
///     id: Option<i32>,
/// }
///
/// fn resize(vector: &mut Vector<'_, Record>) -> Result<()> {
///     let reader = vector.row_reader()?;
///     vector.set_size(10)?;
///     let _ = reader.get(0)?;
///     Ok(())
/// }
/// ```
///
/// # Safety
///
/// Vector accessors trust the implementation: `Native<'a>` must match DuckDB's physical layout for
/// every lifetime and logical type `validate` accepts, and `validate` must establish the layout every
/// reader and writer of the type relies on, including child types and field order.
///
/// A safe implementation is rejected:
///
/// ```compile_fail,E0200
/// # use duckdb_neo::{logical_type::LogicalTypeID, vector::NativeElement};
/// struct Wide([u64; 4]);
///
/// impl NativeElement for Wide {
///     const TYPE_ID: LogicalTypeID = LogicalTypeID::Integer;
///     type Native<'a> = Wide;
/// }
/// ```
pub unsafe trait NativeElement: Sized {
    /// The DuckDB logical type represented by this Rust type.
    const TYPE_ID: LogicalTypeID;

    /// Physical entries exposed by [`VectorReader::as_slice`](crate::vector::VectorReader::as_slice).
    /// Use `()` when the element does not expose a physical representation.
    /// The lifetime bounds any storage referenced by an entry.
    type Native<'a>;

    /// Check the logical type and nested children when attaching this element type to a vector.
    /// A successful check must establish the layout for both reading and writing.
    /// `Ok(false)` means a type mismatch; `Err` means validation failed.
    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        let _ = children;
        Ok(other.id()? == Self::TYPE_ID)
    }
}

/// Element types whose rows decode from their physical slot.
///
/// # Safety
///
/// `get` must only read within the vector's view.
pub unsafe trait ReadableNativeElement: NativeElement {
    /// The borrowed or owned value returned for one vector row.
    type Ref<'a>
    where
        Self: 'a;

    /// Borrow a value at its physical and logical indexes.
    ///
    /// Prefer [`VectorReader::get`](crate::vector::VectorReader::get) or
    /// [`VectorReader::iter`](crate::vector::VectorReader::iter), which uphold these requirements.
    ///
    /// # Safety
    ///
    /// - `reader` must have been validated as `Self`.
    /// - `logical` must be a row of `reader`, `physical` its physical index, and the row must not be `NULL`.
    ///
    /// ```compile_fail,E0133
    /// # use duckdb_neo::vector::{Vector, ReadableNativeElement};
    /// fn read<'a>(vector: &'a Vector<'_, i8>) -> &'a i64 {
    ///     let reader = vector.reader().unwrap();
    ///     <i64 as ReadableNativeElement>::get(&reader, 10_000, 0)
    /// }
    /// ```
    unsafe fn get<'a, U: NativeElement>(reader: &VectorReader<'a, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: 'a;
}

/// Marks vector elements backed by DuckDB's byte storage and arena.
///
/// Implemented for [`String`], [`UncheckedString`](crate::types::UncheckedString),
/// [`BlobValue`](crate::types::BlobValue), [`BitValue`](crate::types::BitValue),
/// [`BigNum`](crate::types::BigNum), and [`BigNumValue`](crate::types::BigNumValue).
///
/// Non-byte vectors do not expose an arena:
///
/// ```compile_fail,E0599
/// use duckdb_neo::vector::Vector;
///
/// fn arena(vector: &mut Vector<'_, i32>) -> duckdb_neo::Result<()> {
///     let _ = vector.writer()?.arena();
///     Ok(())
/// }
/// ```
pub trait HasDuckDBBytes: NativeElement {}

/// Element types whose rows encode into their physical slot.
///
/// Nested types write their slot only: a `LIST` row's checked [`ListIndex`](crate::types::ListIndex),
/// or a `STRUCT` row's validity, whose fields are always initialized.
///
/// # Safety
///
/// `write` must only write valid values of the vector's logical type, since
/// DuckDB and the readers trust what is stored. It may rely on the layout checked
/// by [`NativeElement::validate`], but must enforce value-dependent constraints.
pub unsafe trait WritableNativeElement: NativeElement {
    /// The borrowed input for one row, such as `Self`, `str`, or `[u8]`.
    type Write: ?Sized;

    /// Encode one non-NULL value into a writable vector.
    ///
    /// [`VectorWriter::set`](crate::vector::VectorWriter::set) marks the row valid after successful
    /// encoding; prefer it, since it upholds these requirements.
    ///
    /// # Safety
    ///
    /// - `index` must be less than [`VectorWriter::len`].
    ///
    /// ```compile_fail,E0133
    /// # use duckdb_neo::vector::{Vector, WritableNativeElement};
    /// fn write(vector: &mut Vector<'_, i32>) -> duckdb_neo::Result<()> {
    ///     <i32 as WritableNativeElement>::write(&mut vector.writer()?, 0, &1)
    /// }
    /// ```
    unsafe fn write(writer: &mut VectorWriter<'_, Self>, index: usize, value: &Self::Write) -> Result<()>;

    /// Prepare a row for SQL NULL, e.g. by nulling the descendants DuckDB requires to be NULL.
    ///
    /// [`VectorWriter::set_null`](crate::vector::VectorWriter::set_null) marks the row NULL afterwards.
    ///
    /// # Safety
    ///
    /// - `index` must be less than [`VectorWriter::len`].
    unsafe fn write_null(writer: &mut VectorWriter<'_, Self>, index: usize) -> Result<()> {
        let _ = (writer, index);
        Ok(())
    }
}

/// The element type has not been checked against the vector's logical type yet.
#[derive(Debug, Clone)]
pub struct Unknown;

// SAFETY: `Native` is zero-sized.
unsafe impl NativeElement for Unknown {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Unknown;

    type Native<'a> = ();

    fn validate(_other: &LogicalType, _children: Option<&VectorCollection>) -> Result<bool> {
        Ok(true)
    }
}

// SAFETY: `get` reads nothing.
unsafe impl ReadableNativeElement for Unknown {
    type Ref<'a> = ();

    #[inline]
    unsafe fn get<'a, U: NativeElement>(
        _reader: &VectorReader<'a, U>,
        _physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
    }
}

/// Element types stored as themselves: a slot holds exactly one `Self`, so slices copy unchanged.
///
/// Enables [`VectorWriter::write_slice`](crate::vector::VectorWriter::write_slice) and
/// [`VectorWriter::copy_from`](crate::vector::VectorWriter::copy_from).
///
/// # Safety
///
/// `Self` must have the size and alignment of one physical slot, and every value of `Self` must be
/// a valid row of every logical type `validate` accepts.
pub unsafe trait PlainNative: WritableNativeElement<Write = Self> + ReadableNativeElement + Copy {}

macro_rules! plain_native {
    ($($type:ty),* $(,)?) => {$(
        // SAFETY: the type is its own physical slot, and every value is a valid row.
        unsafe impl PlainNative for $type {}
    )*};
}

plain_native!(
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    f32,
    f64,
    crate::types::HugeInt,
    crate::types::UHugeInt,
    crate::types::DateValue,
    crate::types::TimeValue,
    crate::types::TimeNsValue,
    crate::types::TimeTzValue,
    crate::types::TimestampValue,
    crate::types::TimestampSecValue,
    crate::types::TimestampMsValue,
    crate::types::TimestampNsValue,
    crate::types::TimestampTzValue,
    crate::types::TimestampTzNsValue,
    crate::types::IntervalValue,
    crate::types::ShortDecimal,
    crate::types::WordDecimal,
    crate::types::LongDecimal,
);
