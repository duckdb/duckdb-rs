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
/// Field readers are borrowed through accessors, so a reader cannot be paired with another vector's:
///
/// ```compile_fail,E0615
/// use duckdb_neo::{AsDuckDB, Result, vector::{RowReader, Vector}};
///
/// #[derive(AsDuckDB)]
/// #[duckdb(reader)]
/// struct Record {
///     id: Option<i32>,
/// }
///
/// fn swap<'a>(vector: &'a Vector<'a, Record>, ids: &'a Vector<'a, i32>) -> Result<()> {
///     let mut reader = vector.row_reader()?;
///     reader.id = ids.row_reader()?;
///     Ok(())
/// }
/// ```
///
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
/// Enables [`VectorWriter::write_slice`](crate::vector::VectorWriter::write_slice),
/// [`VectorWriter::write_slice_as`](crate::vector::VectorWriter::write_slice_as) and
/// [`VectorWriter::copy_from`](crate::vector::VectorWriter::copy_from).
///
/// These writes copy values unchecked, so a value can lie outside its logical type's range: a
/// `DECIMAL` value with more digits than the width, or a `TIME` past midnight. Checking every
/// value would defeat the bulk copy; the caller keeps values in range, and DuckDB's results for
/// out-of-range values, such as decimal arithmetic that overflows, are undefined.
///
/// # Safety
///
/// `Self` must have the size and alignment of one physical slot, and every value of `Self` must be
/// a row of every logical type DuckDB stores in the same physical type that is safe to read.
pub unsafe trait PlainNative:
    slot::PlainSlot + WritableNativeElement<Write = Self> + ReadableNativeElement + Copy
{
}

mod slot {
    /// DuckDB's physical types for the slots of plain elements.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Slot {
        Bool,
        I8,
        I16,
        I32,
        I64,
        I128,
        U8,
        U16,
        U32,
        U64,
        U128,
        F32,
        F64,
        Interval,
    }

    pub trait PlainSlot {
        const SLOT: Slot;
    }
}

/// Return the physical type of `logical_type`'s slots when every value of that type is safe to read.
pub(crate) fn plain_slot(logical_type: &LogicalType) -> Result<Option<slot::Slot>> {
    use LogicalTypeID::*;
    use slot::Slot;
    Ok(Some(match logical_type.id()? {
        Boolean => Slot::Bool,
        TinyInt => Slot::I8,
        SmallInt => Slot::I16,
        Integer | Date => Slot::I32,
        BigInt | Time | TimeNs | Timestamp | TimestampSec | TimestampMs | TimestampNs | TimestampTz | TimestampTzNs => {
            Slot::I64
        }
        HugeInt | Uuid => Slot::I128,
        UTinyInt => Slot::U8,
        USmallInt => Slot::U16,
        UInteger => Slot::U32,
        UBigInt | TimeTz => Slot::U64,
        UHugeInt => Slot::U128,
        Float => Slot::F32,
        Double => Slot::F64,
        Interval => Slot::Interval,
        Decimal => match crate::types::DecimalSignature::from_logical_type(logical_type)?.width {
            ..=4 => Slot::I16,
            5..=9 => Slot::I32,
            10..=18 => Slot::I64,
            _ => Slot::I128,
        },
        _ => return Ok(None),
    }))
}

macro_rules! plain_native {
    ($($type:ty => $slot:ident),* $(,)?) => {$(
        impl slot::PlainSlot for $type {
            const SLOT: slot::Slot = slot::Slot::$slot;
        }

        // SAFETY: the type is its own physical slot, and every value is a valid row.
        unsafe impl PlainNative for $type {}
    )*};
}

plain_native!(
    bool => Bool,
    i8 => I8,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    u8 => U8,
    u16 => U16,
    u32 => U32,
    u64 => U64,
    f32 => F32,
    f64 => F64,
    crate::types::HugeInt => I128,
    crate::types::UHugeInt => U128,
    crate::types::UuidValueRaw => I128,
    crate::types::DateValue => I32,
    crate::types::TimeValue => I64,
    crate::types::TimeNsValue => I64,
    crate::types::TimeTzValue => U64,
    crate::types::TimestampValue => I64,
    crate::types::TimestampSecValue => I64,
    crate::types::TimestampMsValue => I64,
    crate::types::TimestampNsValue => I64,
    crate::types::TimestampTzValue => I64,
    crate::types::TimestampTzNsValue => I64,
    crate::types::IntervalValue => Interval,
    crate::types::ShortDecimal => I16,
    crate::types::WordDecimal => I32,
    crate::types::LongDecimal => I64,
    crate::types::HugeDecimal => I128,
);
