use crate::data_chunk::VectorCollection;
use crate::logical_type::{LogicalType, LogicalTypeID};
use crate::{
    Result,
    vector::{VectorReader, VectorWriter},
};

/// Describes validation and decoding for a DuckDB logical type.
///
/// # Safety
///
/// Vector accessors trust the implementation:
///
/// - `Internal` must match DuckDB's physical layout for every logical type `validate` accepts.
/// - `get` must only read within the vector's view and the children `validate` checked.
///
/// A safe implementation is rejected:
///
/// ```compile_fail,E0200
/// # use duckdb_neo::{logical_type::LogicalTypeID, vector::{VectorReader, VectorElement}};
/// struct Wide([u64; 4]);
///
/// impl VectorElement for Wide {
///     const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER;
///     type Internal = Wide;
///     type Ref<'a> = ();
///
///     unsafe fn get<'a, U: VectorElement>(_: &VectorReader<'a, U>, _: usize, _: usize) -> Self::Ref<'a>
///     where
///         Self: 'a,
///     {
///     }
/// }
/// ```
pub unsafe trait VectorElement: Sized {
    /// The DuckDB logical type represented by this Rust type.
    const TYPE_ID: LogicalTypeID;

    /// Physical entries exposed by [`VectorReader::as_slice`](crate::vector::VectorReader::as_slice).
    /// Use `()` when the element does not expose a physical representation.
    type Internal;

    /// The borrowed or owned value returned for one vector row.
    type Ref<'a>
    where
        Self: 'a;

    /// Check the logical type and nested children before reading values.
    /// `Ok(false)` means a type mismatch; `Err` means validation failed.
    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        let _ = children;
        Ok(other.id()? == Self::TYPE_ID)
    }

    /// Borrow a value at its physical and logical indexes.
    ///
    /// Prefer [`VectorReader::get`](crate::vector::VectorReader::get) or
    /// [`VectorReader::iter`](crate::vector::VectorReader::iter), which uphold these requirements.
    ///
    /// # Safety
    ///
    /// - `reader` must have been validated as `Self`, with its descendants prepared.
    /// - `logical` must be a row of `reader`, `physical` its physical index, and the row must not be `NULL`.
    ///
    /// ```compile_fail,E0133
    /// # use duckdb_neo::vector::{Vector, VectorElement};
    /// fn read<'a>(vector: &'a Vector<'_, i8>) -> &'a i64 {
    ///     let reader = vector.reader().unwrap();
    ///     <i64 as VectorElement>::get(&reader, 10_000, 0)
    /// }
    /// ```
    unsafe fn get<'a, U: VectorElement>(reader: &VectorReader<'a, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: 'a;
}

/// Marks vector elements backed by DuckDB's byte storage and arena.
///
/// Implemented for [`String`], [`UnsafeString`](crate::types::UnsafeString),
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
pub trait HasDuckDBBytes: VectorElement {}

/// Adds a supported output representation to a readable vector element.
///
/// # Safety
///
/// `write` must only write valid values of the vector's logical type, since
/// DuckDB and the readers trust what is stored.
pub unsafe trait WritableVectorElement: VectorElement {
    /// The value accepted when writing one row.
    type Write<'a>
    where
        Self: 'a;

    /// Encode one value into a writable vector.
    ///
    /// [`VectorWriter::set`](crate::vector::VectorWriter::set) and
    /// [`VectorWriter::set_unchecked`](crate::vector::VectorWriter::set_unchecked)
    /// maintain the row's validity after
    /// successful encoding. Nested NULL values must still propagate to descendants,
    /// and value-API writers preserve cast nullness.
    ///
    /// Prefer [`VectorWriter::set`](crate::vector::VectorWriter::set), which upholds these requirements.
    ///
    /// # Safety
    ///
    /// - `index` must be less than [`VectorWriter::len`].
    ///
    /// ```compile_fail,E0133
    /// # use duckdb_neo::vector::{Vector, WritableVectorElement};
    /// fn write(vector: &mut Vector<'_, i32>) -> duckdb_neo::Result<()> {
    ///     <i32 as WritableVectorElement>::write(&mut vector.writer()?, 0, Some(1))
    /// }
    /// ```
    unsafe fn write(
        writer: &mut VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()>;
}

/// The element type has not been checked against the vector's logical type yet.
#[derive(Debug, Clone)]
pub struct Unknown;

// SAFETY: `Internal` is zero-sized and `get` reads nothing.
unsafe impl VectorElement for Unknown {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN;

    type Ref<'a> = ();

    type Internal = ();

    fn validate(_other: &LogicalType, _children: Option<&VectorCollection>) -> Result<bool> {
        Ok(true)
    }

    unsafe fn get<'a, U: VectorElement>(
        _reader: &VectorReader<'a, U>,
        _physical: usize,
        _logical: usize,
    ) -> Self::Ref<'a>
    where
        Self: 'a,
    {
    }
}
