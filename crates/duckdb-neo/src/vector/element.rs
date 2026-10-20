use crate::data_chunk::VectorCollection;
use crate::logical_type::{LogicalType, LogicalTypeID};
use crate::{Result, vector::Vector};

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
/// # use duckdb_neo::{logical_type::LogicalTypeID, vector::{Vector, VectorElement}};
/// struct Wide([u64; 4]);
///
/// impl VectorElement for Wide {
///     const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER;
///     type Internal = Wide;
///     type Ref<'a> = ();
///
///     unsafe fn get<'a, U: VectorElement>(_: &'a Vector<'_, U>, _: usize, _: usize) -> Self::Ref<'a>
///     where
///         Self: 'a,
///     {
///     }
/// }
/// ```
pub unsafe trait VectorElement: Sized {
    /// The DuckDB logical type represented by this Rust type.
    const TYPE_ID: LogicalTypeID;

    /// Physical entries exposed by [`VectorView::as_slice`](crate::vector::VectorView::as_slice).
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
    /// Prefer [`Vector::get`] or [`Vector::iter`], which uphold these requirements.
    ///
    /// # Safety
    ///
    /// - `vector` must have been validated as `Self`, and its children cached.
    /// - `vector` must have a readable view.
    /// - `logical` must be a row of `vector`, `physical` its physical index, and the row must not be `NULL`.
    ///
    /// ```compile_fail,E0133
    /// # use duckdb_neo::vector::{Vector, VectorElement};
    /// fn read<'a>(vector: &'a Vector<'_, i8>) -> &'a i64 {
    ///     <i64 as VectorElement>::get(vector, 10_000, 0)
    /// }
    /// ```
    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: 'a;
}

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

    /// Write one value into a writable vector.
    ///
    /// Prefer [`Vector::write`], which upholds this requirement.
    ///
    /// # Safety
    ///
    /// `vector` must be writable, see [`Vector::is_writable`].
    ///
    /// ```compile_fail,E0133
    /// # use duckdb_neo::vector::{Vector, WritableVectorElement};
    /// fn write(vector: &mut Vector<'_, i32>) -> duckdb_neo::Result<()> {
    ///     <i32 as WritableVectorElement>::write(vector, 0, Some(1))
    /// }
    /// ```
    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()>;
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

    unsafe fn get<'a, U: VectorElement>(_vector: &'a Vector<'_, U>, _physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
    }
}
