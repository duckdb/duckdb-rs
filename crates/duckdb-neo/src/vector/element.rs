use crate::data_chunk::VectorCollection;
use crate::logical_type::{LogicalType, LogicalTypeID};
use crate::{Result, vector::Vector};

/// Describes validation and decoding for a DuckDB logical type.
pub trait VectorElement: Sized {
    /// The DuckDB logical type represented by this Rust type.
    const TYPE_ID: LogicalTypeID;

    /// The unboxed type stored contiguously in the DuckDB vector's data buffer.
    ///
    /// Used by the raw accessors on [`Vector`] such as `as_slice`.
    type Internal;

    /// The borrowed value returned for one vector row.
    type Ref<'a>
    where
        Self: 'a;

    /// Validate nested children before values are read.
    fn validate(other: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        let _ = children;
        Ok(other.type_id() == Self::TYPE_ID)
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
        Self: Sized + 'a;
}

/// Adds a supported output representation to a readable vector element.
pub trait WritableVectorElement: VectorElement {
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

impl VectorElement for Unknown {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN;

    type Ref<'a> = ();

    type Internal = ();

    unsafe fn get<'a, U: VectorElement>(_vector: &'a Vector<'_, U>, _physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        panic!("Unknown type: cannot index into data of unknown type");
    }
}
