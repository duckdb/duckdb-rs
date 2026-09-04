use crate::logical_type::{LogicalType, LogicalTypeID};
use crate::{Result, vector::Vector};

/// Describes validation and decoding for a DuckDB logical type.
pub trait VectorElement: Sized {
    /// The DuckDB logical type represented by this Rust type.
    const TYPE_ID: LogicalTypeID;

    type Internal;

    /// The borrowed value returned for one vector row.
    type Ref<'a>
    where
        Self: 'a;

    /// Validate nested children before values are read.
    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        let _ = children;
        Ok(other.type_id() == Self::TYPE_ID)
    }

    /// Borrow a value at its physical and logical indexes.
    fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
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
    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()>;
}

/// The element type has not been checked against the vector's logical type yet.
#[derive(Debug, Clone)]
pub struct Unknown;

impl VectorElement for Unknown {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN;

    type Ref<'a> = ();

    type Internal = ();

    fn get<'a, U: VectorElement>(_vector: &'a Vector<'_, U>, _physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        panic!("Unknown type: cannot index into data of unknown type");
    }
}
