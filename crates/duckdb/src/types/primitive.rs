//! Scalar numeric and boolean logical types.
//!
//! Covers DuckDB's fixed-width numeric and boolean types (`BOOLEAN`, the
//! signed/unsigned integers, and the floating-point types), plus the
//! bind-time `ANY` logical type used in function signatures.

use super::{DuckDBType, FromValue, ToValue};
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Vector, WritableVectorElement},
};
use libduckdb_sys as ffi;

/// The bind-time `ANY` logical type used in function signatures.
pub struct Any;

impl DuckDBType for Any {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ANY, Parameters::None)
    }
}

macro_rules! declare_primitive_from_value {
    ($type:ty, $getter:path) => {
        impl FromValue for $type {
            fn from_value(value: &Value) -> Result<Self> {
                check_api_call!($getter, **value, RET)
            }
        }
    };
}

declare_primitive_from_value!(bool, ffi::duckdb_v2_value_get_bool);
declare_primitive_from_value!(u8, ffi::duckdb_v2_value_get_utinyint);
declare_primitive_from_value!(i8, ffi::duckdb_v2_value_get_tinyint);
declare_primitive_from_value!(i16, ffi::duckdb_v2_value_get_smallint);
declare_primitive_from_value!(i32, ffi::duckdb_v2_value_get_int);
declare_primitive_from_value!(i64, ffi::duckdb_v2_value_get_bigint);
declare_primitive_from_value!(u16, ffi::duckdb_v2_value_get_usmallint);
declare_primitive_from_value!(u32, ffi::duckdb_v2_value_get_uint);
declare_primitive_from_value!(u64, ffi::duckdb_v2_value_get_ubigint);
declare_primitive_from_value!(f32, ffi::duckdb_v2_value_get_float);
declare_primitive_from_value!(f64, ffi::duckdb_v2_value_get_double);

impl FromValue for i128 {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_hugeint, **value, RET)?;
        Ok((i128::from(raw.upper) << 64) | i128::from(raw.lower))
    }
}

impl FromValue for u128 {
    fn from_value(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_uhugeint, **value, RET)?;
        Ok((u128::from(raw.upper) << 64) | u128::from(raw.lower))
    }
}

macro_rules! declare_primitive_to_value {
    ($type:ty, $type_id:ident, $input:ident) => {
        impl DuckDBType for $type {
            fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
                link.logical_type_create_from_id(LogicalTypeID::$type_id, Parameters::None)
            }
        }

        impl ToValue for $type {
            fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
                link.create_value(ValueInput::$input(*self))
            }
        }
    };
}

declare_primitive_to_value!(bool, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN, Bool);
declare_primitive_to_value!(u8, DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT, UTinyInt);
declare_primitive_to_value!(i8, DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT, TinyInt);
declare_primitive_to_value!(i16, DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT, SmallInt);
declare_primitive_to_value!(i32, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER, Int);
declare_primitive_to_value!(i64, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT, BigInt);
declare_primitive_to_value!(u16, DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT, USmallInt);
declare_primitive_to_value!(u32, DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER, UInt);
declare_primitive_to_value!(u64, DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT, UBigInt);
declare_primitive_to_value!(f32, DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT, Float);
declare_primitive_to_value!(f64, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE, Double);
declare_primitive_to_value!(i128, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT, HugeInt);
declare_primitive_to_value!(u128, DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT, UHugeInt);

/// Implements [`VectorElement`] for a type whose vector storage is itself.
///
/// Shared with the temporal family, whose storage-wrapper types follow the
/// same direct pointer-cast access pattern as the primitives below.
macro_rules! DeclareVectorElement {
    ($type:tt , $type_id:ident) => {
        impl $crate::vector::VectorElement for $type {
            const TYPE_ID: $crate::logical_type::LogicalTypeID = $crate::logical_type::LogicalTypeID::$type_id;

            type Internal = $type;

            type Ref<'a> = &'a $type;

            fn get<'a, U: $crate::vector::VectorElement>(
                vector: &'a $crate::vector::Vector<'_, U>,
                physical: usize,
                _logical: usize,
            ) -> Self::Ref<'a>
            where
                Self: Sized + 'a,
            {
                let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const $type;
                unsafe { &*data_ptr.add(physical) }
            }
        }
    };
}
pub(crate) use DeclareVectorElement;

DeclareVectorElement!(bool, DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN);
DeclareVectorElement!(u8, DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT);
DeclareVectorElement!(i8, DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT);
DeclareVectorElement!(i16, DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT);
DeclareVectorElement!(i32, DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
DeclareVectorElement!(i64, DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
DeclareVectorElement!(u16, DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT);
DeclareVectorElement!(u32, DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER);
DeclareVectorElement!(u64, DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT);
DeclareVectorElement!(f32, DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT);
DeclareVectorElement!(f64, DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
DeclareVectorElement!(i128, DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT);
DeclareVectorElement!(u128, DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT);

macro_rules! DeclareWritableVectorElement {
    ($type:ty) => {
        impl WritableVectorElement for $type {
            type Write<'a> = $type;

            fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
                vector.write_raw(index, value)
            }
        }
    };
}

DeclareWritableVectorElement!(bool);
DeclareWritableVectorElement!(i8);
DeclareWritableVectorElement!(i16);
DeclareWritableVectorElement!(i32);
DeclareWritableVectorElement!(i64);
DeclareWritableVectorElement!(i128);
DeclareWritableVectorElement!(u8);
DeclareWritableVectorElement!(u16);
DeclareWritableVectorElement!(u32);
DeclareWritableVectorElement!(u64);
DeclareWritableVectorElement!(u128);
DeclareWritableVectorElement!(f32);
DeclareWritableVectorElement!(f64);
