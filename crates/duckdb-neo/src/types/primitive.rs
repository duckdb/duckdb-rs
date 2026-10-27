//! Scalar numeric and boolean logical types.
//!
//! Covers DuckDB's fixed-width numeric and boolean types (`BOOLEAN`, the
//! signed/unsigned integers, and the floating-point types), plus the
//! bind-time `ANY` logical type used in function signatures.

use super::{DuckDBType, FromValue, ToValue};
use crate::ffi;
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    raw::RawExt,
    value::{Value, ValueInput},
    vector::WritableVectorElement,
};

/// The bind-time `ANY` logical type used in function signatures.
#[derive(Debug)]
pub struct Any;

pub type HugeInt = ffi::duckdb_v2_hugeint_t;
pub type UHugeInt = ffi::duckdb_v2_uhugeint_t;

impl DuckDBType for Any {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::Any, Parameters::None)
    }
}

macro_rules! declare_primitive_from_value {
    ($type:ty, $getter:path) => {
        impl FromValue for $type {
            fn _get_inner(value: &Value) -> Result<Self> {
                check_api_call!($getter, value.raw(), RET)
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
declare_primitive_from_value!(HugeInt, ffi::duckdb_v2_value_get_hugeint);
declare_primitive_from_value!(UHugeInt, ffi::duckdb_v2_value_get_uhugeint);

impl FromValue for i128 {
    fn _get_inner(value: &Value) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_value_get_hugeint, value.raw(), RET).map(crate::value::from_hugeint)
    }
}

impl FromValue for u128 {
    fn _get_inner(value: &Value) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_value_get_uhugeint, value.raw(), RET)
            .map(|raw| (u128::from(raw.upper) << 64) | u128::from(raw.lower))
    }
}

macro_rules! declare_primitive_type {
    ($type:ty, $type_id:ident, $input:ident) => {
        declare_primitive_type!($type, $type_id, $input, std::convert::identity);
    };
    ($type:ty, $type_id:ident, $input:ident, $convert:path) => {
        impl DuckDBType for $type {
            fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
                link.logical_type_create_from_id(LogicalTypeID::$type_id, Parameters::None)
            }
        }

        impl ToValue for $type {
            fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
                link.create_value(ValueInput::$input($convert(*self)))
            }
        }
    };
}

declare_primitive_type!(bool, Boolean, Bool);
declare_primitive_type!(u8, UTinyInt, UTinyInt);
declare_primitive_type!(i8, TinyInt, TinyInt);
declare_primitive_type!(i16, SmallInt, SmallInt);
declare_primitive_type!(i32, Integer, Int);
declare_primitive_type!(i64, BigInt, BigInt);
declare_primitive_type!(u16, USmallInt, USmallInt);
declare_primitive_type!(u32, UInteger, UInt);
declare_primitive_type!(u64, UBigInt, UBigInt);
declare_primitive_type!(f32, Float, Float);
declare_primitive_type!(f64, Double, Double);
declare_primitive_type!(HugeInt, HugeInt, HugeInt);
declare_primitive_type!(UHugeInt, UHugeInt, UHugeInt);
declare_primitive_type!(i128, HugeInt, HugeInt, crate::value::hugeint);
declare_primitive_type!(u128, UHugeInt, UHugeInt, crate::value::uhugeint);

/// Implements [`crate::vector::VectorElement`] for a type whose vector storage is itself.
///
/// Shared with the temporal family, whose storage-wrapper types follow the
/// same direct pointer-cast access pattern as the primitives below.
macro_rules! declare_vector_element {
    ($type:ty, $type_id:ident) => {
        // SAFETY: `$type` is DuckDB's physical storage for `$type_id`.
        unsafe impl $crate::vector::VectorElement for $type {
            const TYPE_ID: $crate::logical_type::LogicalTypeID = $crate::logical_type::LogicalTypeID::$type_id;

            type Internal<'a> = $type;

            type Ref<'a> = &'a $type;

            unsafe fn get<'a, U: $crate::vector::VectorElement>(
                vector: &$crate::vector::VectorReader<'a, U>,
                physical: usize,
                _logical: usize,
            ) -> Self::Ref<'a>
            where
                Self: Sized + 'a,
            {
                let data_ptr = vector.as_ptr() as *const $type;
                unsafe { &*data_ptr.add(physical) }
            }
        }
    };
}
pub(crate) use declare_vector_element;

declare_vector_element!(bool, Boolean);
declare_vector_element!(u8, UTinyInt);
declare_vector_element!(i8, TinyInt);
declare_vector_element!(i16, SmallInt);
declare_vector_element!(i32, Integer);
declare_vector_element!(i64, BigInt);
declare_vector_element!(u16, USmallInt);
declare_vector_element!(u32, UInteger);
declare_vector_element!(u64, UBigInt);
declare_vector_element!(f32, Float);
declare_vector_element!(f64, Double);
declare_vector_element!(HugeInt, HugeInt);
declare_vector_element!(UHugeInt, UHugeInt);

macro_rules! declare_writable_vector_element {
    ($type:ty) => {
        // SAFETY: every `$type` value is valid for its logical type.
        unsafe impl WritableVectorElement for $type {
            type Write<'a> = $type;

            #[inline(always)]
            unsafe fn write(
                vector: &mut crate::vector::VectorWriter<'_, Self>,
                index: usize,
                value: Option<Self::Write<'_>>,
            ) -> Result<()> {
                // SAFETY: The caller guarantees a writable, flat, in-bounds row.
                unsafe { vector.write_internal(index, value) }
            }
        }
    };
}

declare_writable_vector_element!(bool);
declare_writable_vector_element!(i8);
declare_writable_vector_element!(i16);
declare_writable_vector_element!(i32);
declare_writable_vector_element!(i64);
declare_writable_vector_element!(ffi::duckdb_v2_hugeint_t);
declare_writable_vector_element!(u8);
declare_writable_vector_element!(u16);
declare_writable_vector_element!(u32);
declare_writable_vector_element!(u64);
declare_writable_vector_element!(ffi::duckdb_v2_uhugeint_t);
declare_writable_vector_element!(f32);
declare_writable_vector_element!(f64);
