//! Owned DuckDB values and their typed payloads.

use std::{fmt::Debug, ops::Deref};

use libduckdb_sys::{self as ffi, DuckDBStr};

use crate::{
    Result,
    builder_helpers::context_and_connection_fn,
    check_api_call, check_api_call_no_err,
    connection::{Connection, Context, FFILink},
    error::{DuckDBError, Error},
    logical_type::LogicalType,
    types::FromValue,
};

/// An owned logical type and value payload.
///
/// Values represent SQL constants independently of vectors. They can be passed
/// as prepared-statement parameters, function defaults, and logical-type
/// parameters. Nested values own copies of their children. For query results,
/// prefer typed [`crate::vector::Vector`] access; extracting and decoding
/// individual values is the slower path.
///
/// # Example
/// ```
/// use duckdb_rs::{environment::Environment, environment::StorageLocation, ToValue};
///
/// # fn main() -> duckdb_rs::Result<()> {
/// let env = Environment::new()?;
/// let db = env.open(StorageLocation::InMemory)?;
/// let conn = db.connect()?;
///
/// let value = vec![Some(1_i32), None].value(&conn)?;
///
/// assert_eq!(value.child_count()?, 2);
/// assert!(value.get_child(1)?.is_null()?);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct Value {
    pub(crate) handle: ffi::duckdb_v2_value_handle,
}

#[doc(hidden)]
pub enum ValueInput<'a> {
    Null(&'a LogicalType),
    Bool(bool),
    UTinyInt(u8),
    TinyInt(i8),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    USmallInt(u16),
    UInt(u32),
    UBigInt(u64),
    Float(f32),
    Double(f64),
    HugeInt(i128),
    UHugeInt(u128),
    Varchar(&'a str),
    Blob(&'a [u8]),
    Type(&'a LogicalType),
    Date(i32),
    Time(i64),
    TimeNs(i64),
    TimeTz(u64),
    Timestamp(i64),
    TimestampSec(i64),
    TimestampMs(i64),
    TimestampNs(i64),
    TimestampTz(i64),
    TimestampTzNs(i64),
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
    Decimal {
        value: i128,
        width: u8,
        scale: u8,
    },
    Uuid(i128),
    Bit(&'a [u8]),
    BigNum(&'a [u8]),
    List {
        child_type: &'a LogicalType,
        children: &'a [Value],
    },
    Array {
        child_type: &'a LogicalType,
        children: &'a [Value],
    },
    Struct {
        names: &'a [&'a str],
        children: &'a [Value],
    },
    Tuple(&'a [Value]),
    Map {
        key_type: &'a LogicalType,
        value_type: &'a LogicalType,
        keys: &'a [Value],
        values: &'a [Value],
    },
}

fn value_handles(values: &[Value]) -> Vec<ffi::duckdb_v2_value_handle> {
    values.iter().map(|value| value.handle).collect()
}

fn bytes(value: &[u8]) -> ffi::DuckDBStr<'_> {
    ffi::DuckDBStr {
        ptr: value.as_ptr().cast(),
        len: value.len() as u64,
    }
}

fn hugeint(value: i128) -> ffi::duckdb_v2_hugeint_t {
    ffi::duckdb_v2_hugeint_t {
        lower: value as u64,
        upper: (value >> 64) as i64,
    }
}

fn uhugeint(value: u128) -> ffi::duckdb_v2_uhugeint_t {
    ffi::duckdb_v2_uhugeint_t {
        lower: value as u64,
        upper: (value >> 64) as u64,
    }
}

macro_rules! create_value {
    ($function:path, $link:expr $(, $arg:expr)*) => {{
        let handle = check_api_call!($function, $link $(, $arg)*, RET)?;
        Ok(Value { handle })
    }};
}

macro_rules! create_value_dispatch {
    (
        $link:expr,
        $input:expr,
        bool = $bool:path,
        utinyint = $utinyint:path,
        tinyint = $tinyint:path,
        smallint = $smallint:path,
        int = $int:path,
        bigint = $bigint:path,
        usmallint = $usmallint:path,
        uint = $uint:path,
        ubigint = $ubigint:path,
        float = $float:path,
        double = $double:path,
        hugeint = $hugeint:path,
        uhugeint = $uhugeint:path,
        varchar = $varchar:path,
        blob = $blob:path,
        null = $null:path,
        type = $type:path,
        date = $date:path,
        time = $time:path,
        time_ns = $time_ns:path,
        time_tz = $time_tz:path,
        timestamp = $timestamp:path,
        timestamp_sec = $timestamp_sec:path,
        timestamp_ms = $timestamp_ms:path,
        timestamp_ns = $timestamp_ns:path,
        timestamp_tz = $timestamp_tz:path,
        timestamp_tz_ns = $timestamp_tz_ns:path,
        interval = $interval:path,
        decimal = $decimal:path,
        uuid = $uuid:path,
        bit = $bit:path,
        bignum = $bignum:path,
        list = $list:path,
        array = $array:path,
        struct = $struct:path,
        tuple = $tuple:path,
        map = $map:path $(,)?
    ) => {
        match $input {
            ValueInput::Null(logical_type) => create_value!($null, $link, logical_type.handle),
            ValueInput::Bool(value) => create_value!($bool, $link, value),
            ValueInput::UTinyInt(value) => create_value!($utinyint, $link, value),
            ValueInput::TinyInt(value) => create_value!($tinyint, $link, value),
            ValueInput::SmallInt(value) => create_value!($smallint, $link, value),
            ValueInput::Int(value) => create_value!($int, $link, value),
            ValueInput::BigInt(value) => create_value!($bigint, $link, value),
            ValueInput::USmallInt(value) => create_value!($usmallint, $link, value),
            ValueInput::UInt(value) => create_value!($uint, $link, value),
            ValueInput::UBigInt(value) => create_value!($ubigint, $link, value),
            ValueInput::Float(value) => create_value!($float, $link, value),
            ValueInput::Double(value) => create_value!($double, $link, value),
            ValueInput::HugeInt(value) => create_value!($hugeint, $link, hugeint(value)),
            ValueInput::UHugeInt(value) => create_value!($uhugeint, $link, uhugeint(value)),
            ValueInput::Varchar(value) => create_value!($varchar, $link, value.into()),
            ValueInput::Blob(value) => create_value!($blob, $link, bytes(value)),
            ValueInput::Type(logical_type) => create_value!($type, $link, logical_type.handle),
            ValueInput::Date(value) => create_value!($date, $link, value),
            ValueInput::Time(value) => create_value!($time, $link, value),
            ValueInput::TimeNs(value) => create_value!($time_ns, $link, value),
            ValueInput::TimeTz(value) => create_value!($time_tz, $link, value),
            ValueInput::Timestamp(value) => create_value!($timestamp, $link, value),
            ValueInput::TimestampSec(value) => create_value!($timestamp_sec, $link, value),
            ValueInput::TimestampMs(value) => create_value!($timestamp_ms, $link, value),
            ValueInput::TimestampNs(value) => create_value!($timestamp_ns, $link, value),
            ValueInput::TimestampTz(value) => create_value!($timestamp_tz, $link, value),
            ValueInput::TimestampTzNs(value) => create_value!($timestamp_tz_ns, $link, value),
            ValueInput::Interval { months, days, micros } => {
                create_value!($interval, $link, ffi::duckdb_v2_interval_t { months, days, micros })
            }
            ValueInput::Decimal { value, width, scale } => create_value!($decimal, $link, hugeint(value), width, scale),
            ValueInput::Uuid(value) => create_value!($uuid, $link, hugeint(value as i128)),
            ValueInput::Bit(value) => create_value!($bit, $link, bytes(value)),
            ValueInput::BigNum(value) => create_value!($bignum, $link, bytes(value)),
            ValueInput::List { child_type, children } => {
                let children = value_handles(children);
                create_value!(
                    $list,
                    $link,
                    child_type.handle,
                    children.as_ptr(),
                    children.len() as u64
                )
            }
            ValueInput::Array { child_type, children } => {
                let children = value_handles(children);
                create_value!(
                    $array,
                    $link,
                    child_type.handle,
                    children.as_ptr(),
                    children.len() as u64
                )
            }
            ValueInput::Struct { names, children } => {
                let names = names.iter().map(|name| (*name).into()).collect::<Vec<_>>();
                let children = value_handles(children);
                create_value!($struct, $link, names.as_ptr(), children.as_ptr(), children.len() as u64)
            }
            ValueInput::Tuple(children) => {
                let children = value_handles(children);
                create_value!($tuple, $link, children.as_ptr(), children.len() as u64)
            }
            ValueInput::Map {
                key_type,
                value_type,
                keys,
                values,
            } => {
                let keys = value_handles(keys);
                let values = value_handles(values);
                create_value!(
                    $map,
                    $link,
                    key_type.handle,
                    value_type.handle,
                    keys.as_ptr(),
                    values.as_ptr(),
                    keys.len() as u64
                )
            }
        }
    };
}

pub(crate) fn create_with_connection(connection: &Connection, input: ValueInput<'_>) -> Result<Value> {
    create_value_dispatch!(
        **connection,
        input,
        bool = ffi::duckdb_v2_value_create_bool_with_connection,
        utinyint = ffi::duckdb_v2_value_create_utinyint_with_connection,
        tinyint = ffi::duckdb_v2_value_create_tinyint_with_connection,
        smallint = ffi::duckdb_v2_value_create_smallint_with_connection,
        int = ffi::duckdb_v2_value_create_int_with_connection,
        bigint = ffi::duckdb_v2_value_create_bigint_with_connection,
        usmallint = ffi::duckdb_v2_value_create_usmallint_with_connection,
        uint = ffi::duckdb_v2_value_create_uint_with_connection,
        ubigint = ffi::duckdb_v2_value_create_ubigint_with_connection,
        float = ffi::duckdb_v2_value_create_float_with_connection,
        double = ffi::duckdb_v2_value_create_double_with_connection,
        hugeint = ffi::duckdb_v2_value_create_hugeint_with_connection,
        uhugeint = ffi::duckdb_v2_value_create_uhugeint_with_connection,
        varchar = ffi::duckdb_v2_value_create_varchar_with_connection,
        blob = ffi::duckdb_v2_value_create_blob_with_connection,
        null = ffi::duckdb_v2_value_create_null_with_connection,
        type = ffi::duckdb_v2_value_create_type_with_connection,
        date = ffi::duckdb_v2_value_create_date_with_connection,
        time = ffi::duckdb_v2_value_create_time_with_connection,
        time_ns = ffi::duckdb_v2_value_create_time_ns_with_connection,
        time_tz = ffi::duckdb_v2_value_create_time_tz_with_connection,
        timestamp = ffi::duckdb_v2_value_create_timestamp_with_connection,
        timestamp_sec = ffi::duckdb_v2_value_create_timestamp_sec_with_connection,
        timestamp_ms = ffi::duckdb_v2_value_create_timestamp_ms_with_connection,
        timestamp_ns = ffi::duckdb_v2_value_create_timestamp_ns_with_connection,
        timestamp_tz = ffi::duckdb_v2_value_create_timestamp_tz_with_connection,
        timestamp_tz_ns = ffi::duckdb_v2_value_create_timestamp_tz_ns_with_connection,
        interval = ffi::duckdb_v2_value_create_interval_with_connection,
        decimal = ffi::duckdb_v2_value_create_decimal_with_connection,
        uuid = ffi::duckdb_v2_value_create_uuid_with_connection,
        bit = ffi::duckdb_v2_value_create_bit_with_connection,
        bignum = ffi::duckdb_v2_value_create_bignum_with_connection,
        list = ffi::duckdb_v2_value_create_list_with_connection,
        array = ffi::duckdb_v2_value_create_array_with_connection,
        struct = ffi::duckdb_v2_value_create_struct_with_connection,
        tuple = ffi::duckdb_v2_value_create_tuple_with_connection,
        map = ffi::duckdb_v2_value_create_map_with_connection,
    )
}

pub(crate) fn create_with_context(context: &Context, input: ValueInput<'_>) -> Result<Value> {
    create_value_dispatch!(
        **context,
        input,
        bool = ffi::duckdb_v2_value_create_bool_with_context,
        utinyint = ffi::duckdb_v2_value_create_utinyint_with_context,
        tinyint = ffi::duckdb_v2_value_create_tinyint_with_context,
        smallint = ffi::duckdb_v2_value_create_smallint_with_context,
        int = ffi::duckdb_v2_value_create_int_with_context,
        bigint = ffi::duckdb_v2_value_create_bigint_with_context,
        usmallint = ffi::duckdb_v2_value_create_usmallint_with_context,
        uint = ffi::duckdb_v2_value_create_uint_with_context,
        ubigint = ffi::duckdb_v2_value_create_ubigint_with_context,
        float = ffi::duckdb_v2_value_create_float_with_context,
        double = ffi::duckdb_v2_value_create_double_with_context,
        hugeint = ffi::duckdb_v2_value_create_hugeint_with_context,
        uhugeint = ffi::duckdb_v2_value_create_uhugeint_with_context,
        varchar = ffi::duckdb_v2_value_create_varchar_with_context,
        blob = ffi::duckdb_v2_value_create_blob_with_context,
        null = ffi::duckdb_v2_value_create_null_with_context,
        type = ffi::duckdb_v2_value_create_type_with_context,
        date = ffi::duckdb_v2_value_create_date_with_context,
        time = ffi::duckdb_v2_value_create_time_with_context,
        time_ns = ffi::duckdb_v2_value_create_time_ns_with_context,
        time_tz = ffi::duckdb_v2_value_create_time_tz_with_context,
        timestamp = ffi::duckdb_v2_value_create_timestamp_with_context,
        timestamp_sec = ffi::duckdb_v2_value_create_timestamp_sec_with_context,
        timestamp_ms = ffi::duckdb_v2_value_create_timestamp_ms_with_context,
        timestamp_ns = ffi::duckdb_v2_value_create_timestamp_ns_with_context,
        timestamp_tz = ffi::duckdb_v2_value_create_timestamp_tz_with_context,
        timestamp_tz_ns = ffi::duckdb_v2_value_create_timestamp_tz_ns_with_context,
        interval = ffi::duckdb_v2_value_create_interval_with_context,
        decimal = ffi::duckdb_v2_value_create_decimal_with_context,
        uuid = ffi::duckdb_v2_value_create_uuid_with_context,
        bit = ffi::duckdb_v2_value_create_bit_with_context,
        bignum = ffi::duckdb_v2_value_create_bignum_with_context,
        list = ffi::duckdb_v2_value_create_list_with_context,
        array = ffi::duckdb_v2_value_create_array_with_context,
        struct = ffi::duckdb_v2_value_create_struct_with_context,
        tuple = ffi::duckdb_v2_value_create_tuple_with_context,
        map = ffi::duckdb_v2_value_create_map_with_context,
    )
}

impl Drop for Value {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_value_destroy, &mut self.handle).unwrap();
    }
}

impl Deref for Value {
    type Target = ffi::duckdb_v2_value_handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl Value {
    /// Read this value as a Rust type.
    pub fn get<T: FromValue>(&self) -> Result<T> {
        T::from_value(self)
    }

    /// Create a typed `NULL` without a connection or callback context.
    pub fn null(logical_type: &LogicalType) -> Result<Value> {
        let handle = check_api_call!(ffi::duckdb_v2_value_create_null, logical_type.handle, RET)?;
        Ok(Value { handle })
    }

    context_and_connection_fn! {
        /// Cast a value using a connection or callback context's registered casts.
        pub fn cast_with_[context, connection](
            &self,
            target_type: LogicalType,
        ) -> Result<Value>
        {
            context_fn: ffi::duckdb_v2_value_cast_with_context,
            connection_fn: ffi::duckdb_v2_value_cast_with_connection,
        }
        let handle = check_api_call!(
            api_fn!(),
            **api_arg!(),
            self.handle,
            target_type.handle,
            RET
        )?;

        Ok(Value { handle })
    }

    /// Create a `TYPE` value carrying a logical type.
    pub fn from_logical_type<C: FFILink + ?Sized>(link: &C, logical_type: &LogicalType) -> Result<Value> {
        link.create_value(ValueInput::Type(logical_type))
    }

    /// Return the logical type carried by a `TYPE` value.
    pub fn logical_type(&self) -> Result<LogicalType> {
        let handle = check_api_call!(ffi::duckdb_v2_value_get_type, self.handle, RET)?;

        Ok(LogicalType { handle })
    }

    /// Return this value's logical type.
    pub fn fetch_logical_type(&self) -> Result<LogicalType> {
        let handle = check_api_call!(ffi::duckdb_v2_value_get_logical_type, self.handle, RET)?;

        Ok(LogicalType { handle })
    }

    /// Return the number of children in a composite value.
    pub fn child_count(&self) -> Result<usize> {
        let count: u64 = check_api_call!(ffi::duckdb_v2_value_get_child_count, self.handle, RET)?;

        Ok(count as usize)
    }

    pub(crate) fn encode_bignum(data: &[u8], is_negative: bool) -> Result<Vec<u8>> {
        let length = check_api_call!(
            ffi::duckdb_v2_bignum_encode,
            data.as_ptr(),
            data.len() as u64,
            is_negative,
            std::ptr::null_mut(),
            0,
            RET
        )?;
        let mut encoded = Vec::with_capacity(length as usize);
        let written = check_api_call!(
            ffi::duckdb_v2_bignum_encode,
            data.as_ptr(),
            data.len() as u64,
            is_negative,
            encoded.as_mut_ptr(),
            length,
            RET
        )?;
        unsafe {
            encoded.set_len(written as usize);
        }
        Ok(encoded)
    }

    pub(crate) fn decode_bignum(data: &[u8]) -> Result<(bool, Vec<u8>)> {
        let mut is_negative = false;
        let length = check_api_call!(
            ffi::duckdb_v2_bignum_decode,
            data.as_ptr(),
            data.len() as u64,
            std::ptr::null_mut(),
            0,
            RET,
            &mut is_negative
        )?;
        let mut decoded = Vec::with_capacity(length as usize);
        let written = check_api_call!(
            ffi::duckdb_v2_bignum_decode,
            data.as_ptr(),
            data.len() as u64,
            decoded.as_mut_ptr(),
            length,
            RET,
            &mut is_negative
        )?;
        unsafe {
            decoded.set_len(written as usize);
        }
        Ok((is_negative, decoded))
    }

    /// Return an owned copy of a child value.
    ///
    /// This is a per-value slow path; prefer typed vector access for query data.
    /// A non-composite value or out-of-range index returns an error.
    pub fn get_child(&self, index: usize) -> Result<Value> {
        let child_handle = check_api_call!(ffi::duckdb_v2_value_get_child, self.handle, index as u64, RET)?;

        Ok(Value { handle: child_handle })
    }

    /// Return owned copies of all child values.
    ///
    /// This is a per-value slow path; prefer typed vector access for query data.
    pub fn children(&self) -> Result<Vec<Value>> {
        let count = self.child_count()?;
        let mut children = Vec::with_capacity(count);

        for i in 0..count {
            let child = self.get_child(i)?;
            children.push(child);
        }

        Ok(children)
    }

    /// Return whether this value is `NULL`.
    pub fn is_null(&self) -> Result<bool> {
        let is_null: bool = check_api_call!(ffi::duckdb_v2_value_is_null, self.handle, RET)?;

        Ok(is_null)
    }

    /// Render the value for diagnostics.
    pub fn dbg_string(&self) -> Result<String> {
        let capacity = check_api_call!(
            ffi::duckdb_v2_value_to_string,
            self.handle,
            std::ptr::null_mut(),
            0,
            RET
        )?;

        let buffer_capacity = capacity + 1;
        let mut text = Vec::<u8>::with_capacity(buffer_capacity as usize);

        let length = check_api_call!(
            ffi::duckdb_v2_value_to_string,
            self.handle,
            text.as_mut_ptr() as *mut i8,
            buffer_capacity,
            RET
        )?;
        unsafe {
            text.set_len(length as usize);
        }

        String::from_utf8(text).map_err(|_| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_API,
            message: "Failed to convert value string to UTF-8".to_string(),
        })
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::{
        ToValue,
        environment::{Environment, StorageLocation},
        types::BigNumValue,
    };

    #[test]
    fn test_value_create() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let value = vec![
            None,
            Some(BigNumValue {
                is_negative: true,
                magnitude: vec![1, 2, 3, 4],
            }),
            Some(BigNumValue {
                is_negative: false,
                magnitude: vec![5, 6, 7, 8],
            }),
        ]
        .value(&conn)?;

        assert_eq!(value.dbg_string()?, "[NULL, -16909060, 84281096]");
        Ok(())
    }
}
