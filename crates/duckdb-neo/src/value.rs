//! Owned DuckDB values and their typed payloads.

use crate::{DropHandle, Factory, ffi};

use crate::{
    AsRaw, FromRaw, Result, check_api_call, check_api_call_string, connection::FFILink, error::Error,
    logical_type::LogicalType, raw::RawExt, types::FromValue,
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
/// use duckdb_neo::{environment::Environment, environment::StorageLocation, ToValue};
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let env = Environment::new()?;
/// let instance = env.open(StorageLocation::InMemory)?;
/// let conn = instance.connect()?;
///
/// let value = vec![Some(1_i32), None].value(&conn)?;
///
/// assert_eq!(value.child_count()?, 2);
/// assert!(value.get_child(1)?.is_null()?);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_value_destroy)]
pub struct Value {
    pub(crate) handle: ffi::duckdb_v2_value_handle,
}

// SAFETY: an owned, self-contained C++ `Value`; `&self` methods only read it.
unsafe impl Send for Value {}
unsafe impl Sync for Value {}

#[doc(hidden)]
#[derive(Debug)]
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

/// The C API reads both arrays with one count, so a length mismatch would read past the shorter.
fn check_same_len(what: &str, left: usize, right: usize) -> Result<()> {
    if left == right {
        return Ok(());
    }
    Err(Error::invalid_input(format!(
        "{what}: lengths differ ({left} vs {right})"
    )))
}

fn value_handles(values: &[Value]) -> Vec<ffi::duckdb_v2_value_handle> {
    values.iter().map(|value| value.handle).collect()
}

fn hugeint(value: i128) -> ffi::duckdb_v2_hugeint_t {
    ffi::duckdb_v2_hugeint_t {
        lower: value as u64,
        upper: (value >> 64) as i64,
    }
}

pub(crate) fn from_hugeint(value: ffi::duckdb_v2_hugeint_t) -> i128 {
    (i128::from(value.upper) << 64) | i128::from(value.lower)
}

fn uhugeint(value: u128) -> ffi::duckdb_v2_uhugeint_t {
    ffi::duckdb_v2_uhugeint_t {
        lower: value as u64,
        upper: (value >> 64) as u64,
    }
}

macro_rules! create_value {
    ($function:path, $link:expr $(, $arg:expr)*) => {{
        check_api_call!($function, $link $(, $arg)*, RET)
            .map(|handle| Value { handle })
    }};
}

pub(crate) fn create(factory: &Factory, input: ValueInput<'_>) -> Result<Value> {
    match input {
        ValueInput::Null(logical_type) => {
            create_value!(ffi::duckdb_v2_value_create_null, factory.raw(), logical_type.handle)
        }
        ValueInput::Bool(value) => create_value!(ffi::duckdb_v2_value_create_bool, factory.raw(), value),
        ValueInput::UTinyInt(value) => create_value!(ffi::duckdb_v2_value_create_utinyint, factory.raw(), value),
        ValueInput::TinyInt(value) => create_value!(ffi::duckdb_v2_value_create_tinyint, factory.raw(), value),
        ValueInput::SmallInt(value) => create_value!(ffi::duckdb_v2_value_create_smallint, factory.raw(), value),
        ValueInput::Int(value) => create_value!(ffi::duckdb_v2_value_create_int, factory.raw(), value),
        ValueInput::BigInt(value) => create_value!(ffi::duckdb_v2_value_create_bigint, factory.raw(), value),
        ValueInput::USmallInt(value) => create_value!(ffi::duckdb_v2_value_create_usmallint, factory.raw(), value),
        ValueInput::UInt(value) => create_value!(ffi::duckdb_v2_value_create_uint, factory.raw(), value),
        ValueInput::UBigInt(value) => create_value!(ffi::duckdb_v2_value_create_ubigint, factory.raw(), value),
        ValueInput::Float(value) => create_value!(ffi::duckdb_v2_value_create_float, factory.raw(), value),
        ValueInput::Double(value) => create_value!(ffi::duckdb_v2_value_create_double, factory.raw(), value),
        ValueInput::HugeInt(value) => {
            create_value!(ffi::duckdb_v2_value_create_hugeint, factory.raw(), &hugeint(value))
        }
        ValueInput::UHugeInt(value) => {
            create_value!(ffi::duckdb_v2_value_create_uhugeint, factory.raw(), &uhugeint(value))
        }
        ValueInput::Varchar(value) => create_value!(ffi::duckdb_v2_value_create_varchar, factory.raw(), &value.into()),
        ValueInput::Blob(value) => create_value!(
            ffi::duckdb_v2_value_create_blob,
            factory.raw(),
            &crate::ffi_str::bytes_arg(value)
        ),
        ValueInput::Type(logical_type) => {
            create_value!(ffi::duckdb_v2_value_create_type, factory.raw(), logical_type.handle)
        }
        ValueInput::Date(value) => create_value!(ffi::duckdb_v2_value_create_date, factory.raw(), value),
        ValueInput::Time(value) => create_value!(ffi::duckdb_v2_value_create_time, factory.raw(), value),
        ValueInput::TimeNs(value) => create_value!(ffi::duckdb_v2_value_create_time_ns, factory.raw(), value),
        ValueInput::TimeTz(value) => create_value!(ffi::duckdb_v2_value_create_time_tz, factory.raw(), value),
        ValueInput::Timestamp(value) => create_value!(ffi::duckdb_v2_value_create_timestamp, factory.raw(), value),
        ValueInput::TimestampSec(value) => {
            create_value!(ffi::duckdb_v2_value_create_timestamp_sec, factory.raw(), value)
        }
        ValueInput::TimestampMs(value) => create_value!(ffi::duckdb_v2_value_create_timestamp_ms, factory.raw(), value),
        ValueInput::TimestampNs(value) => create_value!(ffi::duckdb_v2_value_create_timestamp_ns, factory.raw(), value),
        ValueInput::TimestampTz(value) => create_value!(ffi::duckdb_v2_value_create_timestamp_tz, factory.raw(), value),
        ValueInput::TimestampTzNs(value) => {
            create_value!(ffi::duckdb_v2_value_create_timestamp_tz_ns, factory.raw(), value)
        }
        ValueInput::Interval { months, days, micros } => {
            create_value!(
                ffi::duckdb_v2_value_create_interval,
                factory.raw(),
                &ffi::duckdb_v2_interval_t { months, days, micros }
            )
        }
        ValueInput::Decimal { value, width, scale } => {
            create_value!(
                ffi::duckdb_v2_value_create_decimal,
                factory.raw(),
                &hugeint(value),
                width,
                scale
            )
        }
        ValueInput::Uuid(value) => create_value!(ffi::duckdb_v2_value_create_uuid, factory.raw(), &hugeint(value)),
        ValueInput::Bit(value) => create_value!(
            ffi::duckdb_v2_value_create_bit,
            factory.raw(),
            &crate::ffi_str::bytes_arg(value)
        ),
        ValueInput::BigNum(value) => create_value!(
            ffi::duckdb_v2_value_create_bignum,
            factory.raw(),
            &crate::ffi_str::bytes_arg(value)
        ),
        ValueInput::List { child_type, children } => {
            let children = value_handles(children);
            create_value!(
                ffi::duckdb_v2_value_create_list,
                factory.raw(),
                child_type.handle,
                children.as_ptr(),
                children.len() as u64
            )
        }
        ValueInput::Array { child_type, children } => {
            let children = value_handles(children);
            create_value!(
                ffi::duckdb_v2_value_create_array,
                factory.raw(),
                child_type.handle,
                children.as_ptr(),
                children.len() as u64
            )
        }
        ValueInput::Struct { names, children } => {
            check_same_len("STRUCT names and children", names.len(), children.len())?;
            let names = names.iter().map(|name| (*name).into()).collect::<Vec<_>>();
            let children = value_handles(children);
            create_value!(
                ffi::duckdb_v2_value_create_struct,
                factory.raw(),
                names.as_ptr(),
                children.as_ptr(),
                children.len() as u64
            )
        }
        ValueInput::Tuple(children) => {
            let children = value_handles(children);
            create_value!(
                ffi::duckdb_v2_value_create_tuple,
                factory.raw(),
                children.as_ptr(),
                children.len() as u64
            )
        }
        ValueInput::Map {
            key_type,
            value_type,
            keys,
            values,
        } => {
            check_same_len("MAP keys and values", keys.len(), values.len())?;
            let keys = value_handles(keys);
            let values = value_handles(values);
            create_value!(
                ffi::duckdb_v2_value_create_map,
                factory.raw(),
                key_type.handle,
                value_type.handle,
                keys.as_ptr(),
                values.as_ptr(),
                keys.len() as u64
            )
        }
    }
}

impl Value {
    /// Read this value as a Rust type, returning `None` for SQL `NULL`.
    pub fn get<T: FromValue>(&self) -> Result<Option<T>> {
        T::from_value(self)
    }

    /// Create a typed `NULL` of `logical_type`.
    pub fn null<C: FFILink + ?Sized>(link: &C, logical_type: &LogicalType) -> Result<Value> {
        link.create_value(ValueInput::Null(logical_type))
    }

    /// Cast a value using a connection or callback context's registered casts.
    pub fn cast<C: FFILink + ?Sized>(&self, link: &C, target_type: &LogicalType) -> Result<Value> {
        check_api_call!(
            ffi::duckdb_v2_value_cast,
            link.factory()?.raw(),
            self.raw(),
            target_type.raw(),
            RET
        )
        .map(|handle| Value { handle })
    }

    /// Create a `TYPE` value carrying a logical type.
    pub fn from_logical_type<C: FFILink + ?Sized>(link: &C, logical_type: &LogicalType) -> Result<Value> {
        link.create_value(ValueInput::Type(logical_type))
    }

    /// Return this value's logical type.
    pub fn logical_type(&self) -> Result<LogicalType> {
        check_api_call!(ffi::duckdb_v2_value_get_logical_type, self.handle, RET).map(|handle| LogicalType { handle })
    }

    /// Return the logical type carried by a `TYPE` value.
    pub(crate) fn type_value_get_inner(&self) -> Result<LogicalType> {
        check_api_call!(ffi::duckdb_v2_value_get_type, self.handle, RET).map(|handle| LogicalType { handle })
    }

    /// Count elements/fields: MAP has two per entry, UNION has tag/member, and NULL or non-composites have zero.
    /// LIST/ARRAY children are elements; STRUCT/TUPLE children are fields.
    pub fn child_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_value_get_child_count, self.handle, RET).map(|count| count as usize)
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
        if written > length {
            return Err(Error::api_error("Invalid bignum length"));
        }
        // SAFETY: the C API initialized `written` bytes within the allocated capacity.
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
        if written > length {
            return Err(Error::api_error("Invalid bignum length"));
        }
        // SAFETY: the C API initialized `written` bytes within the allocated capacity.
        unsafe {
            decoded.set_len(written as usize);
        }
        Ok((is_negative, decoded))
    }

    /// Copy a child: MAP alternates key/value, UNION has tag then active member; invalid indices return errors.
    /// This slow path copies elements/fields; prefer typed vector access for query data.
    pub fn get_child(&self, index: usize) -> Result<Value> {
        check_api_call!(ffi::duckdb_v2_value_get_child, self.handle, index as u64, RET).map(|handle| Value { handle })
    }

    /// Copy all children in [`Self::get_child`] order.
    /// This is a slow per-value path; prefer typed vector access for query data.
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
        check_api_call!(ffi::duckdb_v2_value_is_null, self.handle, RET)
    }

    /// Render the value for diagnostics.
    pub fn dbg_string(&self) -> Result<String> {
        check_api_call_string!(ffi::duckdb_v2_value_to_string, self.handle)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.dbg_string().map_err(|_| std::fmt::Error)?)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::{
        Parameters, ToValue,
        builder_helpers::scalar_callback,
        connection::FFILink,
        environment::{Environment, StorageLocation},
        error::DuckDBError,
        scalar::ScalarFunctionBuilder,
        signature::{FunctionParameter, SignatureBuilder},
        types::{BigNumValue, DuckDBType},
        value::ValueInput,
    };

    fn assert_length_mismatches_rejected<C: FFILink + ?Sized>(link: &C) -> crate::Result<()> {
        let int = i32::logical_type(link)?;
        let two = [1_i32.value(link)?, 2_i32.value(link)?];
        let one = [3_i32.value(link)?];

        let result = link.create_value(ValueInput::Struct {
            names: &["a"],
            children: &two,
        });
        let err = result.expect_err("STRUCT length mismatch must be rejected");
        assert_eq!(err.code, crate::error::DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
        assert_eq!(err.message, "STRUCT names and children: lengths differ (1 vs 2)");

        let result = link.create_value(ValueInput::Map {
            key_type: &int,
            value_type: &int,
            keys: &two,
            values: &one,
        });
        let err = result.expect_err("MAP length mismatch must be rejected");
        assert_eq!(err.code, crate::error::DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
        assert_eq!(err.message, "MAP keys and values: lengths differ (2 vs 1)");
        Ok(())
    }

    scalar_callback!(LengthCheckScalar, i32, |_input, output, ctx, _user_data| {
        assert_length_mismatches_rejected(ctx)?;
        let mut output = output;
        output.set_size(1)?;
        output.writer()?.set(0, Some(1))
    });

    #[test]
    fn test_nested_value_length_mismatch() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        assert_length_mismatches_rejected(&conn)?;

        // The same check through a callback context.
        ScalarFunctionBuilder::new(
            "length_check",
            SignatureBuilder::new()
                .param(FunctionParameter::new::<i32>("x"))
                .return_type(i32::logical_type(&conn)?),
            LengthCheckScalar,
        )
        .register(&conn)?;
        conn.execute("SELECT length_check(1)", Parameters::None)?;

        Ok(())
    }

    #[test]
    fn test_typed_null_list() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let logical_type = Vec::<i32>::logical_type(&conn)?;
        let value = super::Value::null(&conn, &logical_type)?;
        assert!(value.is_null()?);
        assert_eq!(value.child_count()?, 0);
        assert_eq!(value.logical_type()?.to_text()?, "INTEGER[]");
        assert!(value.get::<Vec<Option<i32>>>()?.is_none());
        Ok(())
    }

    #[test]
    fn test_hugeint_parts_round_trip() {
        for value in [
            i128::MIN,
            -(1_i128 << 64) + (1_i128 << 63),
            -1,
            0,
            1_i128 << 63,
            i128::MAX,
        ] {
            assert_eq!(super::from_hugeint(super::hugeint(value)), value);
        }
    }

    #[test]
    fn test_value_error_paths() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let list = vec![1_i32, 2].value(&conn)?;
        for index in [2, usize::MAX] {
            let err = list.get_child(index).expect_err("out-of-range child must fail");
            assert_eq!(err.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
            assert!(err.message.contains("child index out of range"), "{err}");
        }
        let scalar = 42_i32.value(&conn)?;
        let err = scalar.get_child(0).expect_err("scalar has no children");
        assert_eq!(err.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
        let err = scalar.get::<super::LogicalType>().expect_err("integer is not TYPE");
        assert_eq!(err.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
        assert!(err.message.contains("TYPE"), "{err}");

        let text = "not an integer".value(&conn)?;
        let target = i32::logical_type(&conn)?;
        let err = text.cast(&conn, &target).expect_err("invalid integer cast must fail");
        assert_eq!(err.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID);
        assert!(err.message.contains("Failed to cast value"), "{err}");
        assert!(err.message.contains("not an integer"), "{err}");
        assert_eq!(text.get::<String>()?.as_deref(), Some("not an integer"));
        assert_eq!(scalar.get::<i32>()?, Some(42));
        Ok(())
    }

    #[test]
    fn test_value_display_and_type_payload() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        assert_eq!(42_i32.value(&conn)?.to_string(), "42");
        assert_eq!("hello".value(&conn)?.to_string(), "hello");
        let logical_type = i32::logical_type(&conn)?;
        assert_eq!(super::Value::null(&conn, &logical_type)?.to_string(), "NULL");
        let value = super::Value::from_logical_type(&conn, &logical_type)?;
        let payload = value.get::<super::LogicalType>()?.expect("non-NULL TYPE payload");
        assert_eq!(payload.id()?, logical_type.id()?);
        Ok(())
    }

    #[test]
    fn test_cast_borrows_target_type() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;
        let target = i64::logical_type(&conn)?;
        let value = 42_i32.value(&conn)?;

        assert_eq!(value.cast(&conn, &target)?.get::<i64>()?, Some(42));
        assert_eq!(conn.value_cast(&value, &target)?.get::<i64>()?, Some(42));
        assert_eq!(target.to_text()?, "BIGINT");
        let null = super::Value::null(&conn, &target)?;
        assert!(null.is_null()?);
        assert_eq!(null.logical_type()?.to_text()?, "BIGINT");
        Ok(())
    }

    #[test]
    fn test_bignum_encode_decode_round_trip() -> crate::Result<()> {
        for magnitude in [vec![0], vec![1], vec![1, 2, 3, 4], vec![255; 32]] {
            for is_negative in [false, true] {
                if is_negative && magnitude == [0] {
                    continue;
                }
                let encoded = super::Value::encode_bignum(&magnitude, is_negative)?;
                let (decoded_sign, decoded) = super::Value::decode_bignum(&encoded)?;
                assert_eq!(decoded_sign, is_negative);
                assert_eq!(decoded, magnitude);
            }
        }
        Ok(())
    }

    #[test]
    fn test_bignum_list_render() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;
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
        assert_eq!(value.to_string(), "[NULL, -16909060, 84281096]");
        Ok(())
    }
}
