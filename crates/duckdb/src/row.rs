use std::{convert, sync::Arc};

use super::{Error, Result, Statement};
use crate::core::LogicalTypeId;
use crate::types::{self, Decimal, EnumType, FromSql, FromSqlError, ListType, ValueRef};

use arrow::{
    array::{
        self, Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, ListArray, MapArray,
        StructArray,
    },
    datatypes::*,
};
use fallible_iterator::FallibleIterator;
use fallible_streaming_iterator::FallibleStreamingIterator;

/// An handle for the resulting rows of a query.
#[must_use = "Rows is lazy and will do nothing unless consumed"]
pub struct Rows<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
    arr: Arc<Option<StructArray>>,
    row: Option<Row<'stmt>>,
    current_row: usize,
    current_batch_row: usize,
}

impl<'stmt> Rows<'stmt> {
    #[inline]
    fn reset(&mut self) {
        self.current_row = 0;
        self.current_batch_row = 0;
        self.arr = Arc::new(None);
    }

    /// Attempt to get the next row from the query. Returns `Ok(Some(Row))` if
    /// there is another row, `Err(...)` if there was an error
    /// getting the next row, and `Ok(None)` if all rows have been retrieved.
    ///
    /// ## Note
    ///
    /// This interface is not compatible with Rust's `Iterator` trait, because
    /// the lifetime of the returned row is tied to the lifetime of `self`.
    /// This is a fallible "streaming iterator". For a more natural interface,
    /// consider using [`query_map`](crate::Statement::query_map) or
    /// [`query_and_then`](crate::Statement::query_and_then) instead, which
    /// return types that implement `Iterator`.
    #[allow(clippy::should_implement_trait)] // cannot implement Iterator
    #[inline]
    pub fn next(&mut self) -> Result<Option<&Row<'stmt>>> {
        self.advance()?;
        Ok((*self).get())
    }

    #[inline]
    fn batch_row_count(&self) -> usize {
        if self.arr.is_none() {
            return 0;
        }
        self.arr.as_ref().as_ref().unwrap().len()
    }

    /// Map over this `Rows`, converting it to a [`Map`], which
    /// implements `FallibleIterator`.
    ///
    /// **Note:** This method requires the closure to return `duckdb::Result<B>`.
    /// If you need to use custom error types, consider using [`and_then`](Self::and_then)
    /// instead, which allows any error type that implements `From<duckdb::Error>`.
    ///
    /// ```rust,no_run
    /// use fallible_iterator::FallibleIterator;
    /// # use duckdb::{Result, Statement};
    /// fn query(stmt: &mut Statement) -> Result<Vec<i64>> {
    ///     let rows = stmt.query([])?;
    ///     rows.map(|r| r.get(0)).collect()
    /// }
    /// ```
    // FIXME Hide FallibleStreamingIterator::map
    #[inline]
    pub fn map<F, B>(self, f: F) -> Map<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<B>,
    {
        Map { rows: self, f }
    }

    /// Map over this `Rows`, converting it to a [`MappedRows`], which
    /// implements `Iterator`.
    #[inline]
    pub fn mapped<F, B>(self, f: F) -> MappedRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<B>,
    {
        MappedRows { rows: self, map: f }
    }

    /// Map over this `Rows` with a fallible function, converting it to a
    /// [`AndThenRows`], which implements `Iterator` (instead of
    /// `FallibleStreamingIterator`).
    #[inline]
    pub fn and_then<F, T, E>(self, f: F) -> AndThenRows<'stmt, F>
    where
        F: FnMut(&Row<'_>) -> Result<T, E>,
    {
        AndThenRows { rows: self, map: f }
    }

    /// Access the underlying statement
    ///
    /// This method provides a way to access the `Statement` that created these `Rows`
    /// without additional borrowing conflicts. This is particularly useful when you need
    /// to access statement metadata (like column count or names) while iterating over results.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use duckdb::{Connection, Result};
    /// fn process_results(conn: &Connection) -> Result<()> {
    ///     let mut stmt = conn.prepare("SELECT id, name FROM people")?;
    ///     let mut rows = stmt.query([])?;
    ///
    ///     let column_count = rows.as_ref().unwrap().column_count();
    ///     println!("Processing {} columns", column_count);
    ///
    ///     while let Some(row) = rows.next()? {
    ///         // Process row...
    ///     }
    ///     Ok(())
    /// }
    /// ```
    pub fn as_ref(&self) -> Option<&Statement<'stmt>> {
        self.stmt
    }
}

impl<'stmt> Rows<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Self {
        Rows {
            stmt: Some(stmt),
            arr: Arc::new(None),
            row: None,
            current_row: 0,
            current_batch_row: 0,
        }
    }

    #[inline]
    pub(crate) fn get_expected_row(&mut self) -> Result<&Row<'stmt>> {
        match self.next()? {
            Some(row) => Ok(row),
            None => Err(Error::QueryReturnedNoRows),
        }
    }
}

/// `F` is used to transform the _streaming_ iterator into a _fallible_
/// iterator.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Map<'stmt, F> {
    rows: Rows<'stmt>,
    f: F,
}

impl<F, B> FallibleIterator for Map<'_, F>
where
    F: FnMut(&Row<'_>) -> Result<B>,
{
    type Error = Error;
    type Item = B;

    #[inline]
    fn next(&mut self) -> Result<Option<B>> {
        match self.rows.next()? {
            Some(v) => Ok(Some((self.f)(v)?)),
            None => Ok(None),
        }
    }
}

/// An iterator over the mapped resulting rows of a query.
///
/// `F` is used to transform the _streaming_ iterator into a _standard_
/// iterator.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct MappedRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<T, F> Iterator for MappedRows<'_, F>
where
    F: FnMut(&Row<'_>) -> Result<T>,
{
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Result<T>> {
        let map = &mut self.map;
        self.rows.next().transpose().map(|row_result| row_result.and_then(map))
    }
}

/// An iterator over the mapped resulting rows of a query, with an Error type
/// unifying with Error.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct AndThenRows<'stmt, F> {
    rows: Rows<'stmt>,
    map: F,
}

impl<T, E, F> Iterator for AndThenRows<'_, F>
where
    E: convert::From<Error>,
    F: FnMut(&Row<'_>) -> Result<T, E>,
{
    type Item = Result<T, E>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let map = &mut self.map;
        self.rows
            .next()
            .transpose()
            .map(|row_result| row_result.map_err(E::from).and_then(map))
    }
}

/// `FallibleStreamingIterator` differs from the standard library's `Iterator`
/// in two ways:
/// * each call to `next` (sqlite3_step) can fail.
/// * returned `Row` is valid until `next` is called again or `Statement` is
///   reset or finalized.
///
/// While these iterators cannot be used with Rust `for` loops, `while let`
/// loops offer a similar level of ergonomics:
/// ```rust,no_run
/// # use duckdb::{Result, Statement};
/// fn query(stmt: &mut Statement) -> Result<()> {
///     let mut rows = stmt.query([])?;
///     while let Some(row) = rows.next()? {
///         // scan columns value
///     }
///     Ok(())
/// }
/// ```
impl<'stmt> FallibleStreamingIterator for Rows<'stmt> {
    type Error = Error;
    type Item = Row<'stmt>;

    #[inline]
    fn advance(&mut self) -> Result<()> {
        match self.stmt {
            Some(stmt) => {
                if self.current_row < stmt.row_count() {
                    if self.current_batch_row >= self.batch_row_count() {
                        self.arr = Arc::new(stmt.step());
                        if self.arr.is_none() {
                            self.row = None;
                            return Ok(());
                        }
                        self.current_batch_row = 0;
                    }
                    self.row = Some(Row {
                        stmt,
                        arr: self.arr.clone(),
                        current_row: self.current_batch_row,
                    });
                    self.current_row += 1;
                    self.current_batch_row += 1;
                    Ok(())
                } else {
                    self.reset();
                    self.row = None;
                    Ok(())
                }
            }
            None => {
                self.row = None;
                Ok(())
            }
        }
    }

    #[inline]
    fn get(&self) -> Option<&Row<'stmt>> {
        self.row.as_ref()
    }
}

/// A single result row of a query.
pub struct Row<'stmt> {
    pub(crate) stmt: &'stmt Statement<'stmt>,
    arr: Arc<Option<StructArray>>,
    current_row: usize,
}

#[allow(clippy::needless_lifetimes)]
impl<'stmt> Row<'stmt> {
    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Panics if calling [`row.get(idx)`](Row::get) would return an error,
    /// including:
    ///
    /// * If the underlying DuckDB column type is not a valid type as a source
    ///   for `T`
    /// * If the underlying DuckDB integral value is outside the range
    ///   representable by `T`
    /// * If `idx` is outside the range of columns in the returned query
    pub fn get_unwrap<I: RowIndex, T: FromSql>(&self, idx: I) -> T {
        self.get(idx).unwrap()
    }

    /// Get the value of a particular column of the result row.
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnType` if the underlying DuckDB column
    /// type is not a valid type as a source for `T`.
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// Returns an `Error::InvalidColumnName` if `idx` is not a valid column
    /// name for this row.
    ///
    /// If the result type is i128 (which requires the `i128_blob` feature to be
    /// enabled), and the underlying DuckDB column is a blob whose size is not
    /// 16 bytes, `Error::InvalidColumnType` will also be returned.
    pub fn get<I: RowIndex, T: FromSql>(&self, idx: I) -> Result<T> {
        let idx = idx.idx(self.stmt)?;
        let value = self.value_ref(self.current_row, idx);
        FromSql::column_result(value).map_err(|err| match err {
            FromSqlError::InvalidType => {
                Error::InvalidColumnType(idx, self.stmt.column_name_unwrap(idx).into(), value.data_type())
            }
            FromSqlError::OutOfRange(i) => Error::IntegralValueOutOfRange(idx, i),
            FromSqlError::OutOfRangeUnsigned(i) => Error::UnsignedIntegralValueOutOfRange(idx, i),
            FromSqlError::Other(err) => Error::FromSqlConversionFailure(idx, value.data_type(), err),
            #[cfg(feature = "uuid")]
            FromSqlError::InvalidUuidSize(_) => {
                Error::InvalidColumnType(idx, self.stmt.column_name_unwrap(idx).into(), value.data_type())
            }
        })
    }

    /// Get the value of a particular column of the result row as a `ValueRef`,
    /// allowing data to be read out of a row without copying.
    ///
    /// This `ValueRef` is valid only as long as this Row, which is enforced by
    /// it's lifetime. This means that while this method is completely safe,
    /// it can be somewhat difficult to use, and most callers will be better
    /// served by [`get`](Row::get) or [`get_unwrap`](Row::get_unwrap).
    ///
    /// ## Failure
    ///
    /// Returns an `Error::InvalidColumnIndex` if `idx` is outside the valid
    /// column range for this row.
    ///
    /// Returns an `Error::InvalidColumnName` if `idx` is not a valid column
    /// name for this row.
    pub fn get_ref<I: RowIndex>(&self, idx: I) -> Result<ValueRef<'_>> {
        let idx = idx.idx(self.stmt)?;
        // Narrowing from `ValueRef<'stmt>` (which `self.value_ref(row, idx)`
        // returns) to `ValueRef<'a>` is needed because it is only valid until
        // the next row fetch.
        let val_ref = self.value_ref(self.current_row, idx);
        Ok(val_ref)
    }

    fn value_ref(&self, row: usize, col: usize) -> ValueRef<'_> {
        let column = self.arr.as_ref().as_ref().unwrap().column(col);
        let value = Self::value_ref_internal(row, column);

        // Arrow gives HUGEINT, UHUGEINT, and DECIMAL(38,0) the same
        // Decimal128(38,0) physical shape. value_ref_internal keeps that
        // ambiguous shape in the signed HugeInt carrier. Top-level result
        // metadata recovers UHUGEINT and DECIMAL; otherwise preserve the
        // existing HugeInt fallback for parameter-derived or metadata-less
        // results.
        if let ValueRef::HugeInt(value) = value {
            match self.stmt.result_column_logical_id(col) {
                Some(LogicalTypeId::UHugeint) => return ValueRef::UHugeInt(value as u128),
                Some(LogicalTypeId::Decimal) => {
                    if let DataType::Decimal128(width, 0) = column.data_type() {
                        return ValueRef::Decimal(Decimal::from_chunk(*width, 0, value));
                    }
                }
                _ => {}
            }
        }

        value
    }

    pub(crate) fn value_ref_internal<'a>(row: usize, column: &'a ArrayRef) -> ValueRef<'a> {
        if column.is_null(row) {
            return ValueRef::Null;
        }
        // duckdb.cpp SetArrowFormat
        // https://github.com/duckdb/duckdb/blob/71f1c7a7e4b8737cff5e78d1f090c54f5e78e17b/src/main/query_result.cpp#L148
        match column.data_type() {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<array::StringArray>().unwrap();
                ValueRef::from(array.value(row))
            }
            DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<array::LargeStringArray>().unwrap();
                ValueRef::from(array.value(row))
            }
            DataType::Binary => {
                let array = column.as_any().downcast_ref::<array::BinaryArray>().unwrap();
                ValueRef::Blob(array.value(row))
            }
            DataType::LargeBinary => {
                let array = column.as_any().downcast_ref::<array::LargeBinaryArray>().unwrap();
                ValueRef::Blob(array.value(row))
            }
            DataType::FixedSizeBinary(_) => {
                let array = column.as_any().downcast_ref::<FixedSizeBinaryArray>().unwrap();
                ValueRef::Blob(array.value(row))
            }
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<array::BooleanArray>().unwrap();
                ValueRef::Boolean(array.value(row))
            }
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<array::Int8Array>().unwrap();
                ValueRef::TinyInt(array.value(row))
            }
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<array::Int16Array>().unwrap();
                ValueRef::SmallInt(array.value(row))
            }
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<array::Int32Array>().unwrap();
                ValueRef::Int(array.value(row))
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<array::Int64Array>().unwrap();
                ValueRef::BigInt(array.value(row))
            }
            DataType::UInt8 => {
                let array = column.as_any().downcast_ref::<array::UInt8Array>().unwrap();
                ValueRef::UTinyInt(array.value(row))
            }
            DataType::UInt16 => {
                let array = column.as_any().downcast_ref::<array::UInt16Array>().unwrap();
                ValueRef::USmallInt(array.value(row))
            }
            DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<array::UInt32Array>().unwrap();
                ValueRef::UInt(array.value(row))
            }
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<array::UInt64Array>().unwrap();
                ValueRef::UBigInt(array.value(row))
            }
            DataType::Float16 => {
                let array = column.as_any().downcast_ref::<array::Float32Array>().unwrap();
                ValueRef::Float(array.value(row))
            }
            DataType::Float32 => {
                let array = column.as_any().downcast_ref::<array::Float32Array>().unwrap();
                ValueRef::Float(array.value(row))
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<array::Float64Array>().unwrap();
                ValueRef::Double(array.value(row))
            }
            DataType::Decimal128(width, scale) => {
                let array = column.as_any().downcast_ref::<array::Decimal128Array>().unwrap();
                let value = array.value(row);
                let scale = u8::try_from(*scale).expect("Arrow decimal scale must be non-negative");
                if scale == 0 && *width == Decimal::MAX_WIDTH {
                    ValueRef::HugeInt(value)
                } else {
                    ValueRef::Decimal(Decimal::from_chunk(*width, scale, value))
                }
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
                let array = column.as_any().downcast_ref::<array::TimestampSecondArray>().unwrap();
                ValueRef::Timestamp(types::TimeUnit::Second, array.value(row))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<array::TimestampMillisecondArray>()
                    .unwrap();
                ValueRef::Timestamp(types::TimeUnit::Millisecond, array.value(row))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<array::TimestampMicrosecondArray>()
                    .unwrap();
                ValueRef::Timestamp(types::TimeUnit::Microsecond, array.value(row))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<array::TimestampNanosecondArray>()
                    .unwrap();
                ValueRef::Timestamp(types::TimeUnit::Nanosecond, array.value(row))
            }
            DataType::Date32 => {
                let array = column.as_any().downcast_ref::<array::Date32Array>().unwrap();
                ValueRef::Date32(array.value(row))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let array = column.as_any().downcast_ref::<array::Time64MicrosecondArray>().unwrap();
                ValueRef::Time64(types::TimeUnit::Microsecond, array.value(row))
            }
            DataType::Interval(unit) => match unit {
                IntervalUnit::MonthDayNano => {
                    let array = column
                        .as_any()
                        .downcast_ref::<array::IntervalMonthDayNanoArray>()
                        .unwrap();
                    let value = array.value(row);

                    ValueRef::Interval {
                        months: value.months,
                        days: value.days,
                        nanos: value.nanoseconds,
                    }
                }
                _ => unimplemented!("{:?}", unit),
            },
            // TODO: support more data types
            // NOTE: DataTypes not supported by duckdb
            // DataType::Date64 => make_string_date!(array::Date64Array, column, row),
            // DataType::Time32(unit) if *unit == TimeUnit::Second => {
            //     make_string_time!(array::Time32SecondArray, column, row)
            // }
            // DataType::Time32(unit) if *unit == TimeUnit::Millisecond => {
            //     make_string_time!(array::Time32MillisecondArray, column, row)
            // }
            // DataType::Time64(unit) if *unit == TimeUnit::Nanosecond => {
            //     make_string_time!(array::Time64NanosecondArray, column, row)
            // }
            DataType::LargeList(..) => {
                let arr = column.as_any().downcast_ref::<array::LargeListArray>().unwrap();

                ValueRef::List(ListType::Large(arr), row)
            }
            DataType::List(..) => {
                let arr = column.as_any().downcast_ref::<ListArray>().unwrap();

                ValueRef::List(ListType::Regular(arr), row)
            }
            DataType::Dictionary(key_type, ..) => {
                let column = column.as_any();
                ValueRef::Enum(
                    match key_type.as_ref() {
                        DataType::UInt8 => {
                            EnumType::UInt8(column.downcast_ref::<DictionaryArray<UInt8Type>>().unwrap())
                        }
                        DataType::UInt16 => {
                            EnumType::UInt16(column.downcast_ref::<DictionaryArray<UInt16Type>>().unwrap())
                        }
                        DataType::UInt32 => {
                            EnumType::UInt32(column.downcast_ref::<DictionaryArray<UInt32Type>>().unwrap())
                        }
                        typ => panic!("Unsupported key type: {typ:?}"),
                    },
                    row,
                )
            }
            DataType::Struct(_) => {
                let res = column.as_any().downcast_ref::<StructArray>().unwrap();
                ValueRef::Struct(res, row)
            }
            DataType::Map(..) => {
                let arr = column.as_any().downcast_ref::<MapArray>().unwrap();
                ValueRef::Map(arr, row)
            }
            DataType::FixedSizeList(..) => {
                let arr = column.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                ValueRef::Array(arr, row)
            }
            DataType::Union(..) => ValueRef::Union(column, row),
            _ => unreachable!("invalid value: {}", column.data_type()),
        }
    }

    /// Get the value of a particular column of the result row as a `ValueRef`,
    /// allowing data to be read out of a row without copying.
    ///
    /// This `ValueRef` is valid only as long as this Row, which is enforced by
    /// it's lifetime. This means that while this method is completely safe,
    /// it can be difficult to use, and most callers will be better served by
    /// [`get`](Row::get) or [`get_unwrap`](Row::get_unwrap).
    ///
    /// ## Failure
    ///
    /// Panics if calling [`row.get_ref(idx)`](Row::get_ref) would return an
    /// error, including:
    ///
    /// * If `idx` is outside the range of columns in the returned query.
    /// * If `idx` is not a valid column name for this row.
    pub fn get_ref_unwrap<I: RowIndex>(&self, idx: I) -> ValueRef<'_> {
        self.get_ref(idx).unwrap()
    }
}

impl<'stmt> AsRef<Statement<'stmt>> for Row<'stmt> {
    fn as_ref(&self) -> &Statement<'stmt> {
        self.stmt
    }
}

mod sealed {
    /// This trait exists just to ensure that the only impls of `trait Params`
    /// that are allowed are ones in this crate.
    pub trait Sealed {}
    impl Sealed for usize {}
    impl Sealed for &str {}
}

/// A trait implemented by types that can index into columns of a row.
///
/// It is only implemented for `usize` and `&str`.
pub trait RowIndex: sealed::Sealed {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement<'_>) -> Result<usize>;
}

impl RowIndex for usize {
    #[inline]
    fn idx(&self, stmt: &Statement<'_>) -> Result<usize> {
        if *self >= stmt.column_count() {
            Err(Error::InvalidColumnIndex(*self))
        } else {
            Ok(*self)
        }
    }
}

impl RowIndex for &'_ str {
    #[inline]
    fn idx(&self, stmt: &Statement<'_>) -> Result<usize> {
        stmt.column_index(self)
    }
}

macro_rules! tuple_try_from_row {
    () => {
        impl<'a> convert::TryFrom<&'a Row<'a>> for () {
            type Error = crate::Error;

            fn try_from(_: &'a Row<'a>) -> Result<Self> {
                Ok(())
            }
        }
    };
    ($($field:ident),+) => {
        impl<'a, $($field,)*> convert::TryFrom<&'a Row<'a>> for ($($field,)*) where $($field: FromSql,)* {
            type Error = crate::Error;

            fn try_from(row: &'a Row<'a>) -> Result<Self> {
                let mut index = 0;
                let values = (
                    $({
                        let value = row.get::<_, $field>(index)?;
                        index += 1;
                        value
                    },)*
                );
                // Read the final increment so rustc does not flag it as an unused assignment.
                let _ = index;
                Ok(values)
            }
        }
    }
}

macro_rules! tuples_try_from_row {
    () => {
        // not very useful, but maybe some other macro users will find this helpful
        tuple_try_from_row!();
    };
    ($first:ident $(, $remaining:ident)*) => {
        tuple_try_from_row!($first $(, $remaining)*);
        tuples_try_from_row!($($remaining),*);
    };
}

tuples_try_from_row!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);

#[cfg(test)]
mod tests {
    use crate::{
        Connection, Error, Result,
        types::{Decimal, Type, Value, ValueRef},
    };

    const U128_MAX_SQL: &str = "340282366920938463463374607431768211455";
    const I128_MAX_PLUS_ONE_SQL: &str = "170141183460469231731687303715884105728";
    const WIDE_DECIMAL_SQL: &str = "123456789012345678901234567890.12";
    const WIDE_DECIMAL_MANTISSA: i128 = 12_345_678_901_234_567_890_123_456_789_012;

    #[test]
    fn test_try_from_row_for_tuple_0() -> Result<()> {
        use std::convert::TryFrom;

        let conn = Connection::open_in_memory()?;
        conn.query_row("SELECT 1", [], |row| <()>::try_from(row))?;
        Ok(())
    }

    #[test]
    fn test_try_from_row_for_tuple_1() -> Result<()> {
        use crate::ToSql;
        use std::convert::TryFrom;

        let conn = Connection::open_in_memory()?;
        conn.execute(
            "CREATE TABLE test (a INTEGER)",
            crate::params_from_iter(std::iter::empty::<&dyn ToSql>()),
        )?;
        conn.execute("INSERT INTO test VALUES (42)", [])?;
        let val = conn.query_row("SELECT a FROM test", [], |row| <(u32,)>::try_from(row))?;
        assert_eq!(val, (42,));
        let fail = conn.query_row("SELECT a FROM test", [], |row| <(u32, u32)>::try_from(row));
        assert!(fail.is_err());
        Ok(())
    }

    #[test]
    fn test_try_from_row_for_tuple_2() -> Result<()> {
        use std::convert::TryFrom;

        let conn = Connection::open_in_memory()?;
        conn.execute("CREATE TABLE test (a INTEGER, b INTEGER)", [])?;
        conn.execute("INSERT INTO test VALUES (42, 47)", [])?;
        let val = conn.query_row("SELECT a, b FROM test", [], |row| <(u32, u32)>::try_from(row))?;
        assert_eq!(val, (42, 47));
        let fail = conn.query_row("SELECT a, b FROM test", [], |row| <(u32, u32, u32)>::try_from(row));
        assert!(fail.is_err());
        Ok(())
    }

    #[test]
    fn test_try_from_row_for_tuple_3() -> Result<()> {
        use std::convert::TryFrom;

        let conn = Connection::open_in_memory()?;
        let val = conn.query_row("SELECT 3, 5, 8", [], |row| <(u32, u32, u32)>::try_from(row))?;
        assert_eq!(val, (3, 5, 8));
        Ok(())
    }

    #[test]
    fn test_try_from_row_for_tuple_16() -> Result<()> {
        use std::convert::TryFrom;

        let create_table = "CREATE TABLE test (
            a INTEGER,
            b INTEGER,
            c INTEGER,
            d INTEGER,
            e INTEGER,
            f INTEGER,
            g INTEGER,
            h INTEGER,
            i INTEGER,
            j INTEGER,
            k INTEGER,
            l INTEGER,
            m INTEGER,
            n INTEGER,
            o INTEGER,
            p INTEGER
        )";

        let insert_values = "INSERT INTO test VALUES (
            0,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15
        )";

        type BigTuple = (
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
            u32,
        );

        let conn = Connection::open_in_memory()?;
        conn.execute(create_table, [])?;
        conn.execute(insert_values, [])?;
        let val = conn.query_row("SELECT * FROM test", [], |row| BigTuple::try_from(row))?;
        // Debug is not implemented for tuples of 16
        assert_eq!(val.0, 0);
        assert_eq!(val.1, 1);
        assert_eq!(val.2, 2);
        assert_eq!(val.3, 3);
        assert_eq!(val.4, 4);
        assert_eq!(val.5, 5);
        assert_eq!(val.6, 6);
        assert_eq!(val.7, 7);
        assert_eq!(val.8, 8);
        assert_eq!(val.9, 9);
        assert_eq!(val.10, 10);
        assert_eq!(val.11, 11);
        assert_eq!(val.12, 12);
        assert_eq!(val.13, 13);
        assert_eq!(val.14, 14);
        assert_eq!(val.15, 15);

        // We don't test one bigger because it's unimplemented
        Ok(())
    }

    #[test]
    fn uhugeint_arrives_as_uhugeint() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let mut stmt = db.prepare("SELECT (1)::UHUGEINT")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref(0)?, ValueRef::UHugeInt(1));
        assert_eq!(row.get_ref(0)?.data_type(), Type::UHugeInt);
        assert_eq!(row.get::<_, u128>(0)?, 1);

        let mut stmt = db.prepare(&format!("SELECT ({U128_MAX_SQL})::UHUGEINT"))?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref(0)?, ValueRef::UHugeInt(u128::MAX));
        assert_eq!(row.get_ref(0)?.data_type(), Type::UHugeInt);
        assert_eq!(row.get::<_, u128>(0)?, u128::MAX);

        let i128_max_plus_one = (i128::MAX as u128) + 1;
        let mut stmt = db.prepare(&format!("SELECT ({I128_MAX_PLUS_ONE_SQL})::UHUGEINT"))?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        assert_eq!(row.get_ref(0)?, ValueRef::UHugeInt(i128_max_plus_one));
        assert_eq!(row.get::<_, u128>(0)?, i128_max_plus_one);

        Ok(())
    }

    #[test]
    fn uhugeint_promotion_uses_matching_result_column() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let mut stmt = db.prepare(&format!(
            "
            SELECT
                1::HUGEINT AS a,
                2::UHUGEINT AS b,
                3::DECIMAL(38,0) AS c,
                ({U128_MAX_SQL})::UHUGEINT AS d
            "
        ))?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();

        assert_eq!(row.get_ref(0)?, ValueRef::HugeInt(1));
        assert_eq!(row.get_ref(1)?, ValueRef::UHugeInt(2));
        assert_eq!(row.get_ref(2)?, ValueRef::Decimal(Decimal::new(38, 0, 3)?));
        assert_eq!(row.get_ref(3)?, ValueRef::UHugeInt(u128::MAX));

        Ok(())
    }

    #[test]
    fn bound_uhugeint_result_uses_logical_metadata_when_cast() -> Result<()> {
        let db = Connection::open_in_memory()?;
        let max = u128::MAX;

        let value = db.query_row("SELECT ?::UHUGEINT", [&max], |row| {
            assert_eq!(row.get_ref(0)?, ValueRef::UHugeInt(max));
            row.get::<_, u128>(0)
        })?;
        assert_eq!(value, max);

        Ok(())
    }

    #[test]
    fn invalid_arrow_decimal_metadata_keeps_hugeint_fallback() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let byte = 255_i128;
        let byte_value = db.query_row("SELECT ?", [&byte], |row| {
            assert_eq!(row.get_ref(0)?, ValueRef::HugeInt(byte));
            row.get::<_, u8>(0)
        })?;
        assert_eq!(byte_value, 255);

        let value = 5_u128;
        let decimal = db.query_row("SELECT ? + 0::DECIMAL(38,0)", [&value], |row| {
            row.get_ref(0).map(|value| value.to_owned())
        })?;
        assert_eq!(decimal, Value::HugeInt(5));

        Ok(())
    }

    #[test]
    fn decimal_columns_use_duck_decimal_carrier() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let value = db.query_row("SELECT 1.23::DECIMAL(38,2)", [], |row| {
            row.get_ref(0).map(|value| value.to_owned())
        })?;
        assert_eq!(value, Value::Decimal(Decimal::new(38, 2, 123)?));

        let value = db.query_row("SELECT 3::DECIMAL(38,0)", [], |row| {
            row.get_ref(0).map(|value| value.to_owned())
        })?;
        match value {
            Value::Decimal(decimal) => {
                assert_eq!(decimal.width(), 38);
                assert_eq!(decimal, Decimal::new(38, 0, 3)?);
            }
            other => panic!("expected decimal, got {other:?}"),
        }

        let decimal = db.query_row(&format!("SELECT {WIDE_DECIMAL_SQL}::DECIMAL(38,2)"), [], |row| {
            assert_eq!(
                row.get_ref(0)?,
                ValueRef::Decimal(Decimal::new(38, 2, WIDE_DECIMAL_MANTISSA)?)
            );
            row.get::<_, Decimal>(0)
        })?;
        assert_eq!(decimal, Decimal::new(38, 2, WIDE_DECIMAL_MANTISSA)?);

        Ok(())
    }

    #[test]
    fn uhugeint_upper_half_is_out_of_range_for_i128() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let err = db
            .query_row(&format!("SELECT ({I128_MAX_PLUS_ONE_SQL})::UHUGEINT"), [], |row| {
                row.get::<_, i128>(0)
            })
            .unwrap_err();

        match err {
            Error::UnsignedIntegralValueOutOfRange(0, value) => {
                assert_eq!(value, (i128::MAX as u128) + 1)
            }
            other => panic!("expected unsigned out-of-range error, got {other:?}"),
        }

        let err = db
            .query_row(&format!("SELECT ({U128_MAX_SQL})::UHUGEINT"), [], |row| {
                row.get::<_, i128>(0)
            })
            .unwrap_err();

        match err {
            Error::UnsignedIntegralValueOutOfRange(0, value) => assert_eq!(value, u128::MAX),
            other => panic!("expected unsigned out-of-range error, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn nested_uhugeint_uses_metadata_less_hugeint_carrier() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let list = db.query_row(&format!("SELECT [({U128_MAX_SQL})::UHUGEINT]"), [], |row| {
            let value = row.get_ref(0)?;
            assert!(matches!(value.data_type(), Type::List(ref inner) if **inner == Type::Decimal));
            Ok(value.to_owned())
        })?;

        assert_eq!(list, Value::List(vec![Value::HugeInt(-1)]));

        let structure = db.query_row(
            &format!("SELECT struct_pack(v := ({U128_MAX_SQL})::UHUGEINT)"),
            [],
            |row| {
                let value = row.get_ref(0)?;
                assert!(matches!(value.data_type(), Type::Struct(_)));
                Ok(value.to_owned())
            },
        )?;

        match structure {
            Value::Struct(fields) => {
                assert_eq!(fields.get(&"v".to_string()), Some(&Value::HugeInt(-1)));
            }
            other => panic!("expected struct, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn nested_scale_zero_decimal_uses_decimal_when_width_is_unambiguous() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let list = db.query_row("SELECT [3::DECIMAL(5,0)]", [], |row| {
            let value = row.get_ref(0)?;
            assert!(matches!(value.data_type(), Type::List(ref inner) if **inner == Type::Decimal));
            Ok(value.to_owned())
        })?;

        assert_eq!(list, Value::List(vec![Value::Decimal(Decimal::new(5, 0, 3)?)]));

        let structure = db.query_row("SELECT struct_pack(a := 1.5::DECIMAL(5,1))", [], |row| {
            let value = row.get_ref(0)?;
            assert!(matches!(value.data_type(), Type::Struct(_)));
            Ok(value.to_owned())
        })?;

        match structure {
            Value::Struct(fields) => {
                assert_eq!(
                    fields.get(&"a".to_string()),
                    Some(&Value::Decimal(Decimal::new(5, 1, 15)?))
                );
            }
            other => panic!("expected struct, got {other:?}"),
        }

        Ok(())
    }

    #[test]
    fn nested_decimal_38_0_keeps_metadata_less_hugeint_fallback() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let list = db.query_row("SELECT [3::DECIMAL(38,0)]", [], |row| {
            let value = row.get_ref(0)?;
            assert!(matches!(value.data_type(), Type::List(ref inner) if **inner == Type::Decimal));
            Ok(value.to_owned())
        })?;

        assert_eq!(list, Value::List(vec![Value::HugeInt(3)]));

        Ok(())
    }

    #[test]
    fn uhugeint_logical_type_cache_survives_later_batches() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let values = db
            .prepare(
                "
                SELECT
                    CASE
                        WHEN i = 2500 THEN NULL::UHUGEINT
                        ELSE i::UHUGEINT
                    END AS v
                FROM range(3000) AS t(i)
                ORDER BY i
                ",
            )?
            .query_map([], |row| row.get_ref(0).map(|value| value.to_owned()))?
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(values.len(), 3000);
        assert_eq!(values[0], Value::UHugeInt(0));
        assert_eq!(values[2048], Value::UHugeInt(2048));
        assert_eq!(values[2500], Value::Null);
        assert_eq!(values[2999], Value::UHugeInt(2999));

        Ok(())
    }

    #[test]
    fn uhugeint_nulls_use_cached_logical_type() -> Result<()> {
        let db = Connection::open_in_memory()?;

        let values = db
            .prepare(&format!(
                "
                SELECT v FROM (
                    VALUES
                        (0, (0)::UHUGEINT),
                        (1, NULL::UHUGEINT),
                        (2, ({U128_MAX_SQL})::UHUGEINT)
                ) AS t(ord, v)
                ORDER BY ord
                "
            ))?
            .query_map([], |row| row.get_ref(0).map(|value| value.to_owned()))?
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            values,
            vec![Value::UHugeInt(0), Value::Null, Value::UHugeInt(u128::MAX)]
        );

        Ok(())
    }

    #[test]
    #[cfg(feature = "vtab-arrow")]
    fn test_fixed_size_binary_via_arrow() -> Result<()> {
        use crate::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
        use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<ArrowVTab>("arrow")?;

        // Create FixedSizeBinary(16) array - like UUID
        let values = vec![
            vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            vec![16u8, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
            vec![0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255],
        ];

        let byte_array = FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap();
        let arc: ArrayRef = Arc::new(byte_array);
        let schema = Schema::new(vec![Field::new("data", DataType::FixedSizeBinary(16), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arc]).unwrap();

        let mut stmt = conn.prepare("SELECT data FROM arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(arrow_recordbatch_to_query_params(batch))?;
        let rb = arr.next().expect("no record batch");

        // DuckDB converts FixedSizeBinary to regular Binary
        let column = rb.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(column.len(), 3);
        assert_eq!(
            column.value(0),
            &[1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
        );
        assert_eq!(
            column.value(1),
            &[16u8, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]
        );
        assert_eq!(column.value(2), &[0u8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 255]);

        Ok(())
    }

    #[test]
    #[cfg(feature = "vtab-arrow")]
    fn test_fixed_size_binary_with_nulls_via_arrow() -> Result<()> {
        use crate::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
        use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<ArrowVTab>("arrow")?;

        // Create FixedSizeBinary(8) array with nulls
        let values = vec![
            Some(vec![1u8, 2, 3, 4, 5, 6, 7, 8]),
            None,
            Some(vec![9u8, 10, 11, 12, 13, 14, 15, 16]),
        ];

        let byte_array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 8).unwrap();
        let arc: ArrayRef = Arc::new(byte_array);
        let schema = Schema::new(vec![Field::new("data", DataType::FixedSizeBinary(8), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arc]).unwrap();

        let mut stmt = conn.prepare("SELECT data FROM arrow(?, ?)")?;
        let mut arr = stmt.query_arrow(arrow_recordbatch_to_query_params(batch))?;
        let rb = arr.next().expect("no record batch");

        // NOTE: Currently, null handling for FixedSizeBinary is not fully implemented
        // (see vtab/arrow.rs fixed_size_binary_array_to_vector, line 925-926)
        // Nulls are converted to zero bytes instead of actual nulls
        let column = rb.column(0).as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(column.len(), 3);
        assert!(column.is_valid(0));
        // This should be false when null handling is implemented
        // assert!(!column.is_valid(1));
        assert!(column.is_valid(2));
        assert_eq!(column.value(0), &[1u8, 2, 3, 4, 5, 6, 7, 8]);
        // The null value is currently represented as zero bytes
        assert_eq!(column.value(1), &[0u8, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(column.value(2), &[9u8, 10, 11, 12, 13, 14, 15, 16]);

        Ok(())
    }

    #[test]
    #[cfg(feature = "vtab-arrow")]
    fn test_fixed_size_binary_different_sizes_via_arrow() -> Result<()> {
        use crate::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
        use arrow::array::{ArrayRef, FixedSizeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<ArrowVTab>("arrow")?;

        // Test with FixedSizeBinary(4)
        let values = vec![vec![1u8, 2, 3, 4], vec![5u8, 6, 7, 8]];

        let byte_array = FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap();
        let arc: ArrayRef = Arc::new(byte_array);
        let schema = Schema::new(vec![Field::new("data", DataType::FixedSizeBinary(4), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arc]).unwrap();

        let mut stmt = conn.prepare("SELECT data FROM arrow(?, ?)")?;
        let mut rows = stmt.query(arrow_recordbatch_to_query_params(batch))?;

        // Read via Row interface
        let row = rows.next()?.unwrap();
        let bytes: Vec<u8> = row.get(0)?;
        assert_eq!(bytes, vec![1u8, 2, 3, 4]);

        let row = rows.next()?.unwrap();
        let bytes: Vec<u8> = row.get(0)?;
        assert_eq!(bytes, vec![5u8, 6, 7, 8]);

        Ok(())
    }

    #[test]
    #[cfg(feature = "vtab-arrow")]
    fn test_fixed_size_binary_value_ref_via_arrow() -> Result<()> {
        use crate::types::ValueRef;
        use crate::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
        use arrow::array::{ArrayRef, FixedSizeBinaryArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let conn = Connection::open_in_memory()?;
        conn.register_table_function::<ArrowVTab>("arrow")?;

        let values = vec![Some(vec![1u8, 2, 3, 4]), None];

        let byte_array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(values.into_iter(), 4).unwrap();
        let arc: ArrayRef = Arc::new(byte_array);
        let schema = Schema::new(vec![Field::new("data", DataType::FixedSizeBinary(4), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arc]).unwrap();

        let mut stmt = conn.prepare("SELECT data FROM arrow(?, ?)")?;
        let mut rows = stmt.query(arrow_recordbatch_to_query_params(batch))?;

        // First row - non-null
        let row = rows.next()?.unwrap();
        let value_ref = row.get_ref(0)?;
        match value_ref {
            ValueRef::Blob(bytes) => {
                assert_eq!(bytes, &[1u8, 2, 3, 4]);
            }
            _ => panic!("Expected Blob ValueRef, got {:?}", value_ref),
        }

        // Second row - should be null, but currently null handling is not implemented
        // (see vtab/arrow.rs fixed_size_binary_array_to_vector, line 925-926)
        // so it's represented as zero bytes
        let row = rows.next()?.unwrap();
        let value_ref = row.get_ref(0)?;
        match value_ref {
            ValueRef::Blob(bytes) => {
                // This should be ValueRef::Null when null handling is implemented
                assert_eq!(bytes, &[0u8, 0, 0, 0]);
            }
            _ => panic!("Expected Blob ValueRef with zero bytes, got {:?}", value_ref),
        }

        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_fixed_size_binary_uuid() -> Result<()> {
        use uuid::Uuid;

        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE test (id UUID)")?;

        let uuid_str = "550e8400-e29b-41d4-a716-446655440000";
        conn.execute("INSERT INTO test VALUES (?)", [uuid_str])?;

        // Read back as UUID
        let uuid: Uuid = conn.query_row("SELECT id FROM test", [], |r| r.get(0))?;
        assert_eq!(uuid.to_string(), uuid_str);
        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_fixed_size_binary_uuid_roundtrip() -> Result<()> {
        use uuid::Uuid;

        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE test (id UUID)")?;

        let original_uuid = Uuid::new_v4();
        conn.execute("INSERT INTO test VALUES (?)", [original_uuid])?;

        let retrieved_uuid: Uuid = conn.query_row("SELECT id FROM test", [], |r| r.get(0))?;
        assert_eq!(original_uuid, retrieved_uuid);
        Ok(())
    }
}
