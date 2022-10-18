use std::convert;
use std::sync::Arc;

use super::{Error, Result, Statement};
use crate::types::{self, FromSql, FromSqlError, ValueRef};

use arrow::array::{self, Array, StructArray};
use arrow::datatypes::*;
use fallible_iterator::FallibleIterator;
use fallible_streaming_iterator::FallibleStreamingIterator;
use rust_decimal::prelude::*;

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

    /// Give access to the underlying statement
    pub fn as_ref(&self) -> Option<&Statement<'stmt>> {
        self.stmt
    }
}

impl<'stmt> Rows<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Rows<'stmt> {
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
            FromSqlError::Other(err) => Error::FromSqlConversionFailure(idx as usize, value.data_type(), err),
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
        // Narrowing from `ValueRef<'stmt>` (which `self.stmt.value_ref(idx)`
        // returns) to `ValueRef<'a>` is needed because it's only valid until
        // the next call to sqlite3_step.
        let val_ref = self.value_ref(self.current_row, idx);
        Ok(val_ref)
    }

    fn value_ref(&self, row: usize, col: usize) -> ValueRef<'_> {
        let column = self.arr.as_ref().as_ref().unwrap().column(col);
        if column.is_null(row) {
            return ValueRef::Null;
        }
        // duckdb.cpp SetArrowFormat
        // https://github.com/duckdb/duckdb/blob/71f1c7a7e4b8737cff5e78d1f090c54f5e78e17b/src/main/query_result.cpp#L148
        match column.data_type() {
            DataType::Utf8 => {
                let array = column.as_any().downcast_ref::<array::StringArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::from(array.value(row))
            }
            DataType::LargeUtf8 => {
                let array = column.as_any().downcast_ref::<array::LargeStringArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::from(array.value(row))
            }
            DataType::Binary => {
                let array = column.as_any().downcast_ref::<array::BinaryArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Blob(array.value(row))
            }
            DataType::LargeBinary => {
                let array = column.as_any().downcast_ref::<array::LargeBinaryArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Blob(array.value(row))
            }
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<array::BooleanArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Boolean(array.value(row))
            }
            DataType::Int8 => {
                let array = column.as_any().downcast_ref::<array::Int8Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::TinyInt(array.value(row))
            }
            DataType::Int16 => {
                let array = column.as_any().downcast_ref::<array::Int16Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::SmallInt(array.value(row))
            }
            DataType::Int32 => {
                let array = column.as_any().downcast_ref::<array::Int32Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Int(array.value(row))
            }
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<array::Int64Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::BigInt(array.value(row))
            }
            DataType::UInt8 => {
                let array = column.as_any().downcast_ref::<array::UInt8Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::UTinyInt(array.value(row))
            }
            DataType::UInt16 => {
                let array = column.as_any().downcast_ref::<array::UInt16Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::USmallInt(array.value(row))
            }
            DataType::UInt32 => {
                let array = column.as_any().downcast_ref::<array::UInt32Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::UInt(array.value(row))
            }
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<array::UInt64Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::UBigInt(array.value(row))
            }
            DataType::Float16 => {
                let array = column.as_any().downcast_ref::<array::Float32Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Float(array.value(row))
            }
            DataType::Float32 => {
                let array = column.as_any().downcast_ref::<array::Float32Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Float(array.value(row))
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<array::Float64Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Double(array.value(row))
            }
            DataType::Decimal128(..) => {
                let array = column.as_any().downcast_ref::<array::Decimal128Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                // hugeint: d:38,0
                if array.scale() == 0 {
                    return ValueRef::HugeInt(array.value(row));
                }
                ValueRef::Decimal(Decimal::from_i128_with_scale(array.value(row), array.scale() as u32))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Second => {
                let array = column.as_any().downcast_ref::<array::TimestampSecondArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Timestamp(types::TimeUnit::Second, array.value(row))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Millisecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<array::TimestampMillisecondArray>()
                    .unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Timestamp(types::TimeUnit::Millisecond, array.value(row))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Microsecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<array::TimestampMicrosecondArray>()
                    .unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Timestamp(types::TimeUnit::Microsecond, array.value(row))
            }
            DataType::Timestamp(unit, _) if *unit == TimeUnit::Nanosecond => {
                let array = column
                    .as_any()
                    .downcast_ref::<array::TimestampNanosecondArray>()
                    .unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Timestamp(types::TimeUnit::Nanosecond, array.value(row))
            }
            DataType::Date32 => {
                let array = column.as_any().downcast_ref::<array::Date32Array>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Date32(array.value(row))
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                let array = column.as_any().downcast_ref::<array::Time64MicrosecondArray>().unwrap();

                if array.is_null(row) {
                    return ValueRef::Null;
                }
                ValueRef::Time64(types::TimeUnit::Microsecond, array.value(row))
            }
            // TODO: support more data types
            // DataType::Interval(unit) => match unit {
            //     IntervalUnit::DayTime => {
            //         make_string_interval_day_time!(column, row)
            //     }
            //     IntervalUnit::YearMonth => {
            //         make_string_interval_year_month!(column, row)
            //     }
            // },
            // DataType::List(_) => make_string_from_list!(column, row),
            // DataType::Dictionary(index_type, _value_type) => match **index_type {
            //     DataType::Int8 => dict_array_value_to_string::<Int8Type>(column, row),
            //     DataType::Int16 => dict_array_value_to_string::<Int16Type>(column, row),
            //     DataType::Int32 => dict_array_value_to_string::<Int32Type>(column, row),
            //     DataType::Int64 => dict_array_value_to_string::<Int64Type>(column, row),
            //     DataType::UInt8 => dict_array_value_to_string::<UInt8Type>(column, row),
            //     DataType::UInt16 => dict_array_value_to_string::<UInt16Type>(column, row),
            //     DataType::UInt32 => dict_array_value_to_string::<UInt32Type>(column, row),
            //     DataType::UInt64 => dict_array_value_to_string::<UInt64Type>(column, row),
            //     _ => Err(ArrowError::InvalidArgumentError(format!(
            //         "Pretty printing not supported for {:?} due to index type",
            //         column.data_type()
            //     ))),
            // },

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
            _ => unreachable!("invalid value: {}, {}", col, self.stmt.column_type(col)),
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
        stmt.column_index(*self)
    }
}

macro_rules! tuple_try_from_row {
    ($($field:ident),*) => {
        impl<'a, $($field,)*> convert::TryFrom<&'a Row<'a>> for ($($field,)*) where $($field: FromSql,)* {
            type Error = crate::Error;

            // we end with index += 1, which rustc warns about
            // unused_variables and unused_mut are allowed for ()
            #[allow(unused_assignments, unused_variables, unused_mut)]
            fn try_from(row: &'a Row<'a>) -> Result<Self> {
                let mut index = 0;
                $(
                    #[allow(non_snake_case)]
                    let $field = row.get::<_, $field>(index)?;
                    index += 1;
                )*
                Ok(($($field,)*))
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
    #![allow(clippy::redundant_closure)] // false positives due to lifetime issues; clippy issue #5594
    use crate::{Connection, Result};

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
}
