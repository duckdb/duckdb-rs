use crate::{Appender, Result, ToSql};

mod sealed {
    /// This trait exists just to ensure that the only impls of
    /// `trait AppenderParams` that are allowed are ones in this crate.
    pub trait Sealed {}
}
use sealed::Sealed;

/// Trait used for rows passed to [`Appender::append_row`] and
/// [`Appender::append_rows`].
///
/// Note: Currently, this trait can only be implemented inside this crate.
/// Additionally, its methods (which are `doc(hidden)`) should currently not be
/// considered part of the stable API, although it's possible they will
/// stabilize in the future.
///
/// `AppenderParams` normalizes the row values accepted by the appender API into
/// the positional values DuckDB appends to one table row. It is separate from
/// [`Params`](crate::Params), which is used for prepared statements and
/// queries.
///
/// Supported forms include:
///
/// - tuples of mixed types, up to 16 elements;
/// - arrays and references to arrays, up to 32 elements;
/// - [`duckdb::params!`](crate::params!) or `&[&dyn ToSql]` for heterogeneous
///   rows or rows assembled dynamically;
/// - [`appender_params_from_iter`] for iterators over one concrete
///   [`ToSql`] item type.
///
/// # Example
///
/// ```rust,no_run
/// # use duckdb::{Connection, Result, params};
/// fn append_rows(conn: &Connection) -> Result<()> {
///     conn.execute_batch("CREATE TABLE people (id INTEGER, name TEXT)")?;
///     let mut appender = conn.appender("people")?;
///
///     appender.append_row((1i32, "Alice"))?;
///     appender.append_row(params![2i32, "Bob"])?;
///
///     appender.flush()
/// }
/// ```
pub trait AppenderParams: Sealed {
    // XXX not public api, might not need to expose.
    //
    // Binds the parameters to the appender. It is unlikely calling this
    // explicitly will do what you want. Please use `Appender::append_row` or
    // similar directly.
    //
    // For now, just hide the function in the docs...
    #[doc(hidden)]
    fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()>;
}

// Explicitly impl for empty array. Critically, for `conn.execute([])` to be
// unambiguous, this must be the *only* implementation for an empty array. This
// avoids `NO_PARAMS` being a necessary part of the API.
impl Sealed for [&dyn ToSql; 0] {}
impl AppenderParams for [&dyn ToSql; 0] {
    #[inline]
    fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()> {
        // Route through the normal append path so DuckDB can reject empty rows
        // for tables that require values.
        stmt.append_parameter_row(&[] as &[&dyn ToSql])
    }
}

impl Sealed for &[&dyn ToSql] {}
impl AppenderParams for &[&dyn ToSql] {
    #[inline]
    fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()> {
        stmt.append_parameter_row(self)
    }
}

macro_rules! impl_for_array_ref {
    ($($N:literal)+) => {$(
        // These are already generic, and there's a shedload of them, so lets
        // avoid the compile time hit from making them all inline for now.
        impl<T: ToSql + ?Sized> Sealed for &[&T; $N] {}
        impl<T: ToSql + ?Sized> AppenderParams for &[&T; $N] {
            fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()> {
                stmt.append_parameter_row(self)
            }
        }
        impl<T: ToSql> Sealed for [T; $N] {}
        impl<T: ToSql> AppenderParams for [T; $N] {
            #[inline]
            fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()> {
                stmt.append_parameter_row(&self)
            }
        }
    )+};
}

// Following libstd/libcore's (old) lead, implement this for arrays up to `[_;
// 32]`. Note `[_; 0]` is intentionally omitted for coherence reasons, see the
// note above the impl of `[&dyn ToSql; 0]` for more information.
//
// For tables with more than 32 columns, users should use:
// - `&[&dyn ToSql]` for dynamic parameter binding (recommended)
// - `params!` macro
// - `appender_params_from_iter`
impl_for_array_ref!(
    1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17
    18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
);

macro_rules! impl_appender_params_for_tuple {
    ($($T:ident),+) => {
        impl<$($T: ToSql),+> Sealed for ($($T,)+) {}
        impl<$($T: ToSql),+> AppenderParams for ($($T,)+) {
            fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()> {
                #[allow(non_snake_case)]
                let ($($T,)+) = &self;
                stmt.append_parameter_row([$($T as &dyn ToSql),+])
            }
        }
    };
}

// Implement AppenderParams for tuples, following the pattern from rusqlite's Params trait
// (https://github.com/rusqlite/rusqlite/blob/master/src/params.rs).
// This allows ergonomic row insertion without lifetime issues, e.g.:
//   appender.append_rows(data.iter().map(|(id, name)| (id, name)))
// Support tuples up to arity 16, matching rusqlite's Params.
impl_appender_params_for_tuple!(T1);
impl_appender_params_for_tuple!(T1, T2);
impl_appender_params_for_tuple!(T1, T2, T3);
impl_appender_params_for_tuple!(T1, T2, T3, T4);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15);
impl_appender_params_for_tuple!(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16);

/// Adapter type which allows any iterator over [`ToSql`] values to implement
/// [`AppenderParams`].
///
/// This struct is created by the [`appender_params_from_iter`] function.
///
/// This can be useful when the values for one appended row are already stored
/// in a collection or produced by an iterator. It avoids allocating storage for
/// something like a `Vec<&dyn ToSql>`.
///
/// The iterator must yield one concrete item type that implements [`ToSql`].
/// For heterogeneous rows, use a tuple, [`duckdb::params!`](crate::params!), or
/// a `&[&dyn ToSql]`. As with every [`Appender::append_row`] call, the number
/// of values must match the target table's column count.
///
/// # Example
///
/// ```rust,no_run
/// use duckdb::{Connection, Result, appender_params_from_iter};
///
/// fn append_scores(conn: &Connection, scores: Vec<i32>) -> Result<()> {
///     conn.execute_batch("CREATE TABLE scores (a INTEGER, b INTEGER, c INTEGER)")?;
///     let mut appender = conn.appender("scores")?;
///
///     appender.append_row(appender_params_from_iter(scores))?;
///     appender.flush()
/// }
/// ```
#[derive(Clone, Debug)]
pub struct AppenderParamsFromIter<I>(I);

/// Constructor function for an [`AppenderParamsFromIter`]. See its
/// documentation for more.
#[inline]
pub fn appender_params_from_iter<I>(iter: I) -> AppenderParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
    AppenderParamsFromIter(iter)
}

impl<I> Sealed for AppenderParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
}

impl<I> AppenderParams for AppenderParamsFromIter<I>
where
    I: IntoIterator,
    I::Item: ToSql,
{
    #[inline]
    fn __bind_in(self, stmt: &mut Appender<'_>) -> Result<()> {
        stmt.append_parameter_row(self.0)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Connection, Result};

    #[test]
    fn test_append_row_tuple() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")?;

        let mut app = db.appender("test")?;
        app.append_row((1i32, "alice"))?;
        app.flush()?;

        let mut stmt = db.prepare("SELECT id, name FROM test")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        assert_eq!(row.get::<_, i32>(0)?, 1i32);
        assert_eq!(row.get::<_, String>(1)?, "alice");

        Ok(())
    }

    #[test]
    fn test_append_rows_iterator_tuple() -> Result<()> {
        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE test (id INTEGER, name TEXT)")?;

        #[allow(clippy::useless_vec)]
        let data = vec![(1, "alice"), (2, "bob")];
        let mut app = db.appender("test")?;
        app.append_rows(data.iter().map(|(id, name)| (*id, *name)))?;
        app.flush()?;

        let count: i32 = db.query_row("SELECT COUNT(*) FROM test", [], |r| r.get(0))?;
        assert_eq!(count, 2);

        Ok(())
    }
}
