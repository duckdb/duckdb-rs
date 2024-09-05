use super::{Null, TimeUnit, Value, ValueRef};
use crate::Result;
use std::borrow::Cow;

/// `ToSqlOutput` represents the possible output types for implementers of the
/// [`ToSql`] trait.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToSqlOutput<'a> {
    /// A borrowed SQLite-representable value.
    Borrowed(ValueRef<'a>),

    /// An owned SQLite-representable value.
    Owned(Value),
}

// Generically allow any type that can be converted into a ValueRef
// to be converted into a ToSqlOutput as well.
impl<'a, T: ?Sized> From<&'a T> for ToSqlOutput<'a>
where
    &'a T: Into<ValueRef<'a>>,
{
    #[inline]
    fn from(t: &'a T) -> Self {
        ToSqlOutput::Borrowed(t.into())
    }
}

// We cannot also generically allow any type that can be converted
// into a Value to be converted into a ToSqlOutput because of
// coherence rules (https://github.com/rust-lang/rust/pull/46192),
// so we'll manually implement it for all the types we know can
// be converted into Values.
macro_rules! from_value(
    ($t:ty) => (
        impl From<$t> for ToSqlOutput<'_> {
            #[inline]
            fn from(t: $t) -> Self { ToSqlOutput::Owned(t.into())}
        }
    )
);
from_value!(String);
from_value!(Null);
from_value!(bool);
from_value!(i8);
from_value!(i16);
from_value!(i32);
from_value!(i64);
from_value!(i128);
from_value!(isize);
from_value!(u8);
from_value!(u16);
from_value!(u32);
from_value!(u64);
from_value!(usize);
from_value!(f32);
from_value!(f64);
from_value!(Vec<u8>);

#[cfg(feature = "uuid")]
from_value!(uuid::Uuid);

impl ToSql for ToSqlOutput<'_> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(match *self {
            ToSqlOutput::Borrowed(v) => ToSqlOutput::Borrowed(v),
            ToSqlOutput::Owned(ref v) => ToSqlOutput::Borrowed(ValueRef::from(v)),
        })
    }
}

/// A trait for types that can be converted into DuckDB values. Returns
/// [`Error::ToSqlConversionFailure`] if the conversion fails.
pub trait ToSql {
    /// Converts Rust value to DuckDB value
    fn to_sql(&self) -> Result<ToSqlOutput<'_>>;
}

impl<T: ToSql + ToOwned + ?Sized> ToSql for Cow<'_, T> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

impl<T: ToSql + ?Sized> ToSql for Box<T> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

impl<T: ToSql + ?Sized> ToSql for std::rc::Rc<T> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

impl<T: ToSql + ?Sized> ToSql for std::sync::Arc<T> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        self.as_ref().to_sql()
    }
}

// We should be able to use a generic impl like this:
//
// impl<T: Copy> ToSql for T where T: Into<Value> {
//     fn to_sql(&self) -> Result<ToSqlOutput> {
//         Ok(ToSqlOutput::from((*self).into()))
//     }
// }
//
// instead of the following macro, but this runs afoul of
// https://github.com/rust-lang/rust/issues/30191 and reports conflicting
// implementations even when there aren't any.

macro_rules! to_sql_self(
    ($t:ty) => (
        impl ToSql for $t {
            #[inline]
            fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                Ok(ToSqlOutput::from(*self))
            }
        }
    )
);

to_sql_self!(Null);
to_sql_self!(bool);
to_sql_self!(i8);
to_sql_self!(i16);
to_sql_self!(i32);
to_sql_self!(i64);
to_sql_self!(i128);
to_sql_self!(isize);
to_sql_self!(u8);
to_sql_self!(u16);
to_sql_self!(u32);
to_sql_self!(f32);
to_sql_self!(f64);
to_sql_self!(u64);
to_sql_self!(usize);

#[cfg(feature = "uuid")]
to_sql_self!(uuid::Uuid);

impl<T: ?Sized> ToSql for &'_ T
where
    T: ToSql,
{
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        (*self).to_sql()
    }
}

impl ToSql for String {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_str()))
    }
}

impl ToSql for str {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl ToSql for Vec<u8> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self.as_slice()))
    }
}

impl ToSql for [u8] {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl ToSql for Value {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl<T: ToSql> ToSql for Option<T> {
    #[inline]
    fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
        match *self {
            None => Ok(ToSqlOutput::from(Null)),
            Some(ref t) => t.to_sql(),
        }
    }
}

impl ToSql for std::time::Duration {
    fn to_sql(&self) -> crate::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Owned(Value::Timestamp(
            TimeUnit::Microsecond,
            self.as_micros() as i64,
        )))
    }
}

#[cfg(test)]
mod test {
    use super::ToSql;

    fn is_to_sql<T: ToSql>() {}

    #[test]
    fn test_integral_types() {
        is_to_sql::<i8>();
        is_to_sql::<i16>();
        is_to_sql::<i32>();
        is_to_sql::<i64>();
        is_to_sql::<u8>();
        is_to_sql::<u16>();
        is_to_sql::<u32>();
    }

    #[test]
    fn test_cow_str() {
        use std::borrow::Cow;
        let s = "str";
        let cow: Cow<str> = Cow::Borrowed(s);
        let r = cow.to_sql();
        assert!(r.is_ok());
        let cow: Cow<str> = Cow::Owned::<str>(String::from(s));
        let r = cow.to_sql();
        assert!(r.is_ok());
        // Ensure this compiles.
        let _p: &[&dyn ToSql] = crate::params![cow];
    }

    #[test]
    fn test_box_dyn() {
        let s: Box<dyn ToSql> = Box::new("Hello world!");
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = ToSql::to_sql(&s);

        assert!(r.is_ok());
    }

    #[test]
    fn test_box_deref() {
        let s: Box<str> = "Hello world!".into();
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();

        assert!(r.is_ok());
    }

    #[test]
    fn test_box_direct() {
        let s: Box<str> = "Hello world!".into();
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = ToSql::to_sql(&s);

        assert!(r.is_ok());
    }

    #[test]
    fn test_cells() {
        use std::{rc::Rc, sync::Arc};

        let source_str: Box<str> = "Hello world!".into();

        let s: Rc<Box<str>> = Rc::new(source_str.clone());
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Arc<Box<str>> = Arc::new(source_str.clone());
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Arc<str> = Arc::from(&*source_str);
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Arc<dyn ToSql> = Arc::new(source_str.clone());
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Rc<str> = Rc::from(&*source_str);
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());

        let s: Rc<dyn ToSql> = Rc::new(source_str);
        let _s: &[&dyn ToSql] = crate::params![s];
        let r = s.to_sql();
        assert!(r.is_ok());
    }

    // Use gen_random_uuid() to generate uuid
    #[test]
    fn test_uuid_gen() -> crate::Result<()> {
        use crate::Connection;

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (id uuid NOT NULL);")?;

        db.execute("INSERT INTO foo (id) VALUES (gen_random_uuid())", [])?;

        let mut stmt = db.prepare("SELECT id FROM foo")?;
        let mut rows = stmt.query([])?;
        let row = rows.next()?.unwrap();
        let found_id: String = row.get_unwrap(0);
        assert_eq!(found_id.len(), 36);
        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid_blob_type() -> crate::Result<()> {
        use crate::{params, Connection};
        use uuid::Uuid;

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (id BLOB CONSTRAINT uuidchk CHECK (octet_length(id) = 16), label TEXT);")?;

        let id = Uuid::new_v4();
        let id_vec = id.as_bytes().to_vec();
        db.execute("INSERT INTO foo (id, label) VALUES (?, ?)", params![id_vec, "target"])?;

        let mut stmt = db.prepare("SELECT id, label FROM foo WHERE id = ?")?;
        let mut rows = stmt.query(params![id_vec])?;
        let row = rows.next()?.unwrap();
        let found_id: Uuid = row.get_unwrap(0);
        let found_label: String = row.get_unwrap(1);
        assert_eq!(found_id, id);
        assert_eq!(found_label, "target");
        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid_type() -> crate::Result<()> {
        use crate::{params, Connection};
        use uuid::Uuid;

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (id uuid, label TEXT);")?;

        let id = Uuid::new_v4();
        db.execute("INSERT INTO foo (id, label) VALUES (?, ?)", params![id, "target"])?;

        let mut stmt = db.prepare("SELECT id, label FROM foo WHERE id = ?")?;
        let mut rows = stmt.query(params![id])?;
        let row = rows.next()?.unwrap();
        let found_id: Uuid = row.get_unwrap(0);
        let found_label: String = row.get_unwrap(1);
        assert_eq!(found_id, id);
        assert_eq!(found_label, "target");
        Ok(())
    }
}
