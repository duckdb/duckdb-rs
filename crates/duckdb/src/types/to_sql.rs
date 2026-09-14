use super::{Null, TimeUnit, Value, ValueRef, binding_unsupported_value, value_ref_from_value};
use crate::{Error, Result};
use std::borrow::Cow;

/// `ToSqlOutput` represents the possible output types for implementers of the
/// [`ToSql`] trait.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToSqlOutput<'a> {
    /// A borrowed DuckDB-representable value.
    Borrowed(ValueRef<'a>),

    /// An owned DuckDB-representable value.
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
from_value!(u128);
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
        Ok(match self {
            ToSqlOutput::Borrowed(v) => ToSqlOutput::Borrowed(*v),
            // Container values cannot become ValueRef from an owned Value, so
            // keep them Owned for the bind/append sites to handle recursively.
            ToSqlOutput::Owned(v) => match value_ref_from_value(v, binding_unsupported_value) {
                Ok(borrowed) => ToSqlOutput::Borrowed(borrowed),
                Err(_) => ToSqlOutput::Owned(v.clone()),
            },
        })
    }
}

/// A trait for types that can be converted into DuckDB values. Returns
/// [`Error::ToSqlConversionFailure`](crate::Error::ToSqlConversionFailure) if
/// the conversion fails.
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
to_sql_self!(u128);
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
        match self {
            // Container variants cannot become ValueRef from an owned Value.
            // Keep them Owned so the bind/append sites can build duckdb_value
            // recursively via the C value API.
            Value::List(_) | Value::Array(_) | Value::Struct(_) | Value::Map(_) | Value::Union(_) | Value::Enum(_) => {
                Ok(ToSqlOutput::Owned(self.clone()))
            }
            _ => Ok(ToSqlOutput::Borrowed(value_ref_from_value(
                self,
                binding_unsupported_value,
            )?)),
        }
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

/// Implements [`ToSql`] for `Vec<T>` and `[T; N]` by binding the elements as a
/// DuckDB fixed-size `ARRAY` (`T[N]`). Empty containers are rejected because
/// the element type cannot be inferred from the value alone.
macro_rules! to_sql_numeric_array {
    ($t:ty, $variant:ident) => {
        impl ToSql for Vec<$t> {
            fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                if self.is_empty() {
                    return Err(Error::ToSqlConversionFailure(
                        "cannot bind an empty Vec; the array element type cannot be inferred".into(),
                    ));
                }
                let value = Value::Array(self.iter().map(|x| Value::$variant(*x)).collect());
                Ok(ToSqlOutput::Owned(value))
            }
        }

        impl<const N: usize> ToSql for [$t; N] {
            fn to_sql(&self) -> Result<ToSqlOutput<'_>> {
                if N == 0 {
                    return Err(Error::ToSqlConversionFailure(
                        "cannot bind a zero-length array; DuckDB arrays require at least one element".into(),
                    ));
                }
                let value = Value::Array(self.iter().map(|x| Value::$variant(*x)).collect());
                Ok(ToSqlOutput::Owned(value))
            }
        }
    };
}

to_sql_numeric_array!(f32, Float);
to_sql_numeric_array!(f64, Double);
to_sql_numeric_array!(i8, TinyInt);
to_sql_numeric_array!(i16, SmallInt);
to_sql_numeric_array!(i32, Int);
to_sql_numeric_array!(i64, BigInt);
to_sql_numeric_array!(u16, USmallInt);
to_sql_numeric_array!(u32, UInt);
to_sql_numeric_array!(u64, UBigInt);

#[cfg(test)]
mod test {
    use super::{ToSql, ToSqlOutput};

    fn is_to_sql<T: ToSql>() {}

    #[test]
    fn test_integral_types() {
        is_to_sql::<i8>();
        is_to_sql::<i16>();
        is_to_sql::<i32>();
        is_to_sql::<i64>();
        is_to_sql::<i128>();
        is_to_sql::<isize>();
        is_to_sql::<u8>();
        is_to_sql::<u16>();
        is_to_sql::<u32>();
        is_to_sql::<u64>();
        is_to_sql::<u128>();
        is_to_sql::<usize>();
    }

    #[test]
    fn test_value_to_sql_preserves_owned_for_containers() {
        use crate::types::{OrderedMap, Value};

        let containers: &[Value] = &[
            Value::List(vec![Value::Int(1)]),
            Value::Array(vec![Value::Int(1)]),
            Value::Struct(OrderedMap::from(vec![("k".to_string(), Value::Int(1))])),
            Value::Map(OrderedMap::from(vec![(Value::Int(1), Value::Int(2))])),
        ];

        for value in containers {
            match value.to_sql() {
                Ok(ToSqlOutput::Owned(v)) => assert_eq!(v, *value, "container should round-trip as Owned"),
                other => panic!("expected ToSqlOutput::Owned for {:?}, got {other:?}", value),
            }
        }
    }

    #[test]
    fn test_enum_and_union_values_reject_at_bind() {
        use crate::{Connection, Error, types::Value};

        let cases: &[(&str, Value)] = &[
            ("Enum", Value::Enum("variant".to_string())),
            ("Union", Value::Union(Box::new(Value::Int(1)))),
        ];

        let conn = Connection::open_in_memory().unwrap();
        for (variant, value) in cases {
            let err = conn
                .query_row("SELECT ?", [value.clone()], |r| r.get::<_, Value>(0))
                .unwrap_err();
            match err {
                Error::ToSqlConversionFailure(e) => {
                    let msg = e.to_string();
                    assert!(
                        msg.contains(&format!("binding {variant} parameters is not yet supported")),
                        "{variant}: unexpected message {msg}"
                    );
                }
                other => panic!("{variant}: expected ToSqlConversionFailure, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_cow_str() {
        use std::borrow::Cow;
        let s = "str";
        let cow: Cow<'_, str> = Cow::Borrowed(s);
        let r = cow.to_sql();
        assert!(r.is_ok());
        let cow: Cow<'_, str> = Cow::Owned(String::from(s));
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

        let found_id: String = db.prepare("SELECT id FROM foo")?.query_one([], |r| r.get(0))?;
        assert_eq!(found_id.len(), 36);
        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid_blob_type() -> crate::Result<()> {
        use crate::{Connection, params};
        use uuid::Uuid;

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (id BLOB CONSTRAINT uuidchk CHECK (octet_length(id) = 16), label TEXT);")?;

        let id = Uuid::new_v4();
        let id_vec = id.as_bytes().to_vec();
        db.execute("INSERT INTO foo (id, label) VALUES (?, ?)", params![id_vec, "target"])?;

        let (found_id, found_label): (Uuid, String) = db
            .prepare("SELECT id, label FROM foo WHERE id = ?")?
            .query_one(params![id_vec], |r| Ok((r.get_unwrap(0), r.get_unwrap(1))))?;
        assert_eq!(found_id, id);
        assert_eq!(found_label, "target");
        Ok(())
    }

    #[cfg(feature = "uuid")]
    #[test]
    fn test_uuid_type() -> crate::Result<()> {
        use crate::{Connection, params};
        use uuid::Uuid;

        let db = Connection::open_in_memory()?;
        db.execute_batch("CREATE TABLE foo (id uuid, label TEXT);")?;

        let id = Uuid::new_v4();
        db.execute("INSERT INTO foo (id, label) VALUES (?, ?)", params![id, "target"])?;

        let (found_id, found_label): (Uuid, String) = db
            .prepare("SELECT id, label FROM foo WHERE id = ?")?
            .query_one(params![id], |r| Ok((r.get_unwrap(0), r.get_unwrap(1))))?;
        assert_eq!(found_id, id);
        assert_eq!(found_label, "target");
        Ok(())
    }
}
