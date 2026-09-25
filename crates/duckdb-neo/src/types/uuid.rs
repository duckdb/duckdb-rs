//! The `UUID` logical type.
//!
//! [`UuidValueRaw`] mirrors DuckDB's internal signed 128-bit UUID representation
//! exactly, so it is read and written through a direct pointer cast (see
//! [`super::primitive::DeclareVectorElement`]), the same pattern used by the
//! calendar/clock types in [`super::temporal`].
//!
//! With the `uuid` feature, `uuid::Uuid` can be used anywhere [`UuidValueRaw`]
//! can, converting to and from the internal representation on the fly.

use super::primitive::DeclareVectorElement;
use super::{DuckDBType, FromValue, ToValue};
use crate::ffi;
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    value::{Value, ValueInput},
    vector::{Vector, WritableVectorElement},
};

/// DuckDB's internal signed 128-bit UUID representation.
///
/// DuckDB stores a UUID as a signed integer with its most significant bit flipped, so
/// sorting matches the UUID byte order. Enable the `uuid` feature to convert to and from `uuid::Uuid`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UuidValueRaw(pub i128);

impl DuckDBType for UuidValueRaw {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create_from_id(LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID, Parameters::None)
    }
}

impl ToValue for UuidValueRaw {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Uuid(self.0))
    }
}

impl FromValue for UuidValueRaw {
    fn _get_inner(value: &Value) -> Result<Self> {
        let raw = check_api_call!(ffi::duckdb_v2_value_get_uuid, **value, RET)?;
        Ok(Self((i128::from(raw.upper) << 64) | i128::from(raw.lower)))
    }
}

DeclareVectorElement!(UuidValueRaw, DUCKDB_V2_LOGICAL_TYPE_ID_UUID);

impl WritableVectorElement for UuidValueRaw {
    type Write<'a> = Self;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        vector.write_raw(index, value)
    }
}

#[cfg(feature = "uuid")]
mod external {
    use super::UuidValueRaw;
    use crate::{
        Result,
        connection::FFILink,
        logical_type::{LogicalType, LogicalTypeID},
        types::{DuckDBType, FromValue, ToValue},
        value::Value,
        vector::{Vector, VectorElement, WritableVectorElement},
    };
    use ::uuid::Uuid;

    const SIGN_BIT: u128 = 1 << 127;

    impl From<Uuid> for UuidValueRaw {
        fn from(value: Uuid) -> Self {
            Self((value.as_u128() ^ SIGN_BIT) as i128)
        }
    }

    impl From<UuidValueRaw> for Uuid {
        fn from(value: UuidValueRaw) -> Self {
            Uuid::from_u128(value.0 as u128 ^ SIGN_BIT)
        }
    }

    impl DuckDBType for Uuid {
        fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
            UuidValueRaw::logical_type(link)
        }
    }

    impl ToValue for Uuid {
        fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
            UuidValueRaw::from(*self).value(link)
        }
    }

    impl FromValue for Uuid {
        fn _get_inner(value: &Value) -> Result<Self> {
            UuidValueRaw::_get_inner(value).map(Self::from)
        }
    }

    impl VectorElement for Uuid {
        const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID;

        type Internal = UuidValueRaw;

        type Ref<'a> = Uuid;

        fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
        where
            Self: 'a,
        {
            Self::from(*UuidValueRaw::get(vector, physical, logical))
        }
    }

    impl WritableVectorElement for Uuid {
        type Write<'a> = Uuid;

        fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
            vector.write_raw(index, value.map(UuidValueRaw::from))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            Parameters,
            data_chunk::DataChunk,
            environment::{Environment, StorageLocation},
        };
        use ::uuid::uuid;

        #[test]
        fn uuid_conversion_matches_duckdb() -> Result<()> {
            let env = Environment::new()?;
            let db = env.open(StorageLocation::InMemory)?;
            let conn = db.connect()?;
            let expected = [uuid!("0bc67299-bf0d-4bf2-b92c-634b1f79c4f8"), Uuid::nil(), Uuid::max()];

            assert_eq!(Uuid::from(UuidValueRaw(i128::MIN)), Uuid::nil());
            assert_eq!(UuidValueRaw::from(Uuid::max()), UuidValueRaw(i128::MAX));

            let query_one = |sql: &str, params: Parameters<'_>| -> Result<Option<Uuid>> {
                let mut result = conn.query(sql, params)?;
                let chunk = result.next().expect("query returned no rows")?;
                chunk.get_vector_at::<Uuid>(0)?.get(0)
            };

            for uuid in expected {
                let value = uuid.value(&conn)?;
                assert_eq!(value.dbg_string()?, uuid.to_string());
                assert_eq!(value.get::<Uuid>()?, Some(uuid));

                let bound = query_one("SELECT $1::UUID", Parameters::positional(&[&uuid]))?;
                assert_eq!(bound, Some(uuid));
                let parsed = query_one(&format!("SELECT '{uuid}'::UUID"), Parameters::None)?;
                assert_eq!(parsed, Some(uuid));
            }

            let chunk = DataChunk::create(&[Uuid::logical_type(&conn)?], true)?;
            let mut values = chunk.get_vector_at::<Uuid>(0)?;
            values.set_size(expected.len() + 1)?;
            for (index, uuid) in expected.into_iter().enumerate() {
                values.write(index, Some(uuid))?;
            }
            values.write(expected.len(), None)?;
            let mut all = expected.map(Some).to_vec();
            all.push(None);
            assert_eq!(values.iter()?.collect::<Vec<_>>(), all);

            Ok(())
        }
    }
}
