//! The `UUID` logical type.
//!
//! [`UuidValueRaw`] mirrors DuckDB's internal signed 128-bit UUID representation
//! exactly, so it is read and written through a direct pointer cast (see
//! [`super::primitive::declare_vector_element`]), the same pattern used by the
//! calendar/clock types in [`super::temporal`].
//!
//! With the `uuid` feature, `uuid::Uuid` can be used anywhere [`UuidValueRaw`]
//! can, converting to and from the internal representation on the fly.

use super::{DuckDBType, FromValue, ToValue};
use crate::ffi;
use crate::vector::VectorElement;
use crate::{
    Parameters, Result, check_api_call,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    raw::RawExt,
    value::{Value, ValueInput},
    vector::{Vector, WritableVectorElement},
};

/// DuckDB's internal signed 128-bit UUID representation.
///
/// DuckDB stores a UUID as a signed integer with its most significant bit flipped, so
/// sorting matches the UUID byte order. Enable the `uuid` feature to convert to and from `uuid::Uuid`.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UuidValueRaw(pub i128);

impl UuidValueRaw {
    /// Convert canonical big-endian UUID bytes to DuckDB's signed storage form.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self((u128::from_be_bytes(bytes) ^ (1 << 127)) as i128)
    }

    /// Return the canonical big-endian UUID bytes.
    pub const fn to_bytes(self) -> [u8; 16] {
        (self.0 as u128 ^ (1 << 127)).to_be_bytes()
    }
}

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
        check_api_call!(ffi::duckdb_v2_value_get_uuid, value.raw(), RET)
            .map(|raw| Self(crate::value::from_hugeint(raw)))
    }
}

// SAFETY: `UuidValueRaw` has the size and bit layout of DuckDB's UUID storage.
unsafe impl VectorElement for UuidValueRaw {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID;

    type Internal = ffi::duckdb_v2_hugeint_t;

    type Ref<'a>
        = &'a UuidValueRaw
    where
        Self: 'a;

    unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.as_ref().unwrap().as_ptr() as *const UuidValueRaw;
        unsafe { &*data_ptr.add(physical) }
    }
}

// SAFETY: every `i128` is a valid UUID.
unsafe impl WritableVectorElement for UuidValueRaw {
    type Write<'a> = Self;

    unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        // SAFETY: The caller guarantees a writable, flat, in-bounds row.
        unsafe { vector.write_internal(index, value) }
    }
}

#[cfg(test)]
mod tests {
    use super::UuidValueRaw;
    use std::collections::{BTreeSet, HashSet};

    #[test]
    fn uuid_raw_canonical_bytes_order_and_hash() {
        const NIL: UuidValueRaw = UuidValueRaw::from_bytes([0; 16]);
        const NIL_BYTES: [u8; 16] = NIL.to_bytes();
        assert_eq!(NIL, UuidValueRaw(i128::MIN));
        assert_eq!(NIL_BYTES, [0; 16]);

        let canonical = [0, (1_u128 << 127) - 1, 1_u128 << 127, u128::MAX];
        let values = canonical.map(|value| UuidValueRaw::from_bytes(value.to_be_bytes()));
        assert_eq!(values.map(|value| u128::from_be_bytes(value.to_bytes())), canonical);
        assert_eq!(values.map(|value| value.0), [i128::MIN, -1, 0, i128::MAX]);
        assert_eq!(
            values
                .into_iter()
                .rev()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>(),
            values
        );
        assert_eq!(values.into_iter().collect::<HashSet<_>>().len(), values.len());
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

    impl From<Uuid> for UuidValueRaw {
        fn from(value: Uuid) -> Self {
            Self::from_bytes(*value.as_bytes())
        }
    }

    impl From<UuidValueRaw> for Uuid {
        fn from(value: UuidValueRaw) -> Self {
            Uuid::from_bytes(value.to_bytes())
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

    // SAFETY: same storage as `UuidValueRaw`.
    unsafe impl VectorElement for Uuid {
        const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID;

        type Internal = UuidValueRaw;

        type Ref<'a> = Uuid;

        unsafe fn get<'a, U: VectorElement>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
        where
            Self: 'a,
        {
            // SAFETY: same storage as `UuidValueRaw`; the caller upholds the contract.
            Self::from(*unsafe { UuidValueRaw::get(vector, physical, logical) })
        }
    }

    // SAFETY: every `Uuid` converts to a valid `UuidValueRaw`.
    unsafe impl WritableVectorElement for Uuid {
        type Write<'a> = Uuid;

        unsafe fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
            // SAFETY: The caller guarantees a writable, flat, in-bounds row.
            unsafe { vector.write_internal(index, value.map(UuidValueRaw::from)) }
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
            let instance = env.open(StorageLocation::InMemory)?;
            let conn = instance.connect()?;
            let expected = [uuid!("0bc67299-bf0d-4bf2-b92c-634b1f79c4f8"), Uuid::nil(), Uuid::max()];

            assert_eq!(Uuid::from(UuidValueRaw(i128::MIN)), Uuid::nil());
            assert_eq!(UuidValueRaw::from(Uuid::max()), UuidValueRaw(i128::MAX));

            let query_one = |sql: &str, params: Parameters<'_>| -> Result<Option<Uuid>> {
                let mut result = conn.query(sql, params)?;
                let chunk = result.next_chunk()?.expect("query returned no rows");
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

            let mut chunk = DataChunk::create(&conn, &[Uuid::logical_type(&conn)?], true)?;
            let mut values = chunk.get_vector_at_mut::<Uuid>(0)?;
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
