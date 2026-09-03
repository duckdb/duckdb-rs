use crate::{
    Result,
    connection::FFILink,
    logical_type::{LogicalType, LogicalTypeID},
    types::{DuckDBType, FromValue, ToValue, UuidValueRaw},
    value::Value,
    vector::{Vector, VectorElement, WritableVectorElement},
};

impl From<uuid::Uuid> for UuidValueRaw {
    fn from(value: uuid::Uuid) -> Self {
        let lower = value.as_u128() as u64;
        let upper = (value.as_u128() >> 64) as u64;

        if upper > i64::MAX as u64 {
            UuidValueRaw(((upper - i64::MAX as u64 - 1) as i128) << 64 | lower as i128)
        } else {
            UuidValueRaw(((upper as i128) - i64::MAX as i128 - 1) << 64 | lower as i128)
        }
    }
}

impl From<UuidValueRaw> for uuid::Uuid {
    fn from(value: UuidValueRaw) -> Self {
        let upper = (value.0 >> 64) as u64;
        let upper = upper ^ (1u64 << 63);
        let lower = value.0 as u64;

        let mut bytes = [0u8; 16];

        for i in 0..8 {
            bytes[i] = ((upper >> (56 - 8 * i)) & 0xFF) as u8;
            bytes[8 + i] = ((lower >> (56 - 8 * i)) & 0xFF) as u8;
        }
        uuid::Uuid::from_bytes(bytes)
    }
}

impl DuckDBType for uuid::Uuid {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        UuidValueRaw::logical_type(link)
    }
}

impl ToValue for uuid::Uuid {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        UuidValueRaw::from(*self).value(link)
    }
}

impl FromValue for uuid::Uuid {
    fn from_value(value: &Value) -> Result<Self> {
        UuidValueRaw::from_value(value).map(Self::from)
    }
}

impl VectorElement for uuid::Uuid {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UUID;

    type Ref<'a> = Self;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        Self::from(*UuidValueRaw::get(vector, physical, logical))
    }
}

impl WritableVectorElement for uuid::Uuid {
    type Write<'a> = Self;

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
    use uuid::{Uuid, uuid};

    #[test]
    fn uuid_is_supported_by_the_public_api() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;
        let expected = [uuid!("0bc67299-bf0d-4bf2-b92c-634b1f79c4f8"), Uuid::nil(), Uuid::max()];

        for uuid in expected {
            let value = uuid.value(&conn)?;
            assert_eq!(value.get::<Uuid>()?, uuid);

            let mut result = conn.query("SELECT $1::UUID", Parameters::positional(&[&uuid]))?;
            let chunk = result.next().transpose()?.expect("query returned no rows");
            let values = chunk.get_vector_at::<Uuid>(0)?;
            assert_eq!(values.get(0)?, Some(uuid));
        }

        let logical_type = Uuid::logical_type(&conn)?;
        let chunk = DataChunk::create(&[logical_type], true)?;
        let mut values = chunk.get_vector_at::<Uuid>(0)?;
        values.set_size(expected.len())?;
        for (index, uuid) in expected.into_iter().enumerate() {
            values.write(index, Some(uuid))?;
        }
        assert_eq!(values.iter()?.collect::<Vec<_>>(), expected.map(Some));

        Ok(())
    }
}
