use std::fmt::Display;

use libduckdb_sys::{DUCKDB_V2_FUNCTION_PROPERTY_KEY, DUCKDB_V2_FUNCTION_PROPERTY_VALUE};

use crate::{
    Context, DuckDBType, Environment, Parameters, StorageLocation,
    aggregate::{AggregateCallbacks, AggregateFunctionBuilder, BindMetadata, States},
    connection_options::OptionValue,
    data_chunk::DataChunk,
    signature::{Parameter, SignatureBuilder},
    vector::Vector,
};

struct BasicAggregate<T> {
    item: T,
}

// Formula = (user_data + bind_data) +  median()

impl<T: Display> AggregateCallbacks for BasicAggregate<T> {
    type BindData = Vec<f32>;
    type StateItem = Vec<i32>;
    type IncomingType = i32;
    type ResultType = String;

    fn bind(&self, context: Context, metadata: BindMetadata) -> crate::Result<Self::BindData> {
        let mut bind_data: Vec<f32> = Vec::new();

        for i in 0..metadata.arguments.len()? {
            let name = metadata.arguments.name(i)?;
            let arg_type = metadata.arguments.logical_type(i)?;

            assert_eq!(arg_type, i32::logical_type(&context)?);
            assert_eq!(name, "IN");
        }

        assert_eq!(metadata.function_name, "to_concatenated");

        bind_data.push(1.2);
        bind_data.push(3.4);

        Ok(bind_data)
    }

    fn init(&self) -> crate::Result<Self::StateItem> {
        Ok(vec![])
    }

    fn size(&self) -> crate::Result<usize> {
        Ok(std::mem::size_of::<Self::StateItem>())
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        data_chunk: DataChunk,
        states: &mut crate::aggregate::States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = data_chunk.get_vector_at::<Self::IncomingType>(0)?;

        for (i, val) in vec.iter()?.enumerate() {
            if let Some(val) = val {
                states[i].push(*val);
            }
        }

        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        source: &crate::aggregate::States<'_, Self::StateItem>,
        dest: &mut crate::aggregate::States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        for i in 0..dest.len() {
            let values = source[i].clone();
            dest[i].extend(values);
        }

        Ok(())
    }

    fn finalize(
        &self,
        bind_data: Option<&Self::BindData>,
        states: &mut States<'_, Self::StateItem>,
        result: &mut Vector<'_, Self::ResultType>,
        result_offset: usize,
    ) -> crate::Result<()> {
        for (index, state) in states.iter().enumerate() {
            let mut to_write = state
                .iter()
                .map(|value| value.to_string())
                .collect::<String>();
            to_write += &format!(
                " + ({:.1} + {:.1}) - {:.1}",
                bind_data.unwrap()[0],
                bind_data.unwrap()[1],
                self.item
            );
            result.write(result_offset + index, Some(&to_write))?;
        }

        Ok(())
    }
}

#[test]
pub fn basic_aggregate_test() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new(
            [Parameter::normal("IN", i32::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        BasicAggregate::<f32> { item: 0.0 },
    )
    .register_with_connection(&conn)?;

    let result = conn.query(
        "SELECT to_concatenated(i) AS result FROM (VALUES (1), (2), (NULL), (3), (4), (5)) AS t(i)",
        Parameters::None,
    )?;

    for chunk in result {
        let chunk = chunk?;

        let vec = chunk.get_vector_at::<String>(0)?;

        let res = vec.get(0)?;

        assert_eq!(res, Some("12345 + (1.2 + 3.4) - 0.0"));
    }

    Ok(())
}

#[test]
pub fn aggregate_test_invalid_build() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;

    db.set_option(&OptionValue::new("threads", &1.to_string())?)?;

    let conn = db.connect()?;

    let mut result = AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new(
            [Parameter::normal("IN", i32::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        BasicAggregate::<f32> { item: 2.5 },
    );

    result.properties.insert(
        DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_KEY_MAX_ENUM,
        DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_VALUE_MAX_ENUM,
    );

    let result = result.register_with_connection(&conn);

    assert!(result.is_err());

    let result = AggregateFunctionBuilder::new(
        "",
        SignatureBuilder::new(
            [Parameter::normal("IN", i32::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        BasicAggregate::<i32> { item: 0 },
    )
    .register_with_connection(&conn);

    assert!(result.is_err());

    Ok(())
}

#[test]
fn aggregate_test_groups() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new(
            [Parameter::normal("IN", i32::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        BasicAggregate::<i32> { item: 0 },
    )
    .register_with_connection(&conn)?;

    let result = conn.query(
        "SELECT
             (i - 1) // 5 AS group_id,
             to_concatenated(i::INTEGER) AS result
         FROM generate_series(1, 200000) AS t(i)
         GROUP BY group_id
         ORDER BY group_id",
        Parameters::None,
    )?;

    let mut groups = 0;
    for chunk in result {
        let chunk = chunk?;

        let vec = chunk.get_vector_at::<String>(1)?;

        for value in vec.iter()? {
            assert!(value.is_some());

            groups += 1;
        }
    }

    assert_eq!(groups, 40000);

    Ok(())
}
