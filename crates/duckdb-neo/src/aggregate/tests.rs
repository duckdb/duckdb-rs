use std::fmt::Display;

use libduckdb_sys::v2::{DUCKDB_V2_FUNCTION_PROPERTY_KEY, DUCKDB_V2_FUNCTION_PROPERTY_VALUE};

use crate::{
    DuckDBType, Parameters,
    aggregate::{AggregateCallbacks, AggregateFunctionBuilder, States},
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::VectorCollection,
    environment::{Environment, StorageLocation},
    logical_type::LogicalType,
    scalar::ReturnTypeHandle,
    signature::{Parameter, SignatureBuilder},
    vector::{Unknown, Vector},
};

struct BasicAggregate<T> {
    item: T,
}

// Formula = (user_data + bind_data) +  median()

impl<T: Display + Send + Sync + 'static> AggregateCallbacks for BasicAggregate<T> {
    type BindData = Vec<f32>;
    type StateItem = Vec<i32>;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        let mut bind_data: Vec<f32> = Vec::new();

        result_type_handle.override_return(LogicalType::from_text(context, "VARCHAR")?)?;

        for argument in arguments {
            let name = argument.value;
            let arg_type = argument.logical_type;

            assert_eq!(arg_type, i32::logical_type(context)?);
            assert!(name.is_none());
        }

        bind_data.push(1.2);
        bind_data.push(3.4);

        Ok(bind_data)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        vec![]
    }

    fn size(&self, _bind_data: Option<&Self::BindData>) -> crate::Result<usize> {
        Ok(std::mem::size_of::<Self::StateItem>())
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        collection: &VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = collection.get_vector_at::<i32>(0)?;

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
        source: &States<'_, Self::StateItem>,
        mut dest: States<'_, Self::StateItem>,
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
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> crate::Result<()> {
        let mut result = result.cast::<String>()?;

        for (index, state) in states.iter().enumerate() {
            let mut to_write = state.iter().map(|value| value.to_string()).collect::<String>();
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
            u8::logical_type(&conn)?, // Will be overwritten to String
        ),
        BasicAggregate::<f32> { item: 0.0 },
    )
    .register(&conn)?;

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

    db.set_option("threads", &1.to_string())?;

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

    let result = result.register(&conn);

    assert!(result.is_err());

    let result = AggregateFunctionBuilder::new(
        "",
        SignatureBuilder::new(
            [Parameter::normal("IN", i32::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        BasicAggregate::<i32> { item: 0 },
    )
    .register(&conn);

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
    .register(&conn)?;

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

/// Reports a state size smaller than its `StateItem`, which must be rejected.
struct UndersizedState;

impl AggregateCallbacks for UndersizedState {
    type BindData = ();
    type StateItem = u64;

    fn bind(
        &self,
        _context: &Context,
        _arguments: Vec<BindArgument>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        Ok(())
    }

    fn size(&self, _bind_data: Option<&Self::BindData>) -> crate::Result<usize> {
        Ok(1)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        0
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        _collection: &VectorCollection,
        _states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&Self::BindData>,
        _source: &States<'_, Self::StateItem>,
        _dest: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        _states: &[&Self::StateItem],
        _result: Vector<'_, Unknown>,
        _result_offset: usize,
    ) -> crate::Result<()> {
        Ok(())
    }
}

#[test]
fn aggregate_test_undersized_state() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    AggregateFunctionBuilder::new(
        "undersized",
        SignatureBuilder::new(
            [Parameter::normal("IN", i32::logical_type(&conn)?)],
            u64::logical_type(&conn)?,
        ),
        UndersizedState,
    )
    .register(&conn)?;

    let result = conn
        .query("SELECT undersized(i::INTEGER) FROM range(10) AS t(i)", Parameters::None)
        .and_then(|result| result.collect::<crate::Result<Vec<_>>>());

    let err = result.expect_err("an undersized state must fail the query");
    assert!(err.to_string().contains("smaller than size_of"), "{err}");

    Ok(())
}
