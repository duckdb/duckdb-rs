use std::fmt::Display;

use libduckdb_sys::v2::{DUCKDB_V2_FUNCTION_PROPERTY_KEY, DUCKDB_V2_FUNCTION_PROPERTY_VALUE};

use crate::{
    DuckDBType, Parameters,
    aggregate::{AggregateCallbacks, AggregateFunctionBuilder, ReturnTypeHandle, States},
    bind_arguments::BindArguments,
    connection::{Context, SettingScope},
    data_chunk::VectorCollection,
    environment::{Environment, StorageLocation},
    logical_type::LogicalType,
    signature::{FunctionParameter, SignatureBuilder},
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
        arguments: BindArguments<'_>,
        result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        let mut bind_data: Vec<f32> = Vec::new();

        result_type_handle.override_return(LogicalType::from_text(context, "VARCHAR")?)?;

        for index in 0..arguments.len()? {
            assert_eq!(arguments.logical_type(index)?, i32::logical_type(context)?);
            assert!(arguments.value(index)?.is_none());
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
        collection: &mut VectorCollection,
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
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            // Will be overwritten to String
            .return_type(u8::logical_type(&conn)?),
        BasicAggregate::<f32> { item: 0.0 },
    )
    .register(&conn)?;

    let mut result = conn.query(
        "SELECT to_concatenated(i) AS result FROM (VALUES (1), (2), (NULL), (3), (4), (5)) AS t(i)",
        Parameters::None,
    )?;

    while let Some(chunk) = result.next_chunk()? {
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

    db.config()?.set_text("threads", &1.to_string(), SettingScope::Global)?;

    let conn = db.connect()?;

    let mut result = AggregateFunctionBuilder::new(
        "to_concatenated",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(String::logical_type(&conn)?),
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
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(String::logical_type(&conn)?),
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
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(String::logical_type(&conn)?),
        BasicAggregate::<i32> { item: 0 },
    )
    .register(&conn)?;

    let mut result = conn.query(
        "SELECT
             (i - 1) // 5 AS group_id,
             to_concatenated(i::INTEGER) AS result
         FROM generate_series(1, 200000) AS t(i)
         GROUP BY group_id
         ORDER BY group_id",
        Parameters::None,
    )?;

    let mut groups = 0;
    while let Some(chunk) = result.next_chunk()? {
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
        _arguments: BindArguments<'_>,
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
        _collection: &mut VectorCollection,
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
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(u64::logical_type(&conn)?),
        UndersizedState,
    )
    .register(&conn)?;

    let result = conn
        .query("SELECT undersized(i::INTEGER) FROM range(10) AS t(i)", Parameters::None)
        .and_then(|mut result| {
            while result.next_chunk()?.is_some() {}
            Ok(())
        });

    let err = result.expect_err("an undersized state must fail the query");
    assert!(err.to_string().contains("smaller than size_of"), "{err}");

    Ok(())
}

/// Sums with a state size that is not a multiple of the state's alignment.
struct PaddedSum;

impl AggregateCallbacks for PaddedSum {
    type BindData = ();
    type StateItem = i64;

    fn bind(
        &self,
        _context: &Context,
        _arguments: BindArguments<'_>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> crate::Result<Self::BindData> {
        Ok(())
    }

    fn size(&self, _bind_data: Option<&Self::BindData>) -> crate::Result<usize> {
        Ok(12)
    }

    fn init(&self, _bind_data: Option<&Self::BindData>) -> Self::StateItem {
        0
    }

    fn update(
        &self,
        _bind_data: Option<&Self::BindData>,
        collection: &mut VectorCollection,
        mut states: States<'_, Self::StateItem>,
    ) -> crate::Result<()> {
        let vec = collection.get_vector_at::<i32>(0)?;
        for (i, val) in vec.iter()?.enumerate() {
            states[i] += i64::from(*val.unwrap_or(&0));
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
            dest[i] += source[i];
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> crate::Result<()> {
        let mut result = result.cast::<i64>()?;
        // A window passes its result vector unsized.
        if result.len() < result_offset + states.len() {
            result.set_size(result_offset + states.len())?;
        }
        for (index, state) in states.iter().enumerate() {
            result.write(result_offset + index, Some(**state))?;
        }
        Ok(())
    }
}

#[test]
fn aggregate_test_window_with_padded_state() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    AggregateFunctionBuilder::new(
        "padded_sum",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("IN"))
            .return_type(i64::logical_type(&conn)?),
        PaddedSum,
    )
    .register(&conn)?;

    // Window operators lay states out back to back, by the reported size.
    let mut result = conn.query(
        "SELECT padded_sum(i::INTEGER) OVER () AS total,
                padded_sum(i::INTEGER) OVER (ORDER BY i) AS running
         FROM range(1, 101) AS t(i)
         ORDER BY i",
        Parameters::None,
    )?;

    let mut rows = Vec::new();
    while let Some(chunk) = result.next_chunk()? {
        let total = chunk.get_vector_at::<i64>(0)?;
        let running = chunk.get_vector_at::<i64>(1)?;
        for row in 0..total.len() {
            rows.push((*total.get(row)?.unwrap(), *running.get(row)?.unwrap()));
        }
    }

    let expected = (1..=100i64).map(|i| (5050, i * (i + 1) / 2)).collect::<Vec<_>>();
    assert_eq!(rows, expected);

    Ok(())
}
