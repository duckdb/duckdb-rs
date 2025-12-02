use std::sync::Arc;

use arrow::{
    array::{Array, RecordBatch},
    datatypes::DataType,
};

use crate::{
    core::DataChunkHandle,
    vtab::arrow::{data_chunk_to_arrow, to_duckdb_logical_type, write_arrow_array_to_vector, WritableVector},
};

use super::{ScalarFunctionSignature, ScalarParams, VScalar};

/// The possible parameters of a scalar function that accepts and returns arrow types
pub enum ArrowScalarParams {
    /// The exact parameters of the scalar function
    Exact(Vec<DataType>),
    /// The variadic parameter of the scalar function
    Variadic(DataType),
}

impl AsRef<[DataType]> for ArrowScalarParams {
    fn as_ref(&self) -> &[DataType] {
        match self {
            Self::Exact(params) => params.as_ref(),
            Self::Variadic(param) => std::slice::from_ref(param),
        }
    }
}

impl From<ArrowScalarParams> for ScalarParams {
    fn from(params: ArrowScalarParams) -> Self {
        match params {
            ArrowScalarParams::Exact(params) => Self::Exact(
                params
                    .into_iter()
                    .map(|v| to_duckdb_logical_type(&v).expect("type should be converted"))
                    .collect(),
            ),
            ArrowScalarParams::Variadic(param) => {
                Self::Variadic(to_duckdb_logical_type(&param).expect("type should be converted"))
            }
        }
    }
}

/// A signature for a scalar function that accepts and returns arrow types
pub struct ArrowFunctionSignature {
    /// The parameters of the scalar function
    pub parameters: Option<ArrowScalarParams>,
    /// The return type of the scalar function
    pub return_type: DataType,
}

impl ArrowFunctionSignature {
    /// Create an exact function signature
    pub fn exact(params: Vec<DataType>, return_type: DataType) -> Self {
        Self {
            parameters: Some(ArrowScalarParams::Exact(params)),
            return_type,
        }
    }

    /// Create a variadic function signature
    pub fn variadic(param: DataType, return_type: DataType) -> Self {
        Self {
            parameters: Some(ArrowScalarParams::Variadic(param)),
            return_type,
        }
    }
}

/// A trait for scalar functions that accept and return arrow types that can be registered with DuckDB
pub trait VArrowScalar: Sized {
    /// State set at registration time. Persists for the lifetime of the catalog entry.
    /// Shared across worker threads and invocations — must not be modified during execution.
    /// Must be `'static` as it is stored in DuckDB and may outlive the current stack frame.
    type State: Default + Sized + Send + Sync + 'static;

    /// The actual function that is called by DuckDB
    fn invoke(state: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>>;

    /// The possible signatures of the scalar function. These will result in DuckDB scalar function overloads.
    /// The invoke method should be able to handle all of these signatures.
    fn signatures() -> Vec<ArrowFunctionSignature>;

    /// Whether the scalar function is volatile.
    ///
    /// Volatile functions are re-evaluated for each row, even if they have no parameters.
    /// This is useful for functions that generate random or unique values, such as random
    /// number generators, UUID generators, or fake data generators.
    ///
    /// By default, DuckDB optimizes zero-argument scalar functions as constants, evaluating
    /// them only once. Returning true from this method prevents this optimization.
    ///
    /// # Default
    /// Returns `false` by default, meaning the function is not volatile.
    fn volatile() -> bool {
        false
    }
}

impl<T> VScalar for T
where
    T: VArrowScalar,
{
    type State = T::State;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        out: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let array = T::invoke(state, data_chunk_to_arrow(input)?)?;
        write_arrow_array_to_vector(&array, out)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        T::signatures()
            .into_iter()
            .map(|sig| ScalarFunctionSignature {
                parameters: sig.parameters.map(Into::into),
                return_type: to_duckdb_logical_type(&sig.return_type).expect("type should be converted"),
            })
            .collect()
    }
}

#[cfg(test)]
mod test {

    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{Array, RecordBatch, StringArray},
        datatypes::DataType,
    };

    use crate::{vscalar::arrow::ArrowFunctionSignature, Connection};

    use super::VArrowScalar;

    struct HelloScalarArrow {}

    impl VArrowScalar for HelloScalarArrow {
        type State = ();

        fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
            let name = input.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let result = name.iter().map(|v| format!("Hello {}", v.unwrap())).collect::<Vec<_>>();
            Ok(Arc::new(StringArray::from(result)))
        }

        fn signatures() -> Vec<ArrowFunctionSignature> {
            vec![ArrowFunctionSignature::exact(vec![DataType::Utf8], DataType::Utf8)]
        }
    }

    #[derive(Debug)]
    struct MockState {
        info: String,
    }

    impl Default for MockState {
        fn default() -> Self {
            Self {
                info: "some meta".to_string(),
            }
        }
    }

    impl Drop for MockState {
        fn drop(&mut self) {
            println!("dropped meta");
        }
    }

    struct ArrowMultiplyScalar {}

    impl VArrowScalar for ArrowMultiplyScalar {
        type State = MockState;

        fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
            let a = input
                .column(0)
                .as_any()
                .downcast_ref::<::arrow::array::Float32Array>()
                .unwrap();

            let b = input
                .column(1)
                .as_any()
                .downcast_ref::<::arrow::array::Float32Array>()
                .unwrap();

            let result = a
                .iter()
                .zip(b.iter())
                .map(|(a, b)| a.unwrap() * b.unwrap())
                .collect::<Vec<_>>();
            Ok(Arc::new(::arrow::array::Float32Array::from(result)))
        }

        fn signatures() -> Vec<ArrowFunctionSignature> {
            vec![ArrowFunctionSignature::exact(
                vec![DataType::Float32, DataType::Float32],
                DataType::Float32,
            )]
        }
    }

    // accepts a string or a number and parses to int and multiplies by 2
    struct ArrowOverloaded {}

    impl VArrowScalar for ArrowOverloaded {
        type State = MockState;

        fn invoke(state: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
            assert_eq!("some meta", state.info);

            let a = input.column(0);
            let b = input.column(1);

            let result = match a.data_type() {
                DataType::Utf8 => {
                    let a = a
                        .as_any()
                        .downcast_ref::<::arrow::array::StringArray>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap().parse::<f32>().unwrap())
                        .collect::<Vec<_>>();
                    let b = b
                        .as_any()
                        .downcast_ref::<::arrow::array::Float32Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap())
                        .collect::<Vec<_>>();
                    a.iter().zip(b.iter()).map(|(a, b)| a * b).collect::<Vec<_>>()
                }
                DataType::Float32 => {
                    let a = a
                        .as_any()
                        .downcast_ref::<::arrow::array::Float32Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap())
                        .collect::<Vec<_>>();
                    let b = b
                        .as_any()
                        .downcast_ref::<::arrow::array::Float32Array>()
                        .unwrap()
                        .iter()
                        .map(|v| v.unwrap())
                        .collect::<Vec<_>>();
                    a.iter().zip(b.iter()).map(|(a, b)| a * b).collect::<Vec<_>>()
                }
                _ => panic!("unsupported type"),
            };

            Ok(Arc::new(::arrow::array::Float32Array::from(result)))
        }

        fn signatures() -> Vec<ArrowFunctionSignature> {
            vec![
                ArrowFunctionSignature::exact(vec![DataType::Utf8, DataType::Float32], DataType::Float32),
                ArrowFunctionSignature::exact(vec![DataType::Float32, DataType::Float32], DataType::Float32),
            ]
        }
    }

    #[test]
    fn test_arrow_scalar() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<HelloScalarArrow>("hello")?;

        let batches = conn
            .prepare("select hello('foo') as hello from range(10)")?
            .query_arrow([])?
            .collect::<Vec<_>>();

        for batch in batches.iter() {
            let array = batch.column(0);
            let array = array.as_any().downcast_ref::<::arrow::array::StringArray>().unwrap();
            for i in 0..array.len() {
                assert_eq!(array.value(i), format!("Hello foo"));
            }
        }

        Ok(())
    }

    #[test]
    fn test_arrow_scalar_multiply() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<ArrowMultiplyScalar>("multiply_udf")?;

        let batches = conn
            .prepare("select multiply_udf(3.0, 2.0) as mult_result from range(10)")?
            .query_arrow([])?
            .collect::<Vec<_>>();

        for batch in batches.iter() {
            let array = batch.column(0);
            let array = array.as_any().downcast_ref::<::arrow::array::Float32Array>().unwrap();
            for i in 0..array.len() {
                assert_eq!(array.value(i), 6.0);
            }
        }
        Ok(())
    }

    #[test]
    fn test_multiple_signatures_scalar() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<ArrowOverloaded>("multi_sig_udf")?;

        let batches = conn
            .prepare("select multi_sig_udf('3', 5) as message from range(2)")?
            .query_arrow([])?
            .collect::<Vec<_>>();

        for batch in batches.iter() {
            let array = batch.column(0);
            let array = array.as_any().downcast_ref::<::arrow::array::Float32Array>().unwrap();
            for i in 0..array.len() {
                assert_eq!(array.value(i), 15.0);
            }
        }

        let batches = conn
            .prepare("select multi_sig_udf(12, 10) as message from range(2)")?
            .query_arrow([])?
            .collect::<Vec<_>>();

        for batch in batches.iter() {
            let array = batch.column(0);
            let array = array.as_any().downcast_ref::<::arrow::array::Float32Array>().unwrap();
            for i in 0..array.len() {
                assert_eq!(array.value(i), 120.0);
            }
        }

        Ok(())
    }

    #[test]
    fn test_split_function() -> Result<(), Box<dyn Error>> {
        struct SplitFunction {}

        impl VArrowScalar for SplitFunction {
            type State = ();

            fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
                let strings = input.column(0).as_any().downcast_ref::<StringArray>().unwrap();

                let mut builder = arrow::array::ListBuilder::new(arrow::array::StringBuilder::with_capacity(
                    strings.len(),
                    strings.len() * 10,
                ));

                for s in strings.iter() {
                    let s = s.unwrap();
                    for split_value in s.split(' ').collect::<Vec<_>>() {
                        builder.values().append_value(split_value);
                    }
                    builder.append(true);
                }

                Ok(Arc::new(builder.finish()))
            }

            fn signatures() -> Vec<ArrowFunctionSignature> {
                vec![ArrowFunctionSignature::exact(
                    vec![DataType::Utf8],
                    DataType::List(Arc::new(arrow::datatypes::Field::new("item", DataType::Utf8, true))),
                )]
            }
        }

        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<SplitFunction>("split_string")?;

        // Test with single string
        let batches = conn
            .prepare("select split_string('hello world') as result")?
            .query_arrow([])?
            .collect::<Vec<_>>();

        let array = batches[0].column(0);
        let list_array = array.as_any().downcast_ref::<arrow::array::ListArray>().unwrap();
        let values = list_array.value(0);
        let string_values = values.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(string_values.value(0), "hello");
        assert_eq!(string_values.value(1), "world");

        Ok(())
    }
}
