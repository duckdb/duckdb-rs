use std::sync::Arc;

use arrow::{
    array::{Array, RecordBatch},
    datatypes::DataType,
};

use crate::{
    core::{DataChunkHandle, LogicalTypeId},
    vtab::arrow::{data_chunk_to_arrow, write_arrow_array_to_vector, WritableVector},
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
            ArrowScalarParams::Exact(params) => params.as_ref(),
            ArrowScalarParams::Variadic(param) => std::slice::from_ref(param),
        }
    }
}

impl From<ArrowScalarParams> for ScalarParams {
    fn from(params: ArrowScalarParams) -> Self {
        match params {
            ArrowScalarParams::Exact(params) => ScalarParams::Exact(
                params
                    .into_iter()
                    .map(|v| LogicalTypeId::try_from(&v).expect("type should be converted").into())
                    .collect(),
            ),
            ArrowScalarParams::Variadic(param) => ScalarParams::Variadic(
                LogicalTypeId::try_from(&param)
                    .expect("type should be converted")
                    .into(),
            ),
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
        ArrowFunctionSignature {
            parameters: Some(ArrowScalarParams::Exact(params)),
            return_type,
        }
    }

    /// Create a variadic function signature
    pub fn variadic(param: DataType, return_type: DataType) -> Self {
        ArrowFunctionSignature {
            parameters: Some(ArrowScalarParams::Variadic(param)),
            return_type,
        }
    }
}

/// A trait for scalar functions that accept and return arrow types that can be registered with DuckDB
pub trait VArrowScalar: Sized {
    /// State that persists across invocations of the scalar function (the lifetime of the connection)
    /// The state can be accessed by multiple threads, so it must be `Send + Sync`.
    type State: Default + Sized + Send + Sync;

    /// The actual function that is called by DuckDB
    fn invoke(info: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>>;

    /// The possible signatures of the scalar function. These will result in DuckDB scalar function overloads.
    /// The invoke method should be able to handle all of these signatures.
    fn signatures() -> Vec<ArrowFunctionSignature>;
}

impl<T> VScalar for T
where
    T: VArrowScalar,
{
    type State = T::State;

    unsafe fn invoke(
        info: &Self::State,
        input: &mut DataChunkHandle,
        out: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let array = T::invoke(info, data_chunk_to_arrow(input)?)?;
        write_arrow_array_to_vector(&array, out)
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        T::signatures()
            .into_iter()
            .map(|sig| ScalarFunctionSignature {
                parameters: sig.parameters.map(Into::into),
                return_type: LogicalTypeId::try_from(&sig.return_type)
                    .expect("type should be converted")
                    .into(),
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
            MockState {
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

        fn invoke(s: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn std::error::Error>> {
            assert_eq!("some meta", s.info);

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
}
