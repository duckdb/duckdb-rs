use std::marker::PhantomData;

use super::{ScalarCallbacks, ScalarFunctionBuilder};
use crate::{
    DuckDBType, Result,
    connection::Context,
    data_chunk::VectorCollection,
    signature::{FunctionParameter, SignatureBuilder},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Creates unary scalar functions from typed vector closures.
///
/// The input and output element types determine the SQL signature. The closure
/// receives a batch and an output vector already sized to its row count, and
/// must write every output row. DuckDB's default NULL handling applies unless
/// overridden with [`ScalarFunctionBuilder::set_property`].
///
/// Closures may capture owned `Send + Sync` state; binding and worker-local
/// initialization remain available through [`ScalarCallbacks`] instead.
///
/// ```
/// use duckdb_neo::{
///     Parameters,
///     environment::{Environment, StorageLocation},
///     scalar::MicroScalar,
///     vector::Vector,
/// };
///
/// # fn main() -> duckdb_neo::Result<()> {
/// let function = MicroScalar::create(
///     "plus_one",
///     |_ctx, input: Vector<'_, i32>, mut output: Vector<'_, i64>| {
///         let mut writer = output.writer()?;
///         for (row, value) in input.reader()?.iter().enumerate() {
///             writer.set(row, value.map(|value| i64::from(*value) + 1))?;
///         }
///         Ok(())
///     },
/// );
/// let env = Environment::new()?;
/// let conn = env.open(StorageLocation::InMemory)?.connect()?;
/// function.register(&conn)?;
///
/// let chunk = conn.query("SELECT plus_one(41)", Parameters::None)?
///     .next_chunk()?.unwrap();
/// assert_eq!(chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?, Some(&42));
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct MicroScalar;

impl MicroScalar {
    /// Infer a unary signature and wrap the closure in a scalar function builder.
    pub fn create<I, O, F>(name: impl Into<String>, function: F) -> ScalarFunctionBuilder<impl ScalarCallbacks>
    where
        I: DuckDBType + VectorElement + 'static,
        O: DuckDBType + WritableVectorElement + 'static,
        F: Fn(&Context, Vector<'_, I>, Vector<'_, O>) -> Result<()> + Send + Sync + 'static,
    {
        ScalarFunctionBuilder::new(
            name,
            SignatureBuilder::new()
                .param(FunctionParameter::new::<I>("input"))
                .return_type_of::<O>(),
            TypedCallback::<I, O, F> {
                function,
                types: PhantomData,
            },
        )
    }
}

struct TypedCallback<I, O, F> {
    function: F,
    types: PhantomData<fn(I) -> O>,
}

impl<I, O, F> ScalarCallbacks for TypedCallback<I, O, F>
where
    I: VectorElement + 'static,
    O: WritableVectorElement + 'static,
    F: Fn(&Context, Vector<'_, I>, Vector<'_, O>) -> Result<()> + Send + Sync + 'static,
{
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut output = output.cast::<O>()?;
        output.set_size(input.row_count()?)?;
        (self.function)(context, input.get_vector_at_mut::<I>(0)?, output)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
