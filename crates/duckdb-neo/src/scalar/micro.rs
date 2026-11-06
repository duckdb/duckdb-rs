use std::marker::PhantomData;

use super::{ScalarCallbacks, ScalarFunctionBuilder};
use crate::{
    DuckDBType, Result,
    connection::Context,
    data_chunk::VectorCollection,
    signature::{FunctionParameter, SignatureBuilder},
    vector::{Unknown, Vector, VectorElement, WritableVectorElement},
};

/// Creates scalar functions from vector closures.
///
/// [`Self::create`] infers a unary signature from the input and output element
/// types. [`Self::with_signature`] accepts an explicit signature and an input
/// collection for multiple arguments, varargs, or runtime logical types.
/// The output vector is already sized to the batch's row count; the closure
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
        Self::with_signature(
            name,
            SignatureBuilder::new()
                .param(FunctionParameter::new::<I>("input"))
                .return_type_of::<O>(),
            move |context, input, output| function(context, input.get_vector_at_mut::<I>(0)?, output),
        )
    }

    /// Wrap a closure with an explicit signature and checked output element type.
    /// Arguments follow the ordering of [`SignatureBuilder::param`].
    pub fn with_signature<O, F>(
        name: impl Into<String>,
        signature: SignatureBuilder,
        function: F,
    ) -> ScalarFunctionBuilder<impl ScalarCallbacks>
    where
        O: WritableVectorElement + 'static,
        F: Fn(&Context, &mut VectorCollection, Vector<'_, O>) -> Result<()> + Send + Sync + 'static,
    {
        ScalarFunctionBuilder::new(
            name,
            signature,
            TypedCallback::<O, F> {
                function,
                types: PhantomData,
            },
        )
    }
}

struct TypedCallback<O, F> {
    function: F,
    types: PhantomData<fn() -> O>,
}

impl<O, F> ScalarCallbacks for TypedCallback<O, F>
where
    O: WritableVectorElement + 'static,
    F: Fn(&Context, &mut VectorCollection, Vector<'_, O>) -> Result<()> + Send + Sync + 'static,
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
        (self.function)(context, input, output)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
