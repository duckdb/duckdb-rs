//! Bind-time metadata for scalar, aggregate, table, and pragma functions.

/// Counts of positional fixed, `*args`, named-only, and `**kwargs` arguments, in that order.
pub use crate::builder_helpers::ArgCount as ArgumentCounts;

use crate::ffi;
use crate::ffi_str::DuckDBStr;
use crate::{Result, check_api_call, error::DuckDBError, logical_type::LogicalType, value::Value};

/// Arguments are ordered as positional fixed parameters, `*args`, named-only parameters, then `**kwargs`.
/// [`Self::value`] folds constant arguments on demand and returns any evaluation errors.
#[derive(Debug)]
pub struct BindArguments<'a> {
    pub(crate) handle: &'a ffi::duckdb_v2_function_bind_info_handle,
}

/// An owned argument type and constant value supplied to a table function's bind callback.
#[derive(Debug)]
pub struct BindArgument {
    /// The argument's resolved logical type.
    pub logical_type: LogicalType,
    /// The constant value, if the argument can be folded at bind time.
    ///
    /// SQL NULL is represented by `Some` containing a null [`Value`], not `None`.
    pub value: Option<Value>,
}

impl<'a> BindArguments<'a> {
    /// Return the number of arguments.
    pub fn len(&self) -> Result<usize> {
        self.counts().map(|counts| counts.total())
    }

    /// Return the sizes of the four consecutive argument-list parts.
    pub fn counts(&self) -> Result<ArgumentCounts> {
        ArgumentCounts::from_func(|pf, pv, nf, nv| {
            check_api_call!(ffi::duckdb_v2_function_bind_get_arg_count, *self.handle, pf, pv, nf, nv)
        })
    }

    /// Return whether the call site has no arguments.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the resolved logical type of argument `index`.
    pub fn logical_type(&self, index: usize) -> Result<LogicalType> {
        check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_type,
            *self.handle,
            index as u64,
            RET
        )
        .map(|handle| LogicalType { handle })
    }

    /// Fold argument `index` to a constant, or return `None` when it is not constant.
    ///
    /// SQL NULL is represented by `Some` containing a null [`Value`]. Errors raised
    /// while evaluating a constant argument, such as a failing cast, are returned.
    pub fn value(&self, index: usize) -> Result<Option<Value>> {
        let handle = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_value,
            *self.handle,
            index as u64,
            RET
        );

        match handle {
            Ok(handle) => Ok(Some(Value { handle })),
            // Not foldable: a non-constant argument, or an unbound prepared parameter.
            Err(e)
                if matches!(
                    e.code,
                    DuckDBError::DUCKDB_V2_ERROR_QUERY_BINDER
                        | DuckDBError::DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Return the declared parameter name, the caller's name for `**kwargs`, or empty for `*args`.
    pub fn name(&self, index: usize) -> Result<String> {
        let str = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_name,
            *self.handle,
            index as u64,
            RET
        )
        .map(DuckDBStr::from_raw)?;

        Ok(str.as_str().unwrap_or_default().to_string())
    }

    /// Find an argument case-insensitively, skipping positional-only parameters.
    /// Return `Ok(None)` if no argument matches the name.
    pub fn index(&self, name: &str) -> Result<Option<usize>> {
        let mut index: ffi::idx_t = 0;
        let mut found = false;
        check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_index,
            *self.handle,
            &name.into(),
            &mut index,
            &mut found
        )?;
        Ok(found.then_some(index as usize))
    }

    /// Collect every argument with its folded value.
    pub(crate) fn collect(&self) -> Result<Vec<BindArgument>> {
        (0..self.len()?)
            .map(|index| {
                Ok(BindArgument {
                    logical_type: self.logical_type(index)?,
                    value: self.value(index)?,
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use crate::{
        Parameters,
        connection::Context,
        environment::{Environment, StorageLocation},
        pragma_function::{PragmaFunctionBuilder, PragmaQueryCallbacks},
        signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder},
    };

    struct CheckCounts;

    impl PragmaQueryCallbacks for CheckCounts {
        fn query(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<String> {
            let counts: ArgumentCounts = arguments.counts()?;
            assert_eq!(counts.positional_fixed, 1);
            assert_eq!(counts.positional_variadic, 2);
            assert_eq!(counts.named_fixed, 1);
            assert_eq!(counts.named_variadic, 1);
            assert_eq!(counts.total(), arguments.len()?);
            Ok("SELECT 42::INTEGER".into())
        }
    }

    #[test]
    fn bind_arguments_counts_preserve_all_four_parts() -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let signature = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("fixed"))
            .param(FunctionParameter::vararg::<i32>("args"))
            .param(FunctionParameter::new::<i32>("named").kind(FunctionParameterKind::Named))
            .param(FunctionParameter::new::<i32>("kwargs").kind(FunctionParameterKind::NamedVariadic));
        PragmaFunctionBuilder::query("check_counts", signature, CheckCounts).register(&conn)?;

        let mut result = conn.query("PRAGMA check_counts(1, 2, 3, named = 4, extra = 5)", Parameters::None)?;
        let chunk = result.next_chunk()?.expect("expected a result chunk");
        assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&42));
        Ok(())
    }
}
