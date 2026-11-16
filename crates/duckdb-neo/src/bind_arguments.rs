//! Bind-time metadata for scalar, aggregate, table, and pragma functions.

use std::cell::OnceCell;

use duckdb_loadable_macros::AsRaw;

/// Counts of positional fixed, `*args`, named-only, and `**kwargs` arguments, in that order.
pub use crate::builder_helpers::ArgCount as ArgumentCounts;

use crate::ffi_str::DuckDBStr;
use crate::raw::RawExt;
use crate::{Error, FromValue, ffi};
use crate::{Result, check_api_call, error::DuckDBError, logical_type::LogicalType, value::Value};

/// Arguments are ordered as positional fixed parameters, `*args`, named-only parameters, then `**kwargs`.
/// [`Self::value`] folds constant arguments on demand and returns any evaluation errors.
#[derive(Debug, AsRaw)]
pub struct BindArguments<'a> {
    pub(crate) handle: &'a ffi::duckdb_v2_function_bind_info_handle,
    pub(crate) len: OnceCell<usize>,
}

impl<'a> BindArguments<'a> {
    pub(crate) fn new(raw: &'a ffi::duckdb_v2_function_bind_info_handle) -> Self {
        Self {
            handle: raw,
            len: OnceCell::new(),
        }
    }

    /// Return the number of arguments.
    pub fn len(&self) -> Result<usize> {
        if let Some(len) = self.len.get() {
            return Ok(*len);
        }

        let val = self.counts().map(|counts| counts.total())?;

        self.len
            .set(val)
            .map_err(|_| Error::internal("Failed to set OnceCell"))?;

        Ok(val)
    }

    /// Return whether the call site has no arguments.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the sizes of the four consecutive argument-list parts.
    pub fn counts(&self) -> Result<ArgumentCounts> {
        ArgumentCounts::from_func(|pf, pv, nf, nv| {
            check_api_call!(ffi::duckdb_v2_function_bind_get_arg_count, *self.handle, pf, pv, nf, nv)
        })
    }

    pub fn arg(&'a self, index: usize) -> Result<BindArgument<'a>> {
        if index < self.len()? {
            Ok(BindArgument { parent: self, index })
        } else {
            Err(Error::api_error("index is out of bounds for arguments"))
        }
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
}

/// An owned argument type and constant value supplied to a table function's bind callback.
#[derive(Debug)]
pub struct BindArgument<'a> {
    parent: &'a BindArguments<'a>,
    index: usize,
}

impl BindArgument<'_> {
    /// Return the declared parameter name, the caller's name for `**kwargs`, or empty for `*args`.
    pub fn name(&self) -> Result<String> {
        let str = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_name,
            *self.parent.raw(),
            self.index as u64,
            RET
        )
        .map(DuckDBStr::from_raw)?;

        Ok(str.as_str().unwrap_or_default().to_string())
    }

    /// Return the resolved logical type of argument `index`.
    pub fn logical_type(&self) -> Result<LogicalType> {
        check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_type,
            *self.parent.raw(),
            self.index as u64,
            RET
        )
        .map(|handle| LogicalType { handle })
    }

    /// Fold argument `index` to a constant, or return `None` when it is not constant.
    ///
    /// SQL NULL is represented by `Some` containing a null [`Value`]. Errors raised
    /// while evaluating a constant argument, such as a failing cast, are returned.
    pub fn value(&self) -> Result<Option<Value>> {
        let handle = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_value,
            *self.parent.raw(),
            self.index as u64,
            RET
        );

        match handle {
            Ok(handle) => Ok(Some(Value { handle })),
            // Not foldable: a non-constant argument, or an unbound prepared parameter.
            Err(e)
                if matches!(
                    e.code,
                    DuckDBError::QueryBinder | DuckDBError::QueryParameterNotResolved
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Tries to resolve value to T. Err is it does not exist or .
    pub fn resolved_value<T: FromValue>(&self) -> Result<Option<T>> {
        match self.value()? {
            Some(v) => Ok(T::from_value(&v)?),
            None => Err(Error::api_error("Value parameter was not resolved.")),
        }
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
