//! Bind-time metadata for scalar, aggregate, table, and pragma functions.

use crate::builder_helpers::ArgCount;
use crate::ffi;
use crate::ffi_str::DuckDBStr;
use crate::{Result, check_api_call, error::DuckDBError, logical_type::LogicalType, value::Value};

/// The arguments of a scalar or aggregate call site or a `PRAGMA` statement being bound.
///
/// Arguments follow signature-slot order: fixed parameters first, followed by
/// expanded variadic arguments. Constant values are folded only when requested
/// through [`Self::value`], since folding evaluates the argument and can fail.
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
        ArgCount::from_func(|pf, pv, nf, nv| {
            check_api_call!(ffi::duckdb_v2_function_bind_get_arg_count, *self.handle, pf, pv, nf, nv)
        })
        .map(|x| x.total())
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

    pub fn name(&self, index: usize) -> Result<String> {
        let str = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_name,
            *self.handle,
            index as u64,
            RET
        )
        .map(DuckDBStr::from_raw)?;

        Ok(str.as_str().unwrap().to_string())
    }

    pub fn index(&self, name: &String) -> Result<Option<usize>> {
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
