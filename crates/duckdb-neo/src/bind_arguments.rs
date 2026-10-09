//! Bind-time metadata for scalar, aggregate, and table functions.

use crate::ffi;
use crate::{Result, check_api_call, error::DuckDBError, logical_type::LogicalType, value::Value};

#[derive(Debug, Clone, Copy)]
pub(crate) enum BindType<'a> {
    Scalar(&'a ffi::duckdb_v2_scalar_function_bind_info_handle),
    Table(&'a ffi::duckdb_v2_table_function_bind_info_handle),
    Aggregate(&'a ffi::duckdb_v2_aggregate_function_bind_info_handle),
}

/// The arguments of a scalar or aggregate call site being bound.
///
/// Arguments follow signature-slot order: fixed parameters first, followed by
/// expanded variadic arguments. Constant values are folded only when requested
/// through [`Self::value`], since folding evaluates the argument and can fail.
#[derive(Debug)]
pub struct BindArguments<'a> {
    pub(crate) bind_type: BindType<'a>,
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
        let count = match self.bind_type {
            BindType::Scalar(handle) => {
                check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arg_count, *handle, RET)
            }
            BindType::Aggregate(handle) => {
                check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_arg_count, *handle, RET)
            }
            BindType::Table(handle) => check_api_call!(ffi::duckdb_v2_table_function_bind_get_arg_count, *handle, RET),
        }?;
        Ok(count as usize)
    }

    /// Return whether the call site has no arguments.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the resolved logical type of argument `index`.
    pub fn logical_type(&self, index: usize) -> Result<LogicalType> {
        let index = index as u64;
        let handle = match self.bind_type {
            BindType::Scalar(handle) => {
                check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arg_type, *handle, index, RET)
            }
            BindType::Aggregate(handle) => {
                check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_arg_type, *handle, index, RET)
            }
            BindType::Table(handle) => {
                check_api_call!(ffi::duckdb_v2_table_function_bind_get_arg_type, *handle, index, RET)
            }
        }?;
        Ok(LogicalType { handle })
    }

    /// Fold argument `index` to a constant, or return `None` when it is not constant.
    ///
    /// SQL NULL is represented by `Some` containing a null [`Value`]. Errors raised
    /// while evaluating a constant argument, such as a failing cast, are returned.
    pub fn value(&self, index: usize) -> Result<Option<Value>> {
        let index = index as u64;
        let handle = match self.bind_type {
            BindType::Scalar(handle) => {
                check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arg_value, *handle, index, RET)
            }
            BindType::Aggregate(handle) => {
                check_api_call!(
                    ffi::duckdb_v2_aggregate_function_bind_get_arg_value,
                    *handle,
                    index,
                    RET
                )
            }
            BindType::Table(handle) => {
                check_api_call!(ffi::duckdb_v2_table_function_bind_get_arg_value, *handle, index, RET)
            }
        };
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
