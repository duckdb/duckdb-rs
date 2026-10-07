//! Bind-time metadata for scalar, aggregate, and table functions.

use crate::ffi;
use crate::{Result, check_api_call, error::DuckDBError, logical_type::LogicalType, value::Value};

pub(crate) enum BindType<'a> {
    Scalar(&'a ffi::duckdb_v2_scalar_function_bind_info_handle),
    Table(&'a ffi::duckdb_v2_table_function_bind_info_handle),
    Aggregate(&'a ffi::duckdb_v2_aggregate_function_bind_info_handle),
}

/// Metadata available while binding a scalar or aggregate function.
///
/// The argument list follows signature-slot order: fixed parameters first,
/// followed by expanded variadic arguments.
pub struct BindMetadata<'a> {
    pub(crate) bind_type: BindType<'a>,
}

/// An owned argument type and optional constant value supplied to a bind callback.
pub struct BindArgument {
    /// The argument's resolved logical type.
    pub logical_type: LogicalType,
    /// The constant value, if the argument can be folded at bind time.
    ///
    /// SQL NULL is represented by `Some` containing a null [`Value`], not `None`.
    pub value: Option<Value>,
}

impl<'a> BindMetadata<'a> {
    pub(crate) fn get_arguments(&self) -> Result<Vec<BindArgument>> {
        match self.bind_type {
            BindType::Aggregate(handle) => self.aggregate(handle),
            BindType::Scalar(handle) => self.scalar(handle),
            BindType::Table(handle) => self.table(handle),
        }
    }

    fn scalar(&self, handle: &ffi::duckdb_v2_scalar_function_bind_info_handle) -> Result<Vec<BindArgument>> {
        let count = check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arg_count, *handle, RET)?;

        let mut bind_views = vec![];

        for i in 0..count {
            let logical_type = LogicalType {
                handle: check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arg_type, *handle, i, RET)?,
            };

            let value_handle = check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arg_value, *handle, i, RET);
            let value = match value_handle {
                Ok(v) => Some(Value { handle: v }),
                Err(e) => {
                    // Not foldable: a non-constant argument, or an unbound prepared parameter.
                    if matches!(
                        e.code,
                        DuckDBError::DUCKDB_V2_ERROR_QUERY_BINDER
                            | DuckDBError::DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED
                    ) {
                        None
                    } else {
                        return Err(e);
                    }
                }
            };

            bind_views.push(BindArgument { logical_type, value });
        }

        Ok(bind_views)
    }

    fn aggregate(&self, handle: &ffi::duckdb_v2_aggregate_function_bind_info_handle) -> Result<Vec<BindArgument>> {
        let count = check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_arg_count, *handle, RET)?;

        let mut bind_views = vec![];

        for i in 0..count {
            let logical_type = LogicalType {
                handle: check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_arg_type, *handle, i, RET)?,
            };
            let value_handle = check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_arg_value, *handle, i, RET);
            let value = match value_handle {
                Ok(v) => Some(Value { handle: v }),
                Err(e) => {
                    // Not foldable: a non-constant argument, or an unbound prepared parameter.
                    if matches!(
                        e.code,
                        DuckDBError::DUCKDB_V2_ERROR_QUERY_BINDER
                            | DuckDBError::DUCKDB_V2_ERROR_QUERY_PARAMETER_NOT_RESOLVED
                    ) {
                        None
                    } else {
                        return Err(e);
                    }
                }
            };

            bind_views.push(BindArgument { logical_type, value });
        }

        Ok(bind_views)
    }

    fn table(&self, handle: &ffi::duckdb_v2_table_function_bind_info_handle) -> Result<Vec<BindArgument>> {
        let count = check_api_call!(ffi::duckdb_v2_table_function_bind_get_arg_count, *handle, RET)?;

        let mut bind_views = vec![];

        for i in 0..count {
            let logical_type = LogicalType {
                handle: check_api_call!(ffi::duckdb_v2_table_function_bind_get_arg_type, *handle, i, RET)?,
            };
            let value = Value {
                handle: check_api_call!(ffi::duckdb_v2_table_function_bind_get_arg_value, *handle, i, RET)?,
            };

            bind_views.push(BindArgument {
                logical_type,
                value: Some(value),
            });
        }

        Ok(bind_views)
    }
}
