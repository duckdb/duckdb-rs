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
                    if e.code == DuckDBError::DUCKDB_V2_ERROR_QUERY_BINDER {
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
                    if e.code == DuckDBError::DUCKDB_V2_ERROR_QUERY_BINDER {
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

#[cfg(feature = "capi-v2-p4")]
/// A read-only view of a function's resolved arguments during binding.
///
/// Entries follow signature-slot order and include expanded variadic arguments.
/// The view is borrowed from DuckDB and cannot outlive the bind callback.
pub struct BindArguments<'a> {
    handle: ffi::duckdb_v2_bind_arguments_handle,
    _marker: std::marker::PhantomData<&'a ()>,
}

#[cfg(feature = "capi-v2-p4")]
impl<'a> BindArguments<'a> {
    /// Return an owned copy of the resolved type at `index`.
    ///
    /// An out-of-range index returns an error.
    pub fn logical_type(&self, index: usize) -> Result<LogicalType> {
        let handle = check_api_call!(ffi::duckdb_v2_bind_arguments_get_type, self.handle, index as u64, RET)?;

        Ok(LogicalType { handle })
    }

    /// Return the resolved slot name at `index`.
    ///
    /// Unnamed variadic arguments have an empty name.
    /// An out-of-range index returns an error.
    pub fn name(&self, index: usize) -> Result<String> {
        let name: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_bind_arguments_get_name, self.handle, index as u64, RET)?;

        let str: &str = name.into();

        Ok(str.to_string())
    }

    /// Return all resolved slot names in signature order.
    pub fn names(&self) -> Result<Vec<String>> {
        let count = self.len()?;

        let mut names = Vec::with_capacity(count);

        for i in 0..count {
            names.push(self.name(i)?);
        }

        Ok(names)
    }

    /// Evaluate the argument at `index` to an owned constant value.
    ///
    /// Returns an error when the argument is not constant-foldable or the
    /// index is out of range.
    pub fn fold(&self, index: usize, ctx: &crate::Context) -> Result<Value> {
        let value = check_api_call!(
            ffi::duckdb_v2_bind_arguments_fold,
            self.handle,
            **ctx,
            index as u64,
            RET
        )?;

        Ok(Value { handle: value })
    }

    /// Return whether the call has no bound arguments.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the number of bound arguments, including variadic arguments.
    pub fn len(&self) -> Result<usize> {
        let count: u64 = check_api_call!(ffi::duckdb_v2_bind_arguments_get_count, self.handle, RET)?;

        Ok(count as usize)
    }
}
