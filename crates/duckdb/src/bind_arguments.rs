//! Bind-time metadata for scalar, aggregate, and table functions.

use std::marker::PhantomData;

use libduckdb_sys as ffi;

use crate::{Result, check_api_call, logical_type::LogicalType, value::Value};

/// Metadata available while binding a scalar or aggregate function.
///
/// The argument list follows signature-slot order: fixed parameters first,
/// followed by expanded variadic arguments.
pub struct BindMetadata<'a> {
    /// The registered function name selected for the call.
    pub function_name: String,
    /// The call's resolved arguments.
    pub arguments: BindArguments<'a>,
}

impl<'a> BindMetadata<'a> {
    pub(crate) fn from_table_function(
        handle: &'a ffi::duckdb_v2_table_function_bind_info_handle,
    ) -> Result<BindArguments<'a>> {
        let arguments_handle = check_api_call!(ffi::duckdb_v2_table_function_bind_get_arguments, *handle, RET)?;

        Ok(BindArguments {
            handle: arguments_handle,
            _marker: PhantomData,
        })
    }

    pub(crate) fn from_scalar(handle: &'a ffi::duckdb_v2_scalar_function_bind_info_handle) -> Result<Self> {
        let function_name: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_function_name, *handle, RET)?;

        let str: &str = function_name.into();

        let arguments_handle = check_api_call!(ffi::duckdb_v2_scalar_function_bind_get_arguments, *handle, RET)?;

        Ok(Self {
            function_name: str.to_string(),
            arguments: BindArguments {
                handle: arguments_handle,
                _marker: std::marker::PhantomData,
            },
        })
    }

    pub(crate) fn from_aggregate(handle: &'a ffi::duckdb_v2_aggregate_function_bind_info_handle) -> Result<Self> {
        let function_name: ffi::duckdb_v2_str =
            check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_function_name, *handle, RET)?;

        let str: &str = function_name.into();

        let arguments_handle = check_api_call!(ffi::duckdb_v2_aggregate_function_bind_get_arguments, *handle, RET)?;

        Ok(Self {
            function_name: str.to_string(),
            arguments: BindArguments {
                handle: arguments_handle,
                _marker: std::marker::PhantomData,
            },
        })
    }
}

/// A read-only view of a function's resolved arguments during binding.
///
/// Entries follow signature-slot order and include expanded variadic arguments.
/// The view is borrowed from DuckDB and cannot outlive the bind callback.
pub struct BindArguments<'a> {
    handle: ffi::duckdb_v2_bind_arguments_handle,
    _marker: std::marker::PhantomData<&'a ()>,
}

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
