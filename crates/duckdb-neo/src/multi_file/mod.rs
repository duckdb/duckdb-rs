//! Multi-file table functions built on registered single-file readers.
//! The wrapper adds globbing, file lists, Hive partitioning, and `union_by_name`.

use crate::{
    Result,
    error::check_api_call,
    ffi,
    handles::{MultiFileFunctionHandle, MultiFileFunctionLink},
    raw::RawExt,
};

/// Builds and registers a multi-file table function.
///
/// The wrapped single-file function must already be registered. Its only positional
/// parameter must accept a file as `ANY` (a path or file struct) or `VARCHAR` (a path).
/// Named parameters are forwarded unchanged. The reader can report file metadata
/// through [`TableGetBindInfoCallbacks`] and preserve per-file row order across
/// threads through [`TableClaimBatchCallbacks`].
///
/// [`TableGetBindInfoCallbacks`]: crate::table_function::TableGetBindInfoCallbacks
/// [`TableClaimBatchCallbacks`]: crate::table_function::TableClaimBatchCallbacks
#[derive(Debug)]
pub struct MultiFileFunctionBuilder {
    name: String,
    reader_type: Option<String>,
    file_extension: Option<String>,
    function_name: String,
}

impl MultiFileFunctionBuilder {
    /// Create a builder for `name` that wraps the registered table function `function_name`.
    pub fn new(name: impl Into<String>, function_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reader_type: None,
            file_extension: None,
            function_name: function_name.into(),
        }
    }

    /// Set the reader label used in messages, such as `"Avro"`; defaults to the function name.
    pub fn reader_type(mut self, value: impl Into<String>) -> Self {
        self.reader_type = Some(value.into());
        self
    }

    /// Set the file extension to match when reading a directory, without a leading dot.
    pub fn file_extension(mut self, extension: impl Into<String>) -> Self {
        self.file_extension = Some(extension.into());
        self
    }

    fn build(&self, handle: &MultiFileFunctionHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_multi_file_function_set_name,
            handle.raw(),
            &mut (&self.name).into()
        )?;

        if let Some(extension) = self.file_extension.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_multi_file_function_set_file_extension,
                handle.raw(),
                &mut extension.into()
            )?;
        }

        if let Some(reader_type) = self.reader_type.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_multi_file_function_set_reader_type,
                handle.raw(),
                &mut reader_type.into()
            )?;
        }

        check_api_call!(
            ffi::duckdb_v2_multi_file_function_set_single_file_function,
            handle.raw(),
            &mut (&self.function_name).into()
        )
    }

    /// Register the multi-file function through a connection or extension, consuming the builder.
    /// Returns an error if the single-file function is missing or has an unsupported signature.
    #[allow(private_bounds)]
    pub fn register<C: MultiFileFunctionLink>(self, link: &C) -> Result<()> {
        let handle = link.create_multi_file_function_handle()?;

        self.build(&handle)?;

        link.register_multi_file_function_handle(handle)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
