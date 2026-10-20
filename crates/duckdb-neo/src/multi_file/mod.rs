use crate::{
    Result,
    error::check_api_call,
    ffi,
    handles::{MultiFileFunctionHandle, MultiFileFunctionLink},
    raw::RawExt,
};

#[derive(Debug)]
pub struct MultiFileFunctionBuilder {
    name: String,
    reader_type: Option<String>,
    file_extension: Option<String>,
    function_name: String,
}

impl MultiFileFunctionBuilder {
    pub fn new(name: impl Into<String>, function_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reader_type: None,
            file_extension: None,
            function_name: function_name.into(),
        }
    }

    pub fn reader_type(mut self, value: impl Into<String>) -> Self {
        self.reader_type = Some(value.into());
        self
    }

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
                &mut (extension).into()
            )?;
        }

        if let Some(reader_type) = self.reader_type.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_multi_file_function_set_reader_type,
                handle.raw(),
                &mut (reader_type).into()
            )?;
        }

        check_api_call!(
            ffi::duckdb_v2_multi_file_function_set_single_file_function,
            handle.raw(),
            &mut (&self.function_name).into()
        )
    }

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
