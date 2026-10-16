use duckdb_loadable_macros::AsRaw;

use crate::builder_helpers::{OpaqueHandle, handle_unwind};
use crate::ffi_str::DuckDBStr;
use crate::log::{LogLevel, LogScope};
use crate::raw::RawExt;
use crate::{
    Result,
    error::check_api_call,
    ffi,
    handles::{LogSinkBuilderHandle, LogSinkBuilderLink},
};

#[derive(Debug, AsRaw)]
pub struct LogMetadata {
    handle: ffi::duckdb_v2_log_sink_write_info_handle,
}

impl LogMetadata {
    pub fn connection_id(&self) -> Result<Option<u64>> {
        let mut is_present = false;
        let value = check_api_call!(
            ffi::duckdb_v2_log_sink_write_info_get_connection_id,
            self.raw(),
            RET,
            &mut is_present
        )?;

        if is_present { Ok(Some(value)) } else { Ok(None) }
    }

    pub fn query_id(&self) -> Result<Option<u64>> {
        let mut is_present = false;
        let value = check_api_call!(
            ffi::duckdb_v2_log_sink_write_info_get_query_id,
            self.raw(),
            RET,
            &mut is_present
        )?;

        if is_present { Ok(Some(value)) } else { Ok(None) }
    }

    pub fn thread_id(&self) -> Result<Option<u64>> {
        let mut is_present = false;
        let value = check_api_call!(
            ffi::duckdb_v2_log_sink_write_info_get_thread_id,
            self.raw(),
            RET,
            &mut is_present
        )?;

        if is_present { Ok(Some(value)) } else { Ok(None) }
    }

    pub fn transaction_id(&self) -> Result<Option<u64>> {
        let mut is_present = false;
        let value = check_api_call!(
            ffi::duckdb_v2_log_sink_write_info_get_transaction_id,
            self.raw(),
            RET,
            &mut is_present
        )?;

        if is_present { Ok(Some(value)) } else { Ok(None) }
    }
}

#[derive(Debug)]
pub struct LogWriteData {
    pub timestamp: i64,
    pub log_level: LogLevel,
    pub log_scope: LogScope,
    pub log_type: Option<String>,
    pub message: Option<String>,
}

unsafe extern "C" fn flush_callback<T: LogSinkCallbacks>(
    _info: ffi::duckdb_v2_log_sink_flush_info_handle,
    user_data: *mut ::std::os::raw::c_void,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = unsafe { &*(user_data as *const T) };

            T::flush(user_data)
        },
        err,
    );
}

unsafe extern "C" fn write_callback<T: LogSinkCallbacks>(
    info: ffi::duckdb_v2_log_sink_write_info_handle,
    user_data: *mut ::std::os::raw::c_void,
    timestamp: i64,
    level: ffi::DUCKDB_V2_LOG_LEVEL,
    scope: ffi::DUCKDB_V2_LOG_SCOPE,
    type_: *const ffi::duckdb_v2_str,
    message: *const ffi::duckdb_v2_str,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = unsafe { &*(user_data as *const T) };

            let log_type = if type_.is_null() {
                None
            } else {
                DuckDBStr::from_raw(unsafe { *type_ }).as_str().map(|x| x.to_string())
            };

            let message = if message.is_null() {
                None
            } else {
                DuckDBStr::from_raw(unsafe { *message }).as_str().map(|x| x.to_string())
            };

            T::write(
                user_data,
                LogWriteData {
                    timestamp,
                    log_level: level.try_into()?,
                    log_scope: scope.try_into()?,
                    log_type,
                    message,
                },
                LogMetadata { handle: info },
            )
        },
        err,
    );
}

#[derive(Debug)]
pub struct LogSinkBuilder<T: LogSinkCallbacks> {
    name: String,
    user_data: OpaqueHandle<T>,
}

impl<T: LogSinkCallbacks> LogSinkBuilder<T> {
    pub fn new(name: impl Into<String>, implementation: T) -> Self {
        LogSinkBuilder {
            name: name.into(),
            user_data: OpaqueHandle::new(implementation),
        }
    }

    fn build(&self, handle: &LogSinkBuilderHandle) -> Result<()> {
        check_api_call!(ffi::duckdb_v2_log_sink_set_name, handle.raw(), &(&self.name).into())?;

        check_api_call!(
            ffi::duckdb_v2_log_sink_set_user_data,
            handle.raw(),
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_log_sink_set_flush_callback,
            handle.raw(),
            Some(flush_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_log_sink_set_write_callback,
            handle.raw(),
            Some(write_callback::<T>)
        )
    }

    #[allow(private_bounds)] // Keep the handle factory private.
    pub fn register<C: LogSinkBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_log_sink_handle()?;
        self.build(&handle)?;
        link.register_log_sink(handle)
    }
}

pub trait LogSinkCallbacks: Send + Sync + 'static {
    fn flush(&self) -> Result<()>;

    fn write(&self, data: LogWriteData, metadata: LogMetadata) -> Result<()>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
