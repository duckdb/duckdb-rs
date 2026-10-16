use crate::{
    FromRaw, Result,
    builder_helpers::{OpaqueHandle, get_user_data, handle_unwind},
    connection::{Context, SettingScope},
    error::check_api_call,
    ffi,
    handles::{CustomOptionBuilderHandle, CustomOptionBuilderLink},
    logical_type::LogicalType,
    raw::RawExt,
    value::Value,
};

unsafe extern "C" fn update_callback<T: CustomOptionCallbacks>(
    info: ffi::duckdb_v2_custom_option_update_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_custom_option_update_get_user_data, info);

            let scope = check_api_call!(ffi::duckdb_v2_custom_option_update_get_scope, info, RET)?;

            let value = check_api_call!(ffi::duckdb_v2_custom_option_update_get_value, info, RET)
                .map(|handle| Value { handle })?;

            let is_reset = check_api_call!(ffi::duckdb_v2_custom_option_update_is_reset, info, RET)?;

            let result = T::update(
                user_data,
                &unsafe { Context::from_raw(context) },
                value,
                scope.try_into()?,
                is_reset,
            )?;
            check_api_call!(ffi::duckdb_v2_custom_option_update_set_value, info, result.raw())
        },
        err,
    );
}

#[derive(Debug)]
pub struct CustomOptionBuilder<T: CustomOptionCallbacks> {
    name: String,
    logical_type: LogicalType,
    default_scope: Option<SettingScope>,
    default_value: Option<Value>,
    description: String,
    user_data: OpaqueHandle<T>,
    no_global: bool,
    no_session: bool,
}

impl<T: CustomOptionCallbacks> CustomOptionBuilder<T> {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        logical_type: LogicalType,
        implementation: T,
    ) -> Self {
        CustomOptionBuilder {
            name: name.into(),
            description: description.into(),
            logical_type,
            default_scope: None,
            default_value: None,
            user_data: OpaqueHandle::new(implementation),
            no_global: false,
            no_session: false,
        }
    }

    pub fn no_global(mut self) -> Self {
        self.no_global = true;
        self
    }

    pub fn no_session(mut self) -> Self {
        self.no_session = true;
        self
    }

    pub fn default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn default_scope(mut self, scope: SettingScope) -> Self {
        self.default_scope = Some(scope);
        self
    }

    fn build(&self, handle: &CustomOptionBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_custom_option_set_name,
            handle.raw(),
            &(&self.name).into()
        )?;

        check_api_call!(
            ffi::duckdb_v2_custom_option_set_type,
            handle.raw(),
            self.logical_type.raw()
        )?;

        check_api_call!(
            ffi::duckdb_v2_custom_option_set_user_data,
            handle.raw(),
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_custom_option_set_description,
            handle.raw(),
            &(&self.description).into()
        )?;

        if let Some(default_scope) = self.default_scope.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_custom_option_set_default_scope,
                handle.raw(),
                default_scope.clone().into()
            )?;
        }
        if let Some(default_value) = self.default_value.as_ref() {
            check_api_call!(
                ffi::duckdb_v2_custom_option_set_default_value,
                handle.raw(),
                default_value.raw()
            )?;
        }

        check_api_call!(
            ffi::duckdb_v2_custom_option_set_supports_scope,
            handle.raw(),
            SettingScope::Global.into(),
            !self.no_global
        )?;

        check_api_call!(
            ffi::duckdb_v2_custom_option_set_supports_scope,
            handle.raw(),
            SettingScope::Session.into(),
            !self.no_session
        )?;

        check_api_call!(
            ffi::duckdb_v2_custom_option_set_update_callback,
            handle.raw(),
            Some(update_callback::<T>)
        )
    }

    #[allow(private_bounds)]
    pub fn register<C: CustomOptionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_custom_option_handle()?;
        self.build(&handle)?;
        link.register_custom_option_handle(handle)
    }
}

pub trait CustomOptionCallbacks: Send + Sync + 'static {
    fn update(&self, context: &Context, value: Value, scope: SettingScope, is_reset: bool) -> Result<Value>;
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
