//! Extensions written against DuckDB's v2 C API.
//!
//! [`Extension`] is what an extension's entrypoint registers its functionality through. The rest of this module is
//! what `#[duckdb_entrypoint_c_api_v2]` expands to, which depends on how the extension is built:
//!
//! - `build-extension`: a loadable extension, a `cdylib` loaded with `LOAD`. Its calls into DuckDB go through the
//!   function table DuckDB hands over on load.
//! - `build-extension-static`: an extension linked into the binary and registered with
//!   `register_static_extension`. Its calls bind to DuckDB at link time.

use crate::{
    AsRaw, Result,
    connection::{Context, FFILink, Factory, HasFactory},
    ffi,
};

#[cfg(all(feature = "build-extension", feature = "build-extension-static"))]
compile_error!("`build-extension` and `build-extension-static` are mutually exclusive");

/// The extension being loaded, lent to its entrypoint for the duration of the load.
#[derive(AsRaw, Debug)]
pub struct Extension {
    pub(crate) handle: ffi::duckdb_v2_extension_handle,
    pub(crate) context: Context,
}

impl HasFactory for Extension {
    fn factory(&self) -> Result<&Factory> {
        self.context.factory()
    }
}

impl FFILink for Extension {}

/// Runs the body of an extension's entrypoint; called by `#[duckdb_entrypoint_c_api_v2]`.
///
/// # Safety
/// `input` must be the pointer DuckDB passed to the `<name>_init_c_api_v2` entrypoint.
#[cfg(any(feature = "build-extension", feature = "build-extension-static"))]
#[doc(hidden)]
pub unsafe fn run_extension_entrypoint(
    input: *const ffi::duckdb_v2_extension_input,
    version: &str,
    body: impl FnOnce(&Extension) -> std::result::Result<(), Box<dyn std::error::Error>>,
) {
    use crate::{FromRaw, error::Error};

    // SAFETY: the caller passes DuckDB's entrypoint input.
    let Some(input) = (unsafe { input.as_ref() }) else {
        return;
    };

    // Without the vtable nothing can be called, not even the error setters; DuckDB reports the refusal itself.
    // SAFETY: `input` comes straight from DuckDB.
    if !unsafe { init_extension_api(input, version) } {
        return;
    }
    let extension = Extension {
        handle: input.extension,
        // SAFETY: the context is borrowed for the duration of the entrypoint, which outlives `extension`.
        context: unsafe { Context::from_raw(input.context) },
    };
    crate::builder_helpers::handle_unwind(
        || {
            body(&extension).map_err(|error| match error.downcast::<Error>() {
                Ok(error) => *error,
                Err(error) => Error::api_error(error.to_string()),
            })
        },
        input.err,
    );
}

#[cfg(feature = "build-extension")]
unsafe fn init_extension_api(input: &ffi::duckdb_v2_extension_input, version: &str) -> bool {
    // SAFETY: the caller passes DuckDB's entrypoint input.
    matches!(
        unsafe { ffi::duckdb_rs_extension_api_v2_init(input, version) },
        Ok(true)
    )
}

#[cfg(feature = "build-extension-static")]
unsafe fn init_extension_api(input: &ffi::duckdb_v2_extension_input, _version: &str) -> bool {
    // Calls bind to DuckDB at link time, so a vtable from a dynamic load is of no use: DuckDB then refuses the load
    // because get_api was never called.
    input.get_api.is_none()
}

/// Link an extension into this binary: every instance opened afterwards loads it at startup, and `LOAD <name>`
/// resolves to it.
///
/// `describe` is the `duckdb_extension_<name>_describe` function that
/// [`#[duckdb_entrypoint_c_api_v2]`](duckdb_loadable_macros::duckdb_entrypoint_c_api_v2) emits. Registering the same
/// function again is a no-op. Instances that are already open are unaffected.
///
/// # Errors
/// When DuckDB refuses the registration. Every instance opened afterwards then fails to open, reporting why.
///
/// # Safety
/// `describe` must fill the descriptor as `duckdb_static_extension.h` specifies, with an entrypoint that is sound for
/// DuckDB to call. The functions `#[duckdb_entrypoint_c_api_v2]` emits are.
#[cfg(feature = "build-extension-static")]
pub unsafe fn register_static_extension(
    describe: unsafe extern "C" fn(*mut ffi::duckdb_extension_descriptor) -> i32,
) -> Result<()> {
    // SAFETY: the caller vouches for `describe`.
    match unsafe { ffi::duckdb_register_static_extension(Some(describe)) } {
        0 => Ok(()),
        _ => Err(crate::Error::invalid_input(
            "DuckDB refused to register the static extension; opening an instance reports why",
        )),
    }
}

/// Fills the descriptor of a statically linked extension; called through `#[duckdb_entrypoint_c_api_v2]`.
///
/// # Safety
/// `descriptor` must be the pointer DuckDB passed to the `duckdb_extension_<name>_describe` function.
#[cfg(feature = "build-extension-static")]
#[doc(hidden)]
pub unsafe fn describe_static_extension(
    descriptor: *mut ffi::duckdb_extension_descriptor,
    name: &'static std::ffi::CStr,
    extension_version: &'static std::ffi::CStr,
    api_version: &'static std::ffi::CStr,
    entrypoint: unsafe extern "C" fn(*mut ffi::duckdb_v2_extension_input),
) -> i32 {
    // SAFETY: the caller passes DuckDB's descriptor.
    let Some(descriptor) = (unsafe { descriptor.as_mut() }) else {
        return 1;
    };
    if descriptor.version < 1 {
        if let Some(set_error) = descriptor.set_error {
            // SAFETY: DuckDB copies the message before returning.
            unsafe { set_error(descriptor, c"the extension needs descriptor layout 1".as_ptr()) };
        }
        return 1;
    }
    descriptor.version = 1;
    descriptor.name = name.as_ptr();
    descriptor.extension_version = extension_version.as_ptr();
    descriptor.api_version = api_version.as_ptr();
    // SAFETY: DuckDB casts the entry back to the entrypoint signature before calling it.
    descriptor.entry_capi_v2 = Some(unsafe {
        std::mem::transmute::<unsafe extern "C" fn(*mut ffi::duckdb_v2_extension_input), unsafe extern "C" fn()>(
            entrypoint,
        )
    });
    0
}

/// Emits `duckdb_extension_<name>_describe` for `#[duckdb_entrypoint_c_api_v2]`.
#[cfg(feature = "build-extension-static")]
#[doc(hidden)]
#[macro_export]
macro_rules! __describe_static_extension {
    ($describe:ident, $entrypoint:ident, $name:literal, $api_version:literal) => {
        /// # Safety
        /// Only DuckDB may call this, with its own descriptor.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $describe(descriptor: *mut $crate::ffi::duckdb_extension_descriptor) -> i32 {
            const fn c_str(bytes: &'static [u8]) -> &'static ::core::ffi::CStr {
                match ::core::ffi::CStr::from_bytes_with_nul(bytes) {
                    ::core::result::Result::Ok(text) => text,
                    ::core::result::Result::Err(_) => panic!("extension metadata must not contain NUL"),
                }
            }
            const NAME: &::core::ffi::CStr = c_str(concat!($name, "\0").as_bytes());
            const EXTENSION_VERSION: &::core::ffi::CStr = c_str(concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes());
            const API_VERSION: &::core::ffi::CStr = c_str(concat!($api_version, "\0").as_bytes());
            // SAFETY: DuckDB passes its own descriptor.
            unsafe {
                $crate::extension::describe_static_extension(
                    descriptor,
                    NAME,
                    EXTENSION_VERSION,
                    API_VERSION,
                    $entrypoint,
                )
            }
        }
    };
}

/// A loadable extension cannot be linked statically: its calls go through the vtable a dynamic load hands over.
#[cfg(feature = "build-extension")]
#[doc(hidden)]
#[macro_export]
macro_rules! __describe_static_extension {
    ($($tokens:tt)*) => {};
}

/// Without either feature there is no way to call DuckDB from an entrypoint.
#[cfg(not(any(feature = "build-extension", feature = "build-extension-static")))]
#[doc(hidden)]
#[macro_export]
macro_rules! __describe_static_extension {
    ($($tokens:tt)*) => {
        compile_error!(
            "`#[duckdb_entrypoint_c_api_v2]` needs duckdb-neo's `build-extension` or `build-extension-static` feature"
        );
    };
}
