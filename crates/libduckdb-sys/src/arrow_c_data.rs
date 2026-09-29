//! Arrow C Data Interface layouts used by DuckDB's Arrow conversion APIs.
//!
//! DuckDB's public C header forward-declares these structs. The conversion
//! APIs need concrete caller-allocated layouts, so `libduckdb-sys` defines the
//! ABI records directly without taking a dependency on arrow-rs.
//!
//! Specification: <https://arrow.apache.org/docs/format/CDataInterface.html>

#[cfg(any(test, feature = "capi-v2"))]
use std::ffi::c_int;
use std::{
    ffi::{c_char, c_void},
    ptr,
};

/// Arrow C Data Interface array.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArray)>,
    pub private_data: *mut c_void,
}

impl ArrowArray {
    /// Creates a null-release placeholder for a producer to fill.
    pub const fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: ptr::null_mut(),
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

/// Arrow C Data Interface schema.
#[repr(C)]
#[derive(Debug)]
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: Option<unsafe extern "C" fn(*mut ArrowSchema)>,
    pub private_data: *mut c_void,
}

impl ArrowSchema {
    /// Creates a null-release placeholder for a producer to fill.
    pub const fn empty() -> Self {
        Self {
            format: ptr::null(),
            name: ptr::null(),
            metadata: ptr::null(),
            flags: 0,
            n_children: 0,
            children: ptr::null_mut(),
            dictionary: ptr::null_mut(),
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

/// Arrow C stream interface. Only the v2 API exposes it; tests keep it for the layout check.
#[cfg(any(test, feature = "capi-v2"))]
#[repr(C)]
#[derive(Debug)]
pub struct ArrowArrayStream {
    pub get_schema: Option<unsafe extern "C" fn(*mut ArrowArrayStream, *mut ArrowSchema) -> c_int>,
    pub get_next: Option<unsafe extern "C" fn(*mut ArrowArrayStream, *mut ArrowArray) -> c_int>,
    pub get_last_error: Option<unsafe extern "C" fn(*mut ArrowArrayStream) -> *const c_char>,
    pub release: Option<unsafe extern "C" fn(*mut ArrowArrayStream)>,
    pub private_data: *mut c_void,
}

#[cfg(feature = "capi-v2")]
impl ArrowArrayStream {
    /// Creates a null-release placeholder for a producer to fill.
    pub const fn empty() -> Self {
        Self {
            get_schema: None,
            get_next: None,
            get_last_error: None,
            release: None,
            private_data: ptr::null_mut(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, offset_of, size_of};

    use arrow::{
        ffi::{FFI_ArrowArray, FFI_ArrowSchema},
        ffi_stream::FFI_ArrowArrayStream,
    };

    macro_rules! assert_offsets {
        ($left:ty, $right:ty, $($field:ident),+ $(,)?) => {
            $(
                assert_eq!(
                    offset_of!($left, $field),
                    offset_of!($right, $field),
                    concat!("field offset differs for ", stringify!($field))
                );
            )+
        };
    }

    #[test]
    fn matches_arrow_rs_layouts() {
        assert_eq!(size_of::<ArrowArray>(), size_of::<FFI_ArrowArray>());
        assert_eq!(align_of::<ArrowArray>(), align_of::<FFI_ArrowArray>());
        assert_offsets!(
            ArrowArray,
            FFI_ArrowArray,
            length,
            null_count,
            offset,
            n_buffers,
            n_children,
            buffers,
            children,
            dictionary,
            release,
            private_data,
        );

        assert_eq!(size_of::<ArrowSchema>(), size_of::<FFI_ArrowSchema>());
        assert_eq!(align_of::<ArrowSchema>(), align_of::<FFI_ArrowSchema>());
        assert_offsets!(
            ArrowSchema,
            FFI_ArrowSchema,
            format,
            name,
            metadata,
            flags,
            n_children,
            children,
            dictionary,
            release,
            private_data,
        );

        assert_eq!(size_of::<ArrowArrayStream>(), size_of::<FFI_ArrowArrayStream>());
        assert_eq!(align_of::<ArrowArrayStream>(), align_of::<FFI_ArrowArrayStream>());
        assert_offsets!(
            ArrowArrayStream,
            FFI_ArrowArrayStream,
            get_schema,
            get_next,
            get_last_error,
            release,
            private_data,
        );
    }
}
