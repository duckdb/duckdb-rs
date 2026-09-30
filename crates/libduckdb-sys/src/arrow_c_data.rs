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
    use std::{
        collections::HashMap,
        ffi::CStr,
        mem::{align_of, size_of},
        sync::Arc,
    };

    use arrow::{
        array::{Array, DictionaryArray, Int32Array, StructArray},
        datatypes::{DataType, Field, Fields, Int8Type},
        ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    };

    // arrow-rs keeps the FFI fields private, so compare layouts by reading a
    // populated arrow-rs struct through ours and checking every field against
    // arrow-rs's accessors.
    #[test]
    fn matches_arrow_rs_array_layout() {
        assert_eq!(size_of::<ArrowArray>(), size_of::<FFI_ArrowArray>());
        assert_eq!(align_of::<ArrowArray>(), align_of::<FFI_ArrowArray>());

        // Distinct values: length 6, null_count 3, offset 1, n_buffers 2, n_children 0.
        let ints = Int32Array::from(vec![Some(0), None, None, None, Some(4), Some(5), Some(6)]).slice(1, 6);
        let ffi = FFI_ArrowArray::new(&ints.to_data());
        let ours = unsafe { &*(&ffi as *const FFI_ArrowArray as *const ArrowArray) };
        assert_eq!(ours.length as usize, ffi.len());
        assert_eq!(ours.null_count as usize, ffi.null_count());
        assert_eq!(ours.offset as usize, ffi.offset());
        assert_eq!(ours.n_buffers as usize, ffi.num_buffers());
        assert_eq!(ours.n_children as usize, ffi.num_children());
        for i in 0..ffi.num_buffers() {
            assert_eq!(unsafe { *ours.buffers.add(i) } as *const u8, ffi.buffer(i));
        }
        assert_eq!(ours.release.map(|f| f as usize), ffi.release().map(|f| f as usize));
        assert_eq!(ours.private_data, ffi.private_data());

        let dict: DictionaryArray<Int8Type> = ["a", "b", "a"].into_iter().collect();
        let nested = StructArray::from(vec![(
            Arc::new(Field::new("d", dict.data_type().clone(), false)),
            Arc::new(dict) as _,
        )]);
        let ffi = FFI_ArrowArray::new(&nested.to_data());
        let ours = unsafe { &*(&ffi as *const FFI_ArrowArray as *const ArrowArray) };
        assert_eq!(ours.n_children, 1);
        let child = unsafe { &**ours.children };
        assert!(ptr::eq(
            child as *const ArrowArray as *const FFI_ArrowArray,
            ffi.child(0)
        ));
        let dictionary = ffi.child(0).dictionary().unwrap();
        assert!(ptr::eq(child.dictionary as *const FFI_ArrowArray, dictionary));
    }

    #[test]
    fn matches_arrow_rs_schema_layout() {
        let dict = Field::new_dictionary("d", DataType::Int8, DataType::Utf8, false);
        let field = Field::new("s", DataType::Struct(Fields::from(vec![dict])), true)
            .with_metadata(HashMap::from([("k".to_string(), "v".to_string())]));

        let ffi = FFI_ArrowSchema::try_from(&field).unwrap();

        let ours = unsafe { &*(&ffi as *const FFI_ArrowSchema as *const ArrowSchema) };
        assert_eq!(unsafe { CStr::from_ptr(ours.format) }.to_str().unwrap(), ffi.format());
        assert_eq!(unsafe { CStr::from_ptr(ours.name) }.to_str().ok(), ffi.name());
        assert!(!ours.metadata.is_null());
        assert_eq!(ours.flags, ffi.flags().unwrap().bits());
        assert_eq!(ours.n_children as usize, ffi.children().count());

        let child = unsafe { &**ours.children };
        assert!(ptr::eq(
            child as *const ArrowSchema as *const FFI_ArrowSchema,
            ffi.child(0)
        ));

        let dictionary = ffi.child(0).dictionary().unwrap();
        assert!(ptr::eq(child.dictionary as *const FFI_ArrowSchema, dictionary));
        assert_eq!(ours.release.map(|f| f as usize), ffi.release().map(|f| f as usize));
        assert_eq!(ours.private_data, ffi.private_data());
    }
}
