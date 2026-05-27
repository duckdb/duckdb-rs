//! Dependency-free Arrow C Data Interface layouts.
//!
//! DuckDB's public C header only forward-declares `struct ArrowArray` and
//! `struct ArrowSchema`, so bindgen emits opaque zero-sized Rust structs by
//! default. That is enough for APIs that pass opaque pointers around, but it is
//! not enough for the newer DuckDB Arrow conversion APIs where the caller
//! allocates these structs and DuckDB fills in the Arrow C Data Interface
//! fields.
//!
//! These definitions intentionally model only the stable C ABI from the Arrow C
//! Data Interface. They do not depend on arrow-rs, which keeps
//! `libduckdb-sys` from choosing one arrow-rs major version for every
//! downstream crate. Higher-level crates can bridge these structs to an
//! arrow-rs FFI type that follows the same Arrow C Data Interface layout. The
//! compile-time guards below enforce this crate's spec layout, and tests compare
//! it with this crate's dev-dependency arrow-rs version; adapters that target
//! another arrow-rs version should keep the same by-value layout check.
//!
//! Use `ptr::read` or an equivalent by-value transmute for that bridge: the
//! struct bytes are copied into the target FFI type, the underlying Arrow
//! buffers are not copied, and the source struct must be treated as moved-from
//! so its `release` callback is not invoked again. If the source value remains
//! in scope after the bridge, clear that callback with `release.take()`.
//!
//! The structs also intentionally do not implement `Drop`, `Copy`, or `Clone`.
//! Before a producer fills them, `release` is `None` and there is nothing to
//! free. After a producer fills them, the current holder owns the release
//! callback and must arrange for it to be called at most once.
//! This crate also does not add unsafe `Send` or `Sync` impls; downstream
//! wrappers that own a stronger thread-safety invariant can make that decision.
//!
//! Specification: <https://arrow.apache.org/docs/format/CDataInterface.html>

use std::{
    ffi::{c_char, c_void},
    mem::{align_of, offset_of, size_of},
    ptr,
};

/// Release callback stored in an [`ArrowArray`].
///
/// The producer owns the callback implementation. Consumers must call it at
/// most once, and only when they own the C Data Interface object.
pub type ArrowArrayReleaseCallback = Option<unsafe extern "C" fn(*mut ArrowArray)>;

/// Release callback stored in an [`ArrowSchema`].
///
/// The producer owns the callback implementation. Consumers must call it at
/// most once, and only when they own the C Data Interface object.
pub type ArrowSchemaReleaseCallback = Option<unsafe extern "C" fn(*mut ArrowSchema)>;

/// Arrow C Data Interface array.
#[repr(C)]
#[derive(Debug)]
// DO NOT REORDER: this is the Arrow C Data Interface ABI layout.
pub struct ArrowArray {
    pub length: i64,
    pub null_count: i64,
    pub offset: i64,
    pub n_buffers: i64,
    pub n_children: i64,
    pub buffers: *mut *const c_void,
    pub children: *mut *mut ArrowArray,
    pub dictionary: *mut ArrowArray,
    pub release: ArrowArrayReleaseCallback,
    pub private_data: *mut c_void,
}

impl ArrowArray {
    /// Creates an empty placeholder for a producer to overwrite.
    ///
    /// This is not a valid zero-length Arrow array. It is only a null-release
    /// destination struct to pass to a producer such as DuckDB.
    pub fn empty() -> Self {
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

const _: () = assert_arrow_array_layout();

const _: () = assert_arrow_schema_layout();

/// Build-time ABI guard for normal downstream builds.
///
/// This is intentionally not just a unit test: callers can use these structs in
/// release builds without enabling this crate's test-only arrow-rs dependency,
/// so field drift must fail compilation even when tests are not run.
const fn assert_arrow_array_layout() {
    let length = 0;
    let null_count = length + size_of::<i64>();
    let offset = null_count + size_of::<i64>();
    let n_buffers = offset + size_of::<i64>();
    let n_children = n_buffers + size_of::<i64>();
    let buffers = (n_children + size_of::<i64>()).next_multiple_of(align_of::<*mut *const c_void>());
    let children = (buffers + size_of::<*mut *const c_void>()).next_multiple_of(align_of::<*mut *mut ArrowArray>());
    let dictionary = (children + size_of::<*mut *mut ArrowArray>()).next_multiple_of(align_of::<*mut ArrowArray>());
    let release = (dictionary + size_of::<*mut ArrowArray>()).next_multiple_of(align_of::<ArrowArrayReleaseCallback>());
    let private_data = (release + size_of::<ArrowArrayReleaseCallback>()).next_multiple_of(align_of::<*mut c_void>());
    let size = (private_data + size_of::<*mut c_void>()).next_multiple_of(align_of::<ArrowArray>());

    assert!(size_of::<ArrowArrayReleaseCallback>() == size_of::<*mut c_void>());
    assert!(offset_of!(ArrowArray, length) == length);
    assert!(offset_of!(ArrowArray, null_count) == null_count);
    assert!(offset_of!(ArrowArray, offset) == offset);
    assert!(offset_of!(ArrowArray, n_buffers) == n_buffers);
    assert!(offset_of!(ArrowArray, n_children) == n_children);
    assert!(offset_of!(ArrowArray, buffers) == buffers);
    assert!(offset_of!(ArrowArray, children) == children);
    assert!(offset_of!(ArrowArray, dictionary) == dictionary);
    assert!(offset_of!(ArrowArray, release) == release);
    assert!(offset_of!(ArrowArray, private_data) == private_data);
    assert!(size_of::<ArrowArray>() == size);
}

/// Arrow C Data Interface schema.
#[repr(C)]
#[derive(Debug)]
// DO NOT REORDER: this is the Arrow C Data Interface ABI layout.
pub struct ArrowSchema {
    pub format: *const c_char,
    pub name: *const c_char,
    pub metadata: *const c_char,
    pub flags: i64,
    pub n_children: i64,
    pub children: *mut *mut ArrowSchema,
    pub dictionary: *mut ArrowSchema,
    pub release: ArrowSchemaReleaseCallback,
    pub private_data: *mut c_void,
}

impl ArrowSchema {
    /// Creates an empty placeholder for a producer to overwrite.
    ///
    /// This is not a valid Arrow schema. It is only a null-release destination
    /// struct to pass to a producer such as DuckDB.
    pub fn empty() -> Self {
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

/// Build-time ABI guard for normal downstream builds.
///
/// This mirrors [`assert_arrow_array_layout`] for `ArrowSchema`; the arrow-rs
/// tests below compare against one concrete arrow-rs version, while this check
/// keeps the dependency-free C ABI layout fixed in every build.
const fn assert_arrow_schema_layout() {
    let format = 0;
    let name = format + size_of::<*const c_char>();
    let metadata = name + size_of::<*const c_char>();
    let flags = (metadata + size_of::<*const c_char>()).next_multiple_of(align_of::<i64>());
    let n_children = flags + size_of::<i64>();
    let children = (n_children + size_of::<i64>()).next_multiple_of(align_of::<*mut *mut ArrowSchema>());
    let dictionary = (children + size_of::<*mut *mut ArrowSchema>()).next_multiple_of(align_of::<*mut ArrowSchema>());
    let release =
        (dictionary + size_of::<*mut ArrowSchema>()).next_multiple_of(align_of::<ArrowSchemaReleaseCallback>());
    let private_data = (release + size_of::<ArrowSchemaReleaseCallback>()).next_multiple_of(align_of::<*mut c_void>());
    let size = (private_data + size_of::<*mut c_void>()).next_multiple_of(align_of::<ArrowSchema>());

    assert!(size_of::<ArrowSchemaReleaseCallback>() == size_of::<*mut c_void>());
    assert!(offset_of!(ArrowSchema, format) == format);
    assert!(offset_of!(ArrowSchema, name) == name);
    assert!(offset_of!(ArrowSchema, metadata) == metadata);
    assert!(offset_of!(ArrowSchema, flags) == flags);
    assert!(offset_of!(ArrowSchema, n_children) == n_children);
    assert!(offset_of!(ArrowSchema, children) == children);
    assert!(offset_of!(ArrowSchema, dictionary) == dictionary);
    assert!(offset_of!(ArrowSchema, release) == release);
    assert!(offset_of!(ArrowSchema, private_data) == private_data);
    assert!(size_of::<ArrowSchema>() == size);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ptr,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

    static ARRAY_RELEASES: AtomicUsize = AtomicUsize::new(0);
    static SCHEMA_RELEASES: AtomicUsize = AtomicUsize::new(0);

    unsafe extern "C" fn release_array(array: *mut ArrowArray) {
        assert!(!array.is_null());
        let array = unsafe { &mut *array };
        assert!(array.release.is_some());
        array.release = None;
        ARRAY_RELEASES.fetch_add(1, Ordering::SeqCst);
    }

    unsafe extern "C" fn release_schema(schema: *mut ArrowSchema) {
        assert!(!schema.is_null());
        let schema = unsafe { &mut *schema };
        assert!(schema.release.is_some());
        schema.release = None;
        SCHEMA_RELEASES.fetch_add(1, Ordering::SeqCst);
    }

    #[test]
    fn arrow_rs_import_owns_release_callbacks() {
        ARRAY_RELEASES.store(0, Ordering::SeqCst);
        SCHEMA_RELEASES.store(0, Ordering::SeqCst);

        {
            let mut array = ArrowArray {
                release: Some(release_array),
                ..ArrowArray::empty()
            };
            let imported = unsafe { ptr::read((&array as *const ArrowArray).cast::<FFI_ArrowArray>()) };
            assert!(array.release.take().is_some());
            assert!(array.release.is_none());

            drop(imported);
            assert_eq!(ARRAY_RELEASES.load(Ordering::SeqCst), 1);
        }
        assert_eq!(ARRAY_RELEASES.load(Ordering::SeqCst), 1);

        {
            let mut schema = ArrowSchema {
                release: Some(release_schema),
                ..ArrowSchema::empty()
            };
            let imported = unsafe { ptr::read((&schema as *const ArrowSchema).cast::<FFI_ArrowSchema>()) };
            assert!(schema.release.take().is_some());
            assert!(schema.release.is_none());

            drop(imported);
            assert_eq!(SCHEMA_RELEASES.load(Ordering::SeqCst), 1);
        }

        assert_eq!(SCHEMA_RELEASES.load(Ordering::SeqCst), 1);
    }
}
