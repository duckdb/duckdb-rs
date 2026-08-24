use crate::{Result, check_api_call, ffi};

/// DuckDB's internal representation of `VARCHAR`-like data, such as bytes and strings.
///
/// Short values are stored inline. Longer values refer to memory owned by the
/// DuckDB arena supplied to [`DuckDBBytes::new`], so that arena must remain
/// alive while the bytes are accessed.
pub struct DuckDBBytes {
    data: ffi::duckdb_v2_bytes,
}

type DuckDBV2BytesUnion = ffi::duckdb_v2_bytes__bindgen_ty_1;
type DuckDBV2BytesPointer = ffi::duckdb_v2_bytes__bindgen_ty_1__bindgen_ty_1;
type DuckDBV2BytesInlined = ffi::duckdb_v2_bytes__bindgen_ty_1__bindgen_ty_2;
impl DuckDBBytes {
    /// Encode a string in DuckDB's internal byte representation.
    pub fn new<F: FnMut() -> Result<ffi::duckdb_v2_arena_handle>>(
        value: &str,
        mut heap: F,
    ) -> Result<Self> {
        let encoded = if value.len() <= ffi::DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
            let mut inlined = DuckDBV2BytesInlined {
                length: value.len() as u32,
                inlined: [0; ffi::DUCKDB_V2_BYTES_INLINE_LENGTH as usize],
            };
            unsafe {
                std::ptr::copy_nonoverlapping(
                    value.as_ptr(),
                    inlined.inlined.as_mut_ptr().cast(),
                    value.len(),
                );
            }
            DuckDBV2BytesUnion { inlined }
        } else {
            let mut pointer = DuckDBV2BytesPointer {
                length: value.len() as u32,
                prefix: Default::default(),
                ptr: std::ptr::null_mut(),
            };
            let heap = heap()?;

            check_api_call!(
                ffi::duckdb_v2_arena_allocate,
                heap,
                value.len() as u64,
                (&mut pointer.ptr as *mut *mut i8).cast(),
            )?;

            unsafe {
                // the engine compares and orders on the prefix, so it must mirror the first bytes
                std::ptr::copy_nonoverlapping(
                    value.as_ptr(),
                    pointer.prefix.as_mut_ptr().cast(),
                    pointer.prefix.len(),
                );
                std::ptr::copy_nonoverlapping(value.as_ptr(), pointer.ptr.cast(), value.len());
            }
            DuckDBV2BytesUnion { pointer }
        };
        Ok(DuckDBBytes {
            data: ffi::duckdb_v2_bytes { value: encoded },
        })
    }

    /// Return the payload length in bytes.
    pub fn size(&self) -> usize {
        unsafe { self.data.value.inlined.length as usize }
    }

    /// Return the payload as a borrowed byte slice.
    pub fn get_data(&self) -> &[u8] {
        if unsafe { self.data.value.inlined.length } <= ffi::DUCKDB_V2_BYTES_INLINE_LENGTH {
            unsafe {
                std::slice::from_raw_parts(
                    self.data.value.inlined.inlined.as_ptr().cast(),
                    self.data.value.inlined.length as usize,
                )
            }
        } else {
            unsafe {
                std::slice::from_raw_parts(
                    self.data.value.pointer.ptr.cast(),
                    self.data.value.pointer.length as usize,
                )
            }
        }
    }
}
