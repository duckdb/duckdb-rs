use crate::{
    Result,
    error::{DuckDBError, Error},
    ffi,
    vector::Arena,
};

/// DuckDB's internal representation of `VARCHAR`-like data, such as bytes and strings.
///
/// Short values are stored inline. Longer values refer to memory owned by the
/// DuckDB arena they were written into, so that arena must remain
/// alive while the bytes are accessed.
///
/// Use [`Arena::add_bytes`] or [`Arena::add_str`] to encode values for a vector's arena.
/// Raw construction also requires an explicit lifetime guarantee:
///
/// ```compile_fail,E0133
/// use duckdb_neo::vector::{Arena, DuckDBBytes};
///
/// fn encode(arena: &Arena) -> duckdb_neo::Result<DuckDBBytes> {
///     DuckDBBytes::new(arena, b"value")
/// }
/// ```
#[repr(transparent)]
pub struct DuckDBBytes {
    data: ffi::duckdb_v2_bytes,
}

impl std::fmt::Debug for DuckDBBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBBytes")
            .field("size", &self.size())
            .finish_non_exhaustive()
    }
}

type DuckDBV2BytesUnion = ffi::duckdb_v2_bytes__bindgen_ty_1;
type DuckDBV2BytesPointer = ffi::duckdb_v2_bytes__bindgen_ty_1__bindgen_ty_1;
type DuckDBV2BytesInlined = ffi::duckdb_v2_bytes__bindgen_ty_1__bindgen_ty_2;
impl DuckDBBytes {
    /// Encode bytes in DuckDB's internal representation, copying long values into `arena`.
    ///
    /// # Safety
    /// The arena's storage must remain alive and unchanged for every use of the returned value.
    /// Resizing, flattening, or destroying its vector may invalidate that storage.
    pub unsafe fn new(arena: &Arena, value: &[u8]) -> Result<Self> {
        // SAFETY: The caller guarantees the arena's storage lifetime.
        unsafe { Self::from_parts(&[value], || Ok(arena)) }
    }

    #[inline(always)]
    pub(crate) unsafe fn from_parts<'arena>(
        parts: &[&[u8]],
        arena: impl FnOnce() -> Result<&'arena Arena>,
    ) -> Result<Self> {
        let length = parts
            .iter()
            .try_fold(0u32, |length, part| length.checked_add(u32::try_from(part.len()).ok()?))
            .ok_or_else(|| Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE,
                message: format!("combined byte length exceeds the maximum of {} bytes", u32::MAX),
            })?;

        let encoded = if length <= ffi::DUCKDB_V2_BYTES_INLINE_LENGTH {
            let mut inlined = DuckDBV2BytesInlined {
                length,
                inlined: [0; ffi::DUCKDB_V2_BYTES_INLINE_LENGTH as usize],
            };
            let mut offset = 0;
            for part in parts {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        part.as_ptr(),
                        inlined.inlined.as_mut_ptr().cast::<u8>().add(offset),
                        part.len(),
                    );
                }
                offset += part.len();
            }
            DuckDBV2BytesUnion { inlined }
        } else {
            let mut pointer = DuckDBV2BytesPointer {
                length,
                prefix: Default::default(),
                ptr: arena()?.allocate(length as usize)?.cast(),
            };
            let mut offset = 0;
            for part in parts {
                unsafe {
                    std::ptr::copy_nonoverlapping(part.as_ptr(), pointer.ptr.cast::<u8>().add(offset), part.len());
                }
                offset += part.len();
            }
            // The prefix may span several parts, including empty ones.
            unsafe {
                std::ptr::copy_nonoverlapping(pointer.ptr, pointer.prefix.as_mut_ptr(), pointer.prefix.len());
            }
            DuckDBV2BytesUnion { pointer }
        };
        Ok(Self {
            data: ffi::duckdb_v2_bytes { value: encoded },
        })
    }

    /// Return the payload length in bytes.
    pub fn size(&self) -> usize {
        unsafe { self.data.value.inlined.length as usize }
    }

    /// Return the payload as a borrowed byte slice.
    #[inline]
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DuckDBType,
        data_chunk::DataChunk,
        environment::{Environment, StorageLocation},
    };

    fn with_arena(test: impl FnOnce(&Arena) -> Result<()>) -> Result<()> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        let mut chunk = DataChunk::create(&conn, &[String::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<String>(0)?;
        test(vector.writer()?.arena()?)
    }

    #[test]
    fn parts_match_concatenation_and_prefix() -> Result<()> {
        with_arena(|arena| {
            let cases: &[&[&[u8]]] = &[
                &[],
                &[b"", b""],
                &[b"a", b"bc"],
                &[b"1234", b"", b"56789012"],
                &[b"a", b"", b"bc", b"defghijklm"],
                &[b"", b"a longer first part", b"", b"suffix"],
                &[&[0xf0, 0x9f], &[0xa6, 0x86]],
                &[&[0, 255], b"long binary payload", &[128]],
            ];
            for parts in cases {
                let expected = parts.concat();
                // SAFETY: Every encoded value is read before the arena can change.
                let encoded = unsafe { arena.add_parts(parts)? };
                assert_eq!(encoded.size(), expected.len());
                assert_eq!(encoded.get_data(), expected);
                if expected.len() > ffi::DUCKDB_V2_BYTES_INLINE_LENGTH as usize {
                    let prefix = unsafe { encoded.data.value.pointer.prefix }.map(|byte| byte as u8);
                    assert_eq!(prefix.as_slice(), &expected[..prefix.len()]);
                }
            }
            Ok(())
        })
    }

    #[test]
    fn parts_copy_their_inputs() -> Result<()> {
        with_arena(|arena| {
            let encoded = {
                let first = String::from("temporary input ");
                let second = String::from("and suffix");
                // SAFETY: The returned bytes belong to the arena, not the temporary inputs.
                unsafe { arena.add_parts(&[first.as_bytes(), second.as_bytes()])? }
            };
            assert_eq!(encoded.get_data(), b"temporary input and suffix");
            Ok(())
        })
    }

    #[test]
    fn parts_reject_combined_length_overflow() -> Result<()> {
        with_arena(|arena| {
            let fragment = vec![0u8; 1024 * 1024];
            let parts = vec![fragment.as_slice(); 4096];
            // SAFETY: The value, if accepted, would be read within the arena's lifetime.
            let error = unsafe { arena.add_parts(&parts) }.expect_err("4 GiB exceeds the encoded length");
            assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE);
            Ok(())
        })
    }
}
