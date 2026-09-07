#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

pub type DuckDBStr<'a> = duckdb_v2_str;

impl<'a> From<&'a str> for DuckDBStr<'a> {
    fn from(val: &'a str) -> Self {
        DuckDBStr {
            ptr: val.as_ptr() as *const _,
            len: val.len() as idx_t,
        }
    }
}

impl<'a> From<&'a String> for DuckDBStr<'a> {
    fn from(val: &'a String) -> Self {
        DuckDBStr {
            ptr: val.as_ptr() as *const _,
            len: val.len() as idx_t,
        }
    }
}

impl<'a> From<DuckDBStr<'a>> for &'a str {
    fn from(val: DuckDBStr<'_>) -> Self {
        // TODO: Is this safe to do unchecked?
        unsafe { std::str::from_utf8_unchecked(std::slice::from_raw_parts(val.ptr as *const u8, val.len as usize)) }
    }
}

impl<'a> From<&'a duckdb_v2_bytes> for &'a str {
    fn from(value: &'a duckdb_v2_bytes) -> Self {
        unsafe {
            if value.value.inlined.length <= 12 {
                let len = value.value.inlined.length as usize;
                let bytes: &[u8] = std::slice::from_raw_parts(value.value.inlined.inlined.as_ptr() as *const u8, len);
                std::str::from_utf8(bytes).unwrap()
            } else {
                let len = value.value.pointer.length as usize;
                let bytes = std::slice::from_raw_parts(value.value.pointer.ptr as *const u8, len);
                std::str::from_utf8(bytes).unwrap()
            }
        }
    }
}
impl<'a> From<&'a duckdb_v2_bytes> for &'a [u8] {
    fn from(value: &'a duckdb_v2_bytes) -> Self {
        unsafe {
            if value.value.inlined.length <= 12 {
                let len = value.value.inlined.length as usize;
                let bytes: &[u8] = std::slice::from_raw_parts(value.value.inlined.inlined.as_ptr() as *const u8, len);
                bytes
            } else {
                let len = value.value.pointer.length as usize;
                std::slice::from_raw_parts(value.value.pointer.ptr as *const u8, len)
            }
        }
    }
}

impl Default for DuckDBStr<'_> {
    fn default() -> Self {
        DuckDBStr {
            ptr: std::ptr::null(),
            len: 0,
        }
    }
}
