use libduckdb_sys::{duckdb_string_t, duckdb_string_t_data, duckdb_string_t_length};

/// Wrapper for underlying duck string type with a lifetime bound to a &mut duckdb_string_t
pub struct DuckString<'a> {
    ptr: &'a mut duckdb_string_t,
}

impl<'a> DuckString<'a> {
    pub(crate) fn new(ptr: &'a mut duckdb_string_t) -> Self {
        DuckString { ptr }
    }
}

impl<'a> DuckString<'a> {
    /// convert duckdb_string_t to a copy on write string
    pub fn as_str(&mut self) -> std::borrow::Cow<'a, str> {
        String::from_utf8_lossy(self.as_bytes())
    }

    /// convert duckdb_string_t to a byte slice
    pub fn as_bytes(&mut self) -> &'a [u8] {
        unsafe {
            let len = duckdb_string_t_length(*self.ptr);
            let c_ptr = duckdb_string_t_data(self.ptr);
            std::slice::from_raw_parts(c_ptr as *const u8, len as usize)
        }
    }
}
