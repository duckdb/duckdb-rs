//! Qualified names for catalog objects.

use crate::{
    Result, check_api_call, check_api_call_no_err,
    error::{DuckDBError, Error},
};
use libduckdb_sys as ffi;

/// An owned, optionally qualified name for a database object.
///
/// A name contains one to three non-empty identifier parts, ordered from the
/// outermost qualifier to the object name. For example, `catalog.schema.table`
/// has the parts `["catalog", "schema", "table"]`.
///
/// # Example
/// ```
/// use duckdb_rs::qualified_name::QualifiedName;
///
/// let name = QualifiedName::from_parts(&["main", "events"]).unwrap();
///
/// assert_eq!(name.parts().unwrap(), ["main", "events"]);
/// assert_eq!(name.render().unwrap(), "main.events");
/// ```
#[derive(Debug)]
pub struct QualifiedName {
    /// The owned DuckDB qualified-name handle.
    pub handle: ffi::duckdb_v2_qname_handle,
}

impl QualifiedName {
    /// Parse a qualified name using DuckDB's SQL identifier rules.
    pub fn from_sql(sql: &str) -> Result<Self> {
        Ok(QualifiedName {
            handle: check_api_call!(ffi::duckdb_v2_qname_parse, sql.into(), RET)?,
        })
    }

    /// Create a name from one to three non-empty identifier parts.
    pub fn from_parts(parts: &[&str]) -> Result<Self> {
        let parts: Vec<ffi::duckdb_v2_str> = parts.iter().map(|s| (*s).into()).collect();

        Ok(QualifiedName {
            handle: check_api_call!(
                ffi::duckdb_v2_qname_create,
                parts.as_ptr(),
                parts.len() as u64,
                RET
            )?,
        })
    }

    /// Return a hash suitable for in-process lookup.
    ///
    /// The hash follows DuckDB's case-insensitive identifier equality and is
    /// not stable across processes or library versions.
    pub fn hash(&self) -> Result<u64> {
        check_api_call!(ffi::duckdb_v2_qname_hash, self.handle, RET)
    }

    /// Render the name as SQL, quoting and escaping parts when needed.
    pub fn render(&self) -> Result<String> {
        let data = check_api_call!(ffi::duckdb_v2_qname_render, self.handle, RET)?;

        let text = unsafe { std::ffi::CStr::from_ptr(data) }
            .to_str()
            .map(|s| s.to_string())
            .map_err(|e| Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!("Failed to convert result to string: {}", e),
            });

        unsafe {
            libc::free(data as *mut libc::c_void);
        }

        text
    }

    /// Return the number of identifier parts.
    pub fn parts_count(&self) -> Result<usize> {
        let count = check_api_call!(ffi::duckdb_v2_qname_get_part_count, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Return the identifier part at a zero-based index.
    ///
    /// Parts are ordered outermost first. An out-of-range index returns an
    /// error.
    pub fn part(&self, index: usize) -> Result<String> {
        let data = check_api_call!(
            ffi::duckdb_v2_qname_get_part,
            self.handle,
            index as u64,
            RET
        )?;

        let reference: &str = data.into();

        Ok(reference.to_string())
    }

    /// Return all identifier parts, ordered outermost first.
    pub fn parts(&self) -> Result<Vec<String>> {
        let count = check_api_call!(ffi::duckdb_v2_qname_get_part_count, self.handle, RET)?;

        let mut parts = Vec::with_capacity(count as usize);
        for i in 0..count {
            parts.push(self.part(i as usize)?);
        }

        Ok(parts)
    }
}

impl PartialEq for QualifiedName {
    fn eq(&self, other: &Self) -> bool {
        check_api_call!(ffi::duckdb_v2_qname_equals, self.handle, other.handle, RET).unwrap()
    }
}

impl Drop for QualifiedName {
    fn drop(&mut self) {
        check_api_call_no_err!(ffi::duckdb_v2_qname_destroy, &mut self.handle).unwrap();
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::qualified_name::QualifiedName;

    #[test]
    fn test_qualified_name() -> crate::Result<()> {
        let qname = QualifiedName::from_sql("main.test.table")?;
        let qname_2 = QualifiedName::from_sql("main.other.table")?;
        let qname_3 = QualifiedName::from_parts(&["main", "test", "table"])?;

        assert_eq!(qname.parts_count()?, 3);
        assert_eq!(qname.part(0)?, "main");
        assert_eq!(qname.part(1)?, "test");
        assert_eq!(qname.part(2)?, "table");

        assert_ne!(qname.hash()?, qname_2.hash()?);
        assert_ne!(qname, qname_2);
        assert_eq!(qname, qname_3);

        assert_eq!(qname.render()?, "main.test.\"table\"");

        Ok(())
    }
}
