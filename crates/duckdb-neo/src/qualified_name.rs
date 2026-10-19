//! Qualified names for catalog objects.

use crate::ffi;
use crate::ffi_str::DuckDBStr;
use crate::{AsRaw, DropHandle, FromRaw, Result, check_api_call, check_api_call_string, error::Error};

/// Owned identifier parts extracted positionally from a [`QualifiedName`].
#[derive(Debug)]
pub struct QualifiedNameView {
    /// The first part of a three-part name; absent otherwise.
    pub catalog: Option<String>,
    /// The penultimate part; absent for unqualified names.
    /// In a two-part name, resolution decides whether this is a schema or a catalog.
    pub schema: Option<String>,
    /// The table (or other object) name; always present.
    pub table: String,
}

/// An owned, optionally qualified name for a database object.
///
/// A name contains one to three non-empty identifier parts, ordered from the
/// outermost qualifier to the object name. For example, `catalog.schema.table`
/// has the parts `["catalog", "schema", "table"]`.
///
/// # Example
/// ```
/// use duckdb_neo::qualified_name::QualifiedName;
///
/// let name = QualifiedName::from_parts(&["main", "events"]).unwrap();
///
/// assert_eq!(name.parts().unwrap(), ["main", "events"]);
/// assert_eq!(name.render().unwrap(), "main.events");
/// ```
#[derive(AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_qname_destroy)]
pub struct QualifiedName {
    pub(crate) handle: ffi::duckdb_v2_qname_handle,
}

// SAFETY: an owned, self-contained copy of the name; `&self` methods only read it.
unsafe impl Send for QualifiedName {}
unsafe impl Sync for QualifiedName {}

impl TryFrom<&str> for QualifiedName {
    type Error = Error;

    fn try_from(value: &str) -> Result<Self> {
        QualifiedName::from_sql(value)
    }
}

impl QualifiedName {
    /// Parse dot-separated parts; double-quoted parts support dots and doubled quotes, unlike unquoted parts.
    /// Spaces/keywords need no quotes; malformed quoting, zero/empty parts, or over three parts return errors.
    pub fn from_sql(sql: &str) -> Result<Self> {
        check_api_call!(ffi::duckdb_v2_qname_parse, &sql.into(), RET).map(|handle| QualifiedName { handle })
    }

    /// Create a name from identifier parts, outermost first, with the object name last.
    /// Returns an error for zero parts, more than three parts, or any empty part.
    pub fn from_parts(parts: &[&str]) -> Result<Self> {
        let parts: Vec<ffi::duckdb_v2_str> = parts.iter().map(|s| (*s).into()).collect();

        check_api_call!(ffi::duckdb_v2_qname_create, parts.as_ptr(), parts.len() as u64, RET)
            .map(|handle| QualifiedName { handle })
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
        check_api_call_string!(ffi::duckdb_v2_qname_render, self.handle)
    }

    /// Return the number of identifier parts.
    pub fn parts_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_qname_get_part_count, self.handle, RET).map(|count| count as usize)
    }

    /// Copy parts into positional fields, leaving absent qualifiers as `None`.
    /// Resolution determines whether a two-part qualifier names a schema or a catalog.
    pub fn get_view(&self) -> Result<QualifiedNameView> {
        let parts = self.parts()?;
        let mut view = QualifiedNameView {
            catalog: None,
            schema: None,
            // DuckDB guarantees at least one part, with the object name last.
            table: parts[parts.len() - 1].clone(),
        };

        let mut iter = parts.iter().rev().skip(1);

        view.schema = iter.next().cloned();
        view.catalog = iter.next().cloned();

        Ok(view)
    }

    /// Return the identifier part at a zero-based index.
    ///
    /// Parts are ordered outermost first. An out-of-range index returns an
    /// error.
    pub fn part(&self, index: usize) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_qname_get_part, self.handle, index as u64, RET)
            .map(|data| DuckDBStr::from_raw(data).as_str().unwrap_or_default().to_owned())
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

impl std::fmt::Debug for QualifiedName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.parts() {
            Ok(parts) => f.debug_tuple("QualifiedName").field(&parts).finish(),
            Err(_) => f.debug_struct("QualifiedName").field("handle", &self.handle).finish(),
        }
    }
}

impl PartialEq for QualifiedName {
    fn eq(&self, other: &Self) -> bool {
        check_api_call!(ffi::duckdb_v2_qname_equals, self.handle, other.handle, RET).unwrap()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::qualified_name::QualifiedName;

    #[test]
    fn test_qualified_name_view() -> crate::Result<()> {
        for parts in [&["table"][..], &["schema", "table"], &["catalog", "schema", "table"]] {
            let view = QualifiedName::from_parts(parts)?.get_view()?;
            assert_eq!(view.table, "table");
            assert_eq!(view.schema.as_deref(), (parts.len() >= 2).then_some("schema"));
            assert_eq!(view.catalog.as_deref(), (parts.len() == 3).then_some("catalog"));
        }
        Ok(())
    }

    #[test]
    fn test_qualified_name_debug() -> crate::Result<()> {
        let name = QualifiedName::from_parts(&["main", "items"])?;
        assert_eq!(format!("{name:?}"), r#"QualifiedName(["main", "items"])"#);
        Ok(())
    }

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
