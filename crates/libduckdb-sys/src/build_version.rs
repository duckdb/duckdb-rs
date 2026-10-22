pub(crate) fn duckdb_version_from_pkg_version(pkg_version: &str) -> String {
    let encoded = pkg_version
        .split('.')
        .nth(1)
        .expect("CARGO_PKG_VERSION should use the documented 1.MAJOR_MINOR_PATCH.x format")
        .parse::<u32>()
        .expect("CARGO_PKG_VERSION should encode the DuckDB version as an integer in its second component");
    let duckdb_major = encoded / 10_000;
    let duckdb_minor = (encoded / 100) % 100;
    let duckdb_patch = encoded % 100;
    format!("{duckdb_major}.{duckdb_minor}.{duckdb_patch}")
}

#[cfg(any(feature = "bundled-cmake", test))]
pub(crate) fn cmake_version(pkg_version: &str, version: Option<&str>) -> String {
    version
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| format!("v{}", duckdb_version_from_pkg_version(pkg_version)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_crate_versions() {
        assert_eq!(duckdb_version_from_pkg_version("1.10506.0"), "1.5.6");
        assert_eq!(duckdb_version_from_pkg_version("1.20000.0"), "2.0.0");
        assert_eq!(duckdb_version_from_pkg_version("1.21003.4"), "2.10.3");
        assert_eq!(duckdb_version_from_pkg_version("1.123456.0"), "12.34.56");
    }

    #[test]
    fn defaults_to_crate_release() {
        assert_eq!(cmake_version("1.20000.0", None), "v2.0.0");
        assert_eq!(cmake_version("1.10506.0", Some(" ")), "v1.5.6");
        assert_eq!(cmake_version("1.10506.0", Some("")), "v1.5.6");
    }

    #[test]
    fn accepts_version_override() {
        assert_eq!(cmake_version("1.20000.0", Some("v2.1.0-dev42")), "v2.1.0-dev42");
    }

    #[cfg(all(feature = "bundled-cmake", feature = "capi-v1"))]
    #[test]
    fn bundled_cmake_reports_requested_version() {
        let expected = cmake_version(env!("CARGO_PKG_VERSION"), option_env!("DUCKDB_VERSION"));
        let actual = unsafe { std::ffi::CStr::from_ptr(crate::duckdb_library_version()) };
        assert_eq!(actual.to_str().unwrap(), expected);
    }
}
