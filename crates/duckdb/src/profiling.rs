//! Query profiling support for DuckDB.
//!
//! This module provides access to DuckDB's query profiling infrastructure,
//! allowing you to inspect per-operator metrics for executed queries.
//!
//! # Usage
//!
//! Profiling must be explicitly enabled on a connection before metrics are
//! collected. Use the `enable_profiling` and `profiling_mode` PRAGMAs to
//! configure it:
//!
//! ```rust,no_run
//! # use duckdb::Connection;
//! let conn = Connection::open_in_memory().unwrap();
//!
//! // Prevents DuckDB from printing profiling output to stdout after each query,
//! // which is the default behavior when profiling is enabled.
//! conn.execute("PRAGMA enable_profiling = 'no_output'", []).unwrap();
//!
//! // Enables standard profiling mode, which collects a comprehensive set of metrics for each operator in the query plan.
//! // There is also `detailed` mode for even more fine-grained metrics. See DuckDB's documentation for more details.
//! conn.execute("PRAGMA profiling_mode = 'standard'", []).unwrap();
//!
//! // Now we can execute a query and retrieve its profiling info.
//! conn.execute("SELECT 42", []).unwrap();
//!
//! let info = conn.get_profiling_info().expect("profiling should be enabled");
//! println!("rows returned: {}", info.metrics["ROWS_RETURNED"]);
//! ```
//!
//! # Structure
//!
//! [`ProfilingInfo`] is a tree that mirrors DuckDB's query plan. The root node
//! (`QUERY_ROOT`) holds metrics for the entire query (e.g. total `LATENCY` and
//! `ROWS_RETURNED`). Each child node represents a plan operator and carries its
//! own metrics such as `OPERATOR_NAME` and `OPERATOR_CARDINALITY`.

use std::collections::HashMap;

use crate::{Connection, inner_connection::InnerConnection};

/// [`ProfilingInfo`] is a recursive type containing metrics for each node in DuckDB's query plan.
/// There are two types of nodes: the "QUERY_ROOT" and "OPERATOR" nodes.
/// The "QUERY_ROOT" refers exclusively to the top-level node; its metrics are measured over the entire query.
/// The "OPERATOR" nodes refer to the individual operators in the query plan.
#[derive(Debug, Clone)]
pub struct ProfilingInfo {
    /// The metrics for the node, represented as a map from metric name to metric value.
    /// The actual format and units of the metric varies between metric kinds. For example,
    /// - "ROWS_RETURNED" is an integer, formatted as a string
    /// - "LATENCY" is a double-floating point number measuring the total query time in seconds, formatted as a string
    /// - "OPERATOR_NAME" is a string
    /// - "EXTRA_INFO" is a map of its own encoded as a string
    pub metrics: HashMap<String, String>,
    /// The children of the node and their respective metrics.
    pub children: Vec<ProfilingInfo>,
}

impl ProfilingInfo {
    /// # Safety
    /// `info` must be a valid (or NULL) pointer obtained from [`libduckdb_sys::duckdb_get_profiling_info`].
    fn from_raw(info: libduckdb_sys::duckdb_profiling_info) -> Option<Self> {
        if info.is_null() {
            return None;
        }

        // Extract metrics
        let mut map = unsafe { libduckdb_sys::duckdb_profiling_info_get_metrics(info) };
        let map_size = unsafe { libduckdb_sys::duckdb_get_map_size(map) };

        let mut metrics = HashMap::<String, String>::with_capacity(map_size as usize);

        for i in 0..map_size {
            let mut key = unsafe { libduckdb_sys::duckdb_get_map_key(map, i) };
            let mut val = unsafe { libduckdb_sys::duckdb_get_map_value(map, i) };

            let key_mem = unsafe { libduckdb_sys::duckdb_get_varchar(key) };
            let val_mem = unsafe { libduckdb_sys::duckdb_get_varchar(val) };

            let key_str = unsafe { std::ffi::CStr::from_ptr(key_mem) }
                .to_string_lossy()
                .to_string();
            let val_str = unsafe { std::ffi::CStr::from_ptr(val_mem) }
                .to_string_lossy()
                .to_string();

            metrics.insert(key_str, val_str);

            unsafe { libduckdb_sys::duckdb_free(key_mem as *mut std::ffi::c_void) };
            unsafe { libduckdb_sys::duckdb_free(val_mem as *mut std::ffi::c_void) };

            unsafe { libduckdb_sys::duckdb_destroy_value(&mut key) };
            unsafe { libduckdb_sys::duckdb_destroy_value(&mut val) };
        }

        unsafe { libduckdb_sys::duckdb_destroy_value(&mut map) };

        // Extract children
        let child_count = unsafe { libduckdb_sys::duckdb_profiling_info_get_child_count(info) };
        let mut children = Vec::with_capacity(child_count as usize);
        for i in 0..child_count {
            let child_info = unsafe { Self::from_raw(libduckdb_sys::duckdb_profiling_info_get_child(info, i)) };
            if let Some(info) = child_info {
                children.push(info);
            }
        }

        Some(ProfilingInfo { metrics, children })
    }
}

impl InnerConnection {
    /// Retrieves the [`ProfilingInfo`] for the last executed query, if profiling is enabled.
    pub fn get_profiling_info(&self) -> Option<ProfilingInfo> {
        let info = unsafe { libduckdb_sys::duckdb_get_profiling_info(self.con) };
        ProfilingInfo::from_raw(info)
    }
}

impl Connection {
    /// Retrieves the [`ProfilingInfo`] for the last executed query, if profiling is enabled.
    pub fn get_profiling_info(&self) -> Option<ProfilingInfo> {
        self.db.borrow().get_profiling_info()
    }
}

#[cfg(test)]
mod tests {
    use crate::Connection;

    #[test]
    fn test_profiling_info() {
        let conn = Connection::open_in_memory().unwrap();

        conn.execute("SELECT 1", []).unwrap();
        let info = conn.get_profiling_info();

        assert!(
            info.is_none(),
            "Profiling info should be None when profiling is not enabled"
        );

        conn.execute("PRAGMA enable_profiling = 'no_output'", []).unwrap();
        conn.execute("PRAGMA profiling_mode = 'standard'", []).unwrap();
        conn.execute("SELECT 1", []).unwrap();
        let info = conn.get_profiling_info();

        assert!(info.is_some(), "Metrics should be present when profiling is enabled");
        let info = info.unwrap();

        assert!(!info.metrics.is_empty(), "Metrics should not be empty");
        assert!(
            !info.children.is_empty(),
            "There should be at least one child for a simple query"
        );

        assert!(
            info.metrics.contains_key("ROWS_RETURNED"),
            "Metrics should contain ROWS_RETURNED"
        );
        assert!(
            info.metrics.get("ROWS_RETURNED").unwrap() == "1",
            "ROWS_RETURNED should be 1"
        );
    }
}
