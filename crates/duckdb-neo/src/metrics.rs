//! Query profiling metrics, organized as a tree of query and operator nodes.
//! Enable profiling with [`Connection::set_profiling`](crate::connection::Connection::set_profiling) first.

use std::marker::PhantomData;

use duckdb_loadable_macros::{AsRaw, DropHandle, FromRaw};

use crate::{Result, error::check_api_call, ffi, ffi_str::DuckDBStr, raw::RawExt, value::Value};

/// A query or operator node borrowed from a [`QueryMetrics`] snapshot.
///
/// Snapshots may omit metrics and operator nodes; completeness is not guaranteed.
#[derive(Debug, Clone, Copy, AsRaw, FromRaw)]
pub struct QueryMetricsNode<'a> {
    pub(crate) handle: ffi::duckdb_v2_query_metrics_node_handle,
    _marker: PhantomData<&'a ()>,
}

impl<'a> QueryMetricsNode<'a> {
    /// Borrow the child operator at `index`, returning an error if the index is out of range.
    pub fn child(&self, index: usize) -> Result<QueryMetricsNode<'a>> {
        check_api_call!(
            ffi::duckdb_v2_query_metrics_node_get_child,
            self.raw(),
            index as u64,
            RET
        )
        .map(|handle| QueryMetricsNode {
            handle,
            _marker: PhantomData,
        })
    }

    /// Return `QUERY` for the root node or an operator name such as `TABLE_SCAN` for other nodes.
    pub fn name(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_query_metrics_node_get_name, self.raw(), RET)
            .map(|x| DuckDBStr::from_raw(x).as_str().unwrap_or_default().to_string())
    }

    /// Return the number of child operators.
    pub fn child_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_query_metrics_node_get_child_count, self.raw(), RET).map(|x| x as usize)
    }

    /// Return the number of metrics on this node.
    pub fn metric_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_query_metrics_node_get_metric_count, self.raw(), RET).map(|x| x as usize)
    }

    /// Return the metric name and value at `index`, or an error if the index is out of range.
    /// Metric order is unspecified.
    pub fn get_by_index(&self, index: usize) -> Result<(String, Value)> {
        let name = check_api_call!(
            ffi::duckdb_v2_query_metrics_node_get_metric_name,
            self.raw(),
            index as u64,
            RET
        )
        .map(|x| DuckDBStr::from_raw(x).as_str())?
        .unwrap_or_default();

        let value = check_api_call!(
            ffi::duckdb_v2_query_metrics_node_get_metric_value,
            self.raw(),
            index as u64,
            RET
        )
        .map(|handle| Value { handle })?;

        Ok((name.to_string(), value))
    }

    /// Look up a metric by its exact name, such as `query.total_time`.
    /// Returns `None` if the metric is absent.
    pub fn get_by_name(&self, name: &str) -> Result<Option<Value>> {
        let mut name_ptr: ffi::duckdb_v2_str = name.into();
        let mut value_ptr = std::ptr::null_mut();

        let present = check_api_call!(
            ffi::duckdb_v2_query_metrics_node_get_metric,
            self.raw(),
            &mut name_ptr,
            &mut value_ptr,
            RET
        )?;

        if !present {
            Ok(None)
        } else {
            Ok(if !value_ptr.is_null() {
                Some(Value { handle: value_ptr })
            } else {
                None
            })
        }
    }
}

/// An owned snapshot of the metrics for a connection's last completed query.
///
/// Created by [`Connection::query_metrics`](crate::connection::Connection::query_metrics)
/// and unaffected by subsequent queries. If profiling is disabled, the root node
/// has no metrics or children.
#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_query_metrics_destroy)]
pub struct QueryMetrics {
    pub(crate) handle: ffi::duckdb_v2_query_metrics_handle,
}

impl QueryMetrics {
    /// Borrow the root node, which describes the query as a whole.
    pub fn root<'a>(&'a self) -> Result<QueryMetricsNode<'a>> {
        check_api_call!(ffi::duckdb_v2_query_metrics_get_root, self.raw(), RET).map(|handle| QueryMetricsNode {
            handle,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use crate::{
        Parameters,
        connection::SettingScope,
        environment::{Environment, StorageLocation},
        error::DuckDBError,
    };

    #[test]
    fn test_metrics() -> crate::Result<()> {
        let conn = Environment::new()?.open(StorageLocation::InMemory)?.connect()?;

        let metrics = conn.query_metrics()?;
        let root = metrics.root()?;
        assert_eq!(root.name()?, "QUERY");
        assert_eq!((root.metric_count()?, root.child_count()?), (0, 0));

        conn.set_profiling(true)?;
        conn.config()?
            .set_text("enable_profiling", "no_output", SettingScope::Session)?;
        conn.execute(
            "SELECT MAX(x) from RANGE(0, $max) as t(x)",
            Parameters::Named(&[("max", &1024_i32)]),
        )?;

        let metrics = conn.query_metrics()?;
        let root = metrics.root()?;
        assert_eq!(root.name()?, "QUERY");
        assert_eq!(root.child_count()?, 1);
        let rows = root
            .get_by_name("query.total_intermediate_rows")?
            .unwrap()
            .dbg_string()?;
        assert_eq!(rows, "2049");
        assert!(root.get_by_name("query.no_such_metric")?.is_none());

        // Index and name lookups must return the same metric values.
        let count = root.metric_count()?;
        assert!(count > 60);
        for index in 0..count {
            let (name, value) = root.get_by_index(index)?;
            let by_name = root.get_by_name(&name)?.expect("listed metric is present");
            assert_eq!(by_name.dbg_string()?, value.dbg_string()?, "{name}");
        }
        let error = root.get_by_index(count).unwrap_err();
        assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE);
        let error = root.child(1).unwrap_err();
        assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_INPUT_OUT_OF_RANGE);

        // Operator nodes remain accessible below the result collector.
        let mut node = root.child(0)?;
        let mut operators = vec![node.name()?];
        while node.child_count()? > 0 {
            node = node.child(0)?;
            operators.push(node.name()?);
        }
        assert_eq!(operators[0], "RESULT_COLLECTOR");
        assert!(operators.len() > 2, "{operators:?}");

        // Metrics are unavailable while a query result is still live.
        let result = conn.query("SELECT * FROM range(1000)", Parameters::None)?;
        let error = conn.query_metrics().unwrap_err();
        assert_eq!(error.code, DuckDBError::DUCKDB_V2_ERROR_RESOURCE_IN_USE);
        drop(result);

        // Later queries leave an earlier snapshot unchanged.
        conn.execute("SELECT * FROM range(10)", Parameters::None)?;
        assert_eq!(
            root.get_by_name("query.total_intermediate_rows")?
                .unwrap()
                .dbg_string()?,
            rows
        );

        conn.set_profiling(false)?;
        conn.execute("SELECT 42", Parameters::None)?;
        let metrics = conn.query_metrics()?;
        let root = metrics.root()?;
        assert_eq!((root.metric_count()?, root.child_count()?), (0, 0));
        Ok(())
    }
}
