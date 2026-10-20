use std::marker::PhantomData;

use duckdb_loadable_macros::{AsRaw, DropHandle, FromRaw};

use crate::{Result, error::check_api_call, ffi, ffi_str::DuckDBStr, raw::RawExt, value::Value};

#[derive(Debug, AsRaw, FromRaw)]
pub struct QueryMetricsNode<'a> {
    pub(crate) handle: ffi::duckdb_v2_query_metrics_node_handle,
    _marker: PhantomData<&'a ()>,
}

impl<'a> QueryMetricsNode<'a> {
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

    pub fn name(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_query_metrics_node_get_name, self.raw(), RET).map(|x| {
            DuckDBStr::from_raw(x)
                .as_str()
                .expect("Failed to parse name")
                .to_string()
        })
    }

    pub fn child_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_query_metrics_node_get_child_count, self.raw(), RET).map(|x| x as usize)
    }

    pub fn metric_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_query_metrics_node_get_metric_count, self.raw(), RET).map(|x| x as usize)
    }

    pub fn get_by_index(&self, index: usize) -> Result<(String, Value)> {
        let name = check_api_call!(
            ffi::duckdb_v2_query_metrics_node_get_metric_name,
            self.raw(),
            index as u64,
            RET
        )
        .map(|x| DuckDBStr::from_raw(x).as_str())?
        .expect("Failed to fetch name");

        let value = check_api_call!(
            ffi::duckdb_v2_query_metrics_node_get_metric_value,
            self.raw(),
            index as u64,
            RET
        )
        .map(|handle| Value { handle })?;

        Ok((name.to_string(), value))
    }

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

#[derive(Debug, AsRaw, FromRaw, DropHandle)]
#[drop_handle(ffi::duckdb_v2_query_metrics_destroy)]
pub struct QueryMetrics {
    pub(crate) handle: ffi::duckdb_v2_query_metrics_handle,
}

impl QueryMetrics {
    pub fn root<'a>(&'a self) -> Result<QueryMetricsNode<'a>> {
        check_api_call!(ffi::duckdb_v2_query_metrics_get_root, self.raw(), RET).map(|handle| QueryMetricsNode {
            handle,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::SettingScope;
    use crate::types::ToValue;
    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        instance::Instance,
    };

    #[test]
    fn test_metrics() -> crate::Result<()> {
        let env = Environment::new()?;
        let instance = Instance::new(env)?;
        instance.attach(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let metrics = conn.query_metrics()?;
        let root = metrics.root()?;

        assert_eq!(root.metric_count()?, 0);
        assert_eq!(root.name()?, "QUERY");

        conn.set_profiling(true)?;
        conn.config()?
            .set_text("enable_profiling", "no_output", SettingScope::Session)?;

        let _ = conn.execute(
            "SELECT MAX(x) from RANGE(0, $max) as t(x)",
            Parameters::Named(&[("max", &1024_i32)]),
        )?;

        let metrics = conn.query_metrics()?;
        let root = metrics.root()?;

        assert_eq!(root.name()?, "QUERY");
        assert_eq!(root.child_count()?, 1);
        assert!(root.metric_count()? > 60);

        assert!(
            root.get_by_name("query.total_intermediate_rows")?
                .unwrap()
                .dbg_string()?
                == "2049"
        );

        let child = root.child(0)?;

        assert_eq!(child.name()?, "RESULT_COLLECTOR");

        Ok(())
    }
}
