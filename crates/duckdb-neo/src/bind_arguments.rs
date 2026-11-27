//! Bind-time metadata for scalar, aggregate, table, and pragma functions.

use alloc::{
    format,
    string::{String, ToString},
};
use core::{cell::OnceCell, marker::PhantomData};

use duckdb_loadable_macros::AsRaw;

/// Counts of positional fixed, `*args`, named-only, and `**kwargs` arguments, in that order.
pub use crate::builder_helpers::ArgCount as ArgumentCounts;

use crate::ffi_str::DuckDBStr;
use crate::{Error, FromValue, ffi};
use crate::{Result, check_api_call, error::DuckDBError, logical_type::LogicalType, value::Value};

/// The arguments of one call site, as seen by a bind callback.
///
/// Arguments are ordered as positional fixed parameters, `*args`, named-only parameters, then `**kwargs`.
/// Access them with [`Self::arg`], [`Self::arg_by_name`], or [`Self::iter`];
/// [`BindArgument::value`] folds constant arguments on demand and returns any evaluation errors.
#[derive(Debug, AsRaw)]
pub struct BindArguments<'a> {
    handle: ffi::duckdb_v2_function_bind_info_handle,
    len: OnceCell<usize>,
    _info: PhantomData<&'a ffi::duckdb_v2_function_bind_info_handle>,
}

impl<'a> BindArguments<'a> {
    pub(crate) fn new(raw: &'a ffi::duckdb_v2_function_bind_info_handle) -> Self {
        Self {
            handle: *raw,
            len: OnceCell::new(),
            _info: PhantomData,
        }
    }

    /// Return the number of arguments.
    pub fn len(&self) -> Result<usize> {
        if let Some(len) = self.len.get() {
            return Ok(*len);
        }
        let len = self.counts()?.total();
        Ok(*self.len.get_or_init(|| len))
    }

    /// Return whether the call site has no arguments.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Return the sizes of the four consecutive argument-list parts.
    pub fn counts(&self) -> Result<ArgumentCounts> {
        ArgumentCounts::from_func(|pf, pv, nf, nv| {
            check_api_call!(ffi::duckdb_v2_function_bind_get_arg_count, self.handle, pf, pv, nf, nv)
        })
    }

    /// Return argument `index`, or an error if it is out of bounds.
    pub fn arg(&self, index: usize) -> Result<BindArgument<'_>> {
        let len = self.len()?;
        if index < len {
            Ok(BindArgument {
                handle: self.handle,
                index,
                _arguments: PhantomData,
            })
        } else {
            Err(Error::invalid_input(format!(
                "argument index {index} is out of bounds for {len} arguments"
            )))
        }
    }

    /// Find an argument case-insensitively, skipping positional-only parameters.
    /// Return `Ok(None)` if no argument matches the name.
    pub fn arg_by_name(&self, name: &str) -> Result<Option<BindArgument<'_>>> {
        self.find(name)?.map(|index| self.arg(index)).transpose()
    }

    /// Like [`Self::arg_by_name`], but return the argument's index.
    pub fn find(&self, name: &str) -> Result<Option<usize>> {
        let mut index: ffi::idx_t = 0;
        let mut found = false;
        check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_index,
            self.handle,
            &name.into(),
            &mut index,
            &mut found
        )?;
        Ok(found.then_some(index as usize))
    }

    /// Iterate over every argument in order.
    pub fn iter(&self) -> Result<impl ExactSizeIterator + DoubleEndedIterator<Item = BindArgument<'_>>> {
        let handle = self.handle;
        Ok((0..self.len()?).map(move |index| BindArgument {
            handle,
            index,
            _arguments: PhantomData,
        }))
    }
}

/// One argument of a call site, borrowed from [`BindArguments`].
#[derive(Debug, Clone, Copy)]
pub struct BindArgument<'a> {
    handle: ffi::duckdb_v2_function_bind_info_handle,
    index: usize,
    _arguments: PhantomData<&'a BindArguments<'a>>,
}

impl BindArgument<'_> {
    /// Return the argument's position in [`BindArguments`].
    pub fn index(&self) -> usize {
        self.index
    }

    /// Return the declared parameter name, the caller's name for `**kwargs`, or empty for `*args`.
    pub fn name(&self) -> Result<String> {
        let str = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_name,
            self.handle,
            self.index as u64,
            RET
        )
        .map(DuckDBStr::from_raw)?;

        Ok(str.as_str().unwrap_or_default().to_string())
    }

    /// Return the argument's resolved logical type.
    pub fn logical_type(&self) -> Result<LogicalType> {
        check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_type,
            self.handle,
            self.index as u64,
            RET
        )
        .map(|handle| LogicalType { handle })
    }

    /// Fold the argument to a constant, or return `None` when it is not constant.
    ///
    /// SQL NULL is represented by `Some` containing a null [`Value`]. Errors raised
    /// while evaluating a constant argument, such as a failing cast, are returned.
    pub fn value(&self) -> Result<Option<Value>> {
        let handle = check_api_call!(
            ffi::duckdb_v2_function_bind_get_arg_value,
            self.handle,
            self.index as u64,
            RET
        );

        match handle {
            Ok(handle) => Ok(Some(Value { handle })),
            // Not foldable: a non-constant argument, or an unbound prepared parameter.
            Err(e)
                if matches!(
                    e.code,
                    DuckDBError::QueryBinder | DuckDBError::QueryParameterNotResolved
                ) =>
            {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    /// Like [`Self::value`], but convert the constant to `T`.
    /// The outer `None` means not constant, the inner `None` means SQL NULL.
    pub fn resolved_value<T: FromValue>(&self) -> Result<Option<Option<T>>> {
        self.value()?.map(|value| T::from_value(&value)).transpose()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::{
        DuckDBType, Parameters,
        connection::{Connection, Context},
        data_chunk::{DataChunk, VectorCollection},
        enums::FunctionProperty,
        environment::{Environment, StorageLocation},
        logical_type::LogicalTypeID,
        pragma_function::{PragmaFunctionBuilder, PragmaQueryCallbacks},
        scalar::{ScalarBindCallbacks, ScalarCallbacks, ScalarFunctionBuilder},
        signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder},
        table_function::{BindFunctionHandle, ExecColumnInfo, TableFunctionBuilder, TableFunctionCallbacks},
        vector::{Unknown, Vector},
    };

    #[derive(Debug, PartialEq)]
    struct Snapshot {
        counts: [usize; 4],
        args: Vec<(usize, String, LogicalTypeID, Option<i32>)>,
        lookups: Vec<(&'static str, Option<usize>)>,
        out_of_bounds: Option<DuckDBError>,
    }

    fn snapshot(arguments: &BindArguments<'_>) -> Result<Snapshot> {
        let counts = arguments.counts()?;
        assert_eq!(counts.total(), arguments.len()?);
        let args = arguments
            .iter()?
            .map(|arg| {
                let value = arg.resolved_value::<i32>()?.flatten();
                Ok((arg.index(), arg.name()?, arg.logical_type()?.id()?, value))
            })
            .collect::<Result<_>>()?;
        let lookups = ["fixed", "args", "NAMED", "extra", "more", "missing"]
            .into_iter()
            .map(|name| {
                let index = arguments.find(name)?;
                assert_eq!(arguments.arg_by_name(name)?.map(|arg| arg.index()), index);
                Ok((name, index))
            })
            .collect::<Result<_>>()?;
        Ok(Snapshot {
            counts: [
                counts.positional_fixed,
                counts.positional_variadic,
                counts.named_fixed,
                counts.named_variadic,
            ],
            args,
            lookups,
            out_of_bounds: arguments.arg(arguments.len()?).err().map(|e| e.code),
        })
    }

    fn all_kinds() -> SignatureBuilder {
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("fixed"))
            .param(FunctionParameter::vararg::<i32>("args"))
            .param(FunctionParameter::new::<i32>("named").kind(FunctionParameterKind::Named))
            .param(FunctionParameter::new::<i32>("kwargs").kind(FunctionParameterKind::NamedVariadic))
    }

    fn all_kinds_call(kwargs: &[(&'static str, i32)], op: &str) -> String {
        let kwargs: String = kwargs
            .iter()
            .map(|(name, value)| format!(", {name} {op} {value}"))
            .collect();
        format!("(1, 2, 3, named {op} 4{kwargs})")
    }

    fn all_kinds_expected(kwargs: &[(&'static str, i32)]) -> Snapshot {
        let int = LogicalTypeID::Integer;
        let mut args = vec![
            (0, "fixed".into(), int, Some(1)),
            (1, String::new(), int, Some(2)),
            (2, String::new(), int, Some(3)),
            (3, "named".into(), int, Some(4)),
        ];
        args.extend(
            (4..)
                .zip(kwargs)
                .map(|(index, (name, value))| (index, name.to_string(), int, Some(*value))),
        );
        let kwarg_index = |name| {
            (4..)
                .zip(kwargs)
                .find(|(_, (kwarg, _))| *kwarg == name)
                .map(|(index, _)| index)
        };
        Snapshot {
            counts: [1, 2, 1, kwargs.len()],
            args,
            lookups: vec![
                ("fixed", Some(0)),
                ("args", None),
                ("NAMED", Some(3)),
                ("extra", kwarg_index("extra")),
                ("more", kwarg_index("more")),
                ("missing", None),
            ],
            out_of_bounds: Some(DuckDBError::InputInvalid),
        }
    }

    // Not sorted by name, so the expected order is the call site's.
    const KWARGS: &[(&str, i32)] = &[("more", 6), ("extra", 5)];

    type Snapshots = Arc<Mutex<Vec<Snapshot>>>;
    type Constants = Arc<Mutex<Vec<Option<Option<i32>>>>>;

    // Debug builds of DuckDB verify the plan by binding it again, so one statement may record several binds.
    fn drain_binds<T: std::fmt::Debug + PartialEq>(log: &Mutex<Vec<T>>) -> T {
        let mut binds = log.lock().unwrap().drain(..).collect::<Vec<_>>();
        assert!(!binds.is_empty(), "expected at least one bind");
        assert!(
            binds.windows(2).all(|pair| pair[0] == pair[1]),
            "binds differ: {binds:?}"
        );
        binds.swap_remove(0)
    }

    fn connect() -> Result<(Environment, Connection)> {
        let env = Environment::new()?;
        let conn = env.open(StorageLocation::InMemory)?.connect()?;
        Ok((env, conn))
    }

    struct PragmaProbe(Snapshots);

    impl PragmaQueryCallbacks for PragmaProbe {
        fn query(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<String> {
            self.0.lock().unwrap().push(snapshot(&arguments)?);
            Ok("SELECT 42::INTEGER".into())
        }
    }

    struct TableProbe(Snapshots);

    impl TableFunctionCallbacks for TableProbe {
        type BindData = ();
        type GlobalState = ();
        type LocalState = ();

        fn bind(
            &self,
            context: &Context,
            arguments: BindArguments<'_>,
            bind_handle: BindFunctionHandle<'_>,
        ) -> Result<()> {
            self.0.lock().unwrap().push(snapshot(&arguments)?);
            bind_handle.add_result_column("x", &i32::logical_type(context)?)?;
            Ok(())
        }

        fn exec(
            &self,
            _: &Context,
            _: &(),
            _: Option<&mut ()>,
            _: Option<&()>,
            _: &mut DataChunk<'_>,
            _: ExecColumnInfo<'_>,
        ) -> Result<()> {
            Ok(())
        }
    }

    /// Records each bind's snapshot, plus `resolved_value` for argument 0.
    struct ScalarProbe {
        snapshots: Snapshots,
        constants: Constants,
    }

    impl ScalarBindCallbacks for ScalarProbe {
        fn bind(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<()> {
            self.snapshots.lock().unwrap().push(snapshot(&arguments)?);
            let constant = arguments.arg(0)?.resolved_value::<i32>()?;
            self.constants.lock().unwrap().push(constant);
            Ok(())
        }
    }

    impl ScalarCallbacks for ScalarProbe {
        type BindData = ();
        type InitData = ();

        fn exec(
            &self,
            _context: &Context,
            _bind_data: Option<&()>,
            _init_data: Option<&mut ()>,
            input: &mut VectorCollection,
            output: Vector<'_, Unknown>,
        ) -> Result<()> {
            let rows = input.row_count()?;
            let mut output = output.cast::<i32>()?;
            output.set_size(rows)?;
            for row in 0..rows {
                output.writer()?.set_option(row, Some(&0))?;
            }
            Ok(())
        }
    }

    #[test]
    fn pragma_arguments_of_every_kind() -> Result<()> {
        let (_env, conn) = connect()?;
        let snapshots = Snapshots::default();
        PragmaFunctionBuilder::query("probe", all_kinds(), PragmaProbe(snapshots.clone())).register(&conn)?;

        // PRAGMA only accepts `=`, and orders `**kwargs` by hash, so pass just one.
        let kwargs = &[("extra", 5)];
        conn.execute(format!("PRAGMA probe{}", all_kinds_call(kwargs, "=")), Parameters::None)?;
        assert_eq!(*snapshots.lock().unwrap(), [all_kinds_expected(kwargs)]);
        Ok(())
    }

    #[test]
    fn table_function_arguments_of_every_kind() -> Result<()> {
        let (_env, conn) = connect()?;
        let snapshots = Snapshots::default();
        TableFunctionBuilder::new("probe", all_kinds(), TableProbe(snapshots.clone())).register(&conn)?;

        conn.execute(
            format!("SELECT * FROM probe{}", all_kinds_call(KWARGS, "=")),
            Parameters::None,
        )?;
        conn.execute(
            format!("SELECT * FROM probe{}", all_kinds_call(KWARGS, ":=")),
            Parameters::None,
        )?;
        assert_eq!(
            *snapshots.lock().unwrap(),
            [all_kinds_expected(KWARGS), all_kinds_expected(KWARGS)]
        );
        Ok(())
    }

    #[test]
    fn scalar_arguments_of_every_kind() -> Result<()> {
        let (_env, conn) = connect()?;
        let probe = ScalarProbe {
            snapshots: Snapshots::default(),
            constants: Arc::default(),
        };
        let (snapshots, constants) = (probe.snapshots.clone(), probe.constants.clone());
        let signature = all_kinds().return_type(i32::logical_type(&conn)?);
        ScalarFunctionBuilder::new("probe", signature, probe)
            .with_bind()
            .register(&conn)?;

        // In a scalar call `=` is a comparison, so arguments are named with `:=`.
        conn.execute(
            format!("SELECT probe{}", all_kinds_call(KWARGS, ":=")),
            Parameters::None,
        )?;
        assert_eq!(drain_binds(&snapshots), all_kinds_expected(KWARGS));
        assert_eq!(drain_binds(&constants), Some(Some(1)));
        Ok(())
    }

    #[test]
    fn resolved_value_distinguishes_null_from_non_constant() -> Result<()> {
        let (_env, conn) = connect()?;
        let probe = ScalarProbe {
            snapshots: Snapshots::default(),
            constants: Arc::default(),
        };
        let constants = probe.constants.clone();
        let signature = SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("fixed"))
            .return_type(i32::logical_type(&conn)?);
        // Without special NULL handling, a NULL argument folds the call to NULL before bind.
        ScalarFunctionBuilder::new("probe", signature, probe)
            .with_bind()
            .set_property(FunctionProperty::HasSpecialNullHandling(true))
            .register(&conn)?;

        conn.execute("SELECT probe(NULL::INTEGER)", Parameters::None)?;
        assert_eq!(drain_binds(&constants), Some(None));
        conn.execute("SELECT probe(x::INTEGER) FROM range(3) t(x)", Parameters::None)?;
        assert_eq!(drain_binds(&constants), None);
        let statement = conn.parse("SELECT probe(?::INTEGER)")?.next().unwrap()?;
        statement.prepare(&conn, true)?;
        assert_eq!(drain_binds(&constants), None);
        Ok(())
    }
}
