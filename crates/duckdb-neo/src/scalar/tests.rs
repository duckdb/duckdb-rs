use crate::{
    DuckDBType, Parameters,
    connection::Context,
    environment::{Environment, StorageLocation},
    instance::{AttachBuilder, InstanceBuilder},
    logical_type::LogicalTypeID,
    signature::{FunctionParameter, SignatureBuilder},
    types::FromValue,
};

use super::*;

struct ScalarWithData {
    base_data: Vec<i32>,
}

impl ScalarBindCallbacks for ScalarWithData {
    fn bind(&self, context: &Context, arguments: BindArguments<'_>) -> Result<Self::BindData> {
        assert_eq!(arguments.logical_type(0)?, LogicalType::from_text(context, "INTEGER")?);
        assert!(
            arguments
                .value(0)?
                .is_some_and(|v| i32::from_value(&v).unwrap() == Some(2))
        );

        Ok(vec![10, 20])
    }
}

impl ScalarInitCallbacks for ScalarWithData {
    fn init(&self, bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        let bind_data = bind_data.expect("bind callback must provide data");
        assert_eq!(self.base_data, [1, 2, 3]);
        assert_eq!(bind_data, &[10, 20]);
        Ok(bind_data.iter().sum())
    }
}

impl ScalarCallbacks for ScalarWithData {
    type BindData = Vec<i32>;
    type InitData = i32;

    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &Context,

        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut output: Vector<'_, i32> = output.cast::<i32>()?;

        let in_vector = input.get_vector_at::<i32>(0)?;
        let in_data = in_vector
            .reader()?
            .iter()
            .next()
            .expect("Expected at least one value in input vector")
            .expect("Expected input value to be non-null");

        let bind_data = bind_data.expect("bind callback must provide data");
        let init_data = init_data.expect("init callback must provide data");
        assert_eq!(bind_data, &[10, 20]);
        assert_eq!(*init_data, 30);
        output.set_size(1)?;
        output.writer()?.set(
            0,
            Some(self.base_data.iter().map(|x| x * in_data).sum::<i32>() + bind_data.iter().sum::<i32>() - *init_data),
        )?;

        Ok(())
    }
}

struct BasicScalarFunction;

impl ScalarCallbacks for BasicScalarFunction {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        _input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut output: Vector<'_, i32> = output.cast::<i32>()?;

        output.set_size(1)?;
        output.writer()?.set(0, Some(42))?;
        Ok(())
    }
}

struct BasicScalarPanicFunction;

impl ScalarCallbacks for BasicScalarPanicFunction {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        _input: &mut VectorCollection,
        _output: Vector<'_, Unknown>,
    ) -> Result<()> {
        panic!("This function panics");
    }
}

#[test]
fn test_scalar_bind_init_user_data() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "custom_scalar",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("multiplier"))
            .return_type(i32::logical_type(&conn)?),
        ScalarWithData {
            base_data: vec![1, 2, 3],
        },
    )
    .with_bind()
    .with_init()
    .register(&conn)?;

    let mut result = conn.query("SELECT custom_scalar(2)", Parameters::None)?;

    let chunk = result.next_chunk()?.expect("expected a result chunk");
    assert_eq!(chunk.row_count()?, 1);
    assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&12));
    assert!(result.next_chunk()?.is_none());

    Ok(())
}

#[test]
fn test_scalar_panic() -> crate::Result<()> {
    let env = Environment::new()?;
    // A client-thread panic invalidates the instance, not just the worker transaction.
    let instance = InstanceBuilder::new().option("threads", "1").build(&env)?;
    instance.attach(AttachBuilder::new(StorageLocation::InMemory).make_default())?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "panic_func",
        SignatureBuilder::new().return_type(i32::logical_type(&conn)?),
        BasicScalarPanicFunction,
    )
    .register(&conn)?;

    let error = conn
        .query("SELECT panic_func()", Parameters::None)
        .and_then(|mut result| result.drain())
        .expect_err("expected a panic error");
    assert!(error.message.contains("This function panics"), "{error}");

    let error = conn
        .query("SELECT 42", Parameters::None)
        .and_then(|mut result| result.drain())
        .expect_err("expected database invalidation");
    assert!(error.message.to_lowercase().contains("invalidated"), "{error}");

    Ok(())
}

#[test]
fn test_invalid_scalar_function_registration() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let error = ScalarFunctionBuilder::new(
        "basic",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("x"))
            .param(FunctionParameter::new::<i32>("x"))
            .return_type(i32::logical_type(&conn)?),
        BasicScalarFunction,
    )
    .register(&conn)
    .expect_err("duplicate parameter names must be rejected");
    assert!(error.message.contains("Duplicate parameter name"), "{error}");

    let error = ScalarFunctionBuilder::new(
        "",
        SignatureBuilder::new().return_type(i32::logical_type(&conn)?),
        BasicScalarFunction,
    )
    .register(&conn)
    .expect_err("empty function names must be rejected");
    assert!(error.message.contains("Function name cannot be empty"), "{error}");

    let error = ScalarFunctionBuilder::new("missing_return", SignatureBuilder::new(), BasicScalarFunction)
        .register(&conn)
        .expect_err("missing return types must be rejected");
    assert!(error.message.contains("Return type must be set"), "{error}");

    Ok(())
}

#[test]
fn test_scalar_building() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "basic",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(&conn)?),
        BasicScalarFunction,
    )
    .register(&conn)?;

    let mut result = conn.query("SELECT basic(10)", Parameters::None)?;

    let chunk = result.next_chunk()?.expect("expected a result chunk");
    assert_eq!(chunk.row_count()?, 1);
    assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&42));
    assert!(result.next_chunk()?.is_none());

    Ok(())
}

#[test]
fn test_scalar_property() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "basic",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(&conn)?),
        BasicScalarFunction,
    )
    .set_property(FunctionProperty::HasSpecialNullHandling(true))
    .register(&conn)?;

    ScalarFunctionBuilder::new(
        "default_nulls",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("input"))
            .return_type(i32::logical_type(&conn)?),
        BasicScalarFunction,
    )
    .register(&conn)?;

    let mut result = conn.query(
        "SELECT basic(NULL::INTEGER), default_nulls(NULL::INTEGER)",
        Parameters::None,
    )?;
    let chunk = result.next_chunk()?.expect("expected a result chunk");
    assert_eq!(chunk.row_count()?, 1);
    assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&42));
    assert_eq!(chunk.get_vector_at::<i32>(1)?.reader()?.get(0)?, None);
    assert!(result.next_chunk()?.is_none());

    Ok(())
}

/// Records, per bind, whether the first argument was folded to a constant.
struct FoldProbe {
    folded: std::sync::Arc<std::sync::Mutex<Vec<bool>>>,
}

impl ScalarBindCallbacks for FoldProbe {
    fn bind(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<Self::BindData> {
        self.folded.lock().unwrap().push(arguments.value(0)?.is_some());
        Ok(())
    }
}

impl ScalarCallbacks for FoldProbe {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let input = input.get_vector_at::<i32>(0)?;
        let mut output = output.cast::<i32>()?;
        output.set_size(input.len())?;
        for (i, value) in input.reader()?.iter().enumerate() {
            output.writer()?.set(i, value.copied())?;
        }
        Ok(())
    }
}

#[test]
fn test_scalar_bind_unresolved_parameter() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    let folded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    ScalarFunctionBuilder::new(
        "fold_probe",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("x"))
            .return_type(i32::logical_type(&conn)?),
        FoldProbe { folded: folded.clone() },
    )
    .with_bind()
    .register(&conn)?;

    // An unbound prepared parameter is not a constant, so binding must not fail.
    let statement = conn.parse("SELECT fold_probe(?::INTEGER)")?.next().unwrap()?;
    let prepared = statement.prepare(&conn, true)?;
    assert_eq!(folded.lock().unwrap().first(), Some(&false));

    let chunk = prepared
        .execute(Parameters::positional(&[&5_i32]))?
        .next_chunk()?
        .expect("expected a result chunk");
    assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&5));

    // A literal argument still folds.
    folded.lock().unwrap().clear();
    conn.query("SELECT fold_probe(3)", Parameters::None)?
        .next_chunk()?
        .unwrap();
    assert_eq!(folded.lock().unwrap().first(), Some(&true));

    Ok(())
}

// With no bind callback registered, argument expressions are not evaluated.
struct Identity;

impl ScalarCallbacks for Identity {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let input = input.get_vector_at::<i32>(0)?;
        let mut output = output.cast::<i32>()?;
        output.set_size(input.len())?;
        for (i, value) in input.reader()?.iter().enumerate() {
            output.writer()?.set(i, value.copied())?;
        }
        Ok(())
    }
}

#[test]
fn test_scalar_bind_does_not_fold_arguments() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "identity",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<i32>("x"))
            .return_type(i32::logical_type(&conn)?),
        Identity,
    )
    .register(&conn)?;

    // The failing cast is never executed, so the query succeeds, as with a native function.
    let mut result = conn.query(
        "SELECT CASE WHEN i > 10 THEN identity('abc'::INTEGER) ELSE i::INTEGER END FROM range(3) t(i)",
        Parameters::None,
    )?;
    let chunk = result.next_chunk()?.expect("expected a result chunk");
    let values: Vec<_> = chunk
        .get_vector_at::<i32>(0)?
        .reader()?
        .iter()
        .map(|v| v.copied())
        .collect();
    assert_eq!(values, vec![Some(0), Some(1), Some(2)]);
    Ok(())
}

struct DynamicResolve;

impl ScalarResolveTypesCallbacks for DynamicResolve {
    fn resolve_types(&self, _context: &Context, arguments: BindArguments<'_>) -> Result<LogicalType> {
        arguments.logical_type(0)
    }
}

impl ScalarCallbacks for DynamicResolve {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        vectors: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let vec = vectors.get_untyped_vector_at(0)?;

        // SAFETY: the resolver selects the input type, and DuckDB retains both vectors for this batch.
        unsafe {
            output.reference(vec)?;
        }

        Ok(())
    }
}

#[test]
fn test_scalar_any_resolves_input_type_and_value() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    ScalarFunctionBuilder::new(
        "dynamic_resolve",
        SignatureBuilder::new()
            .param(FunctionParameter::with_type(
                "input",
                LogicalType::create_from_id(&conn, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ANY, Parameters::None)?,
            ))
            .return_type(LogicalType::create_from_id(
                &conn,
                LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ANY,
                Parameters::None,
            )?),
        DynamicResolve,
    )
    .with_resolve_types()
    .register(&conn)?;

    let mut query = conn.query(
        "SELECT dynamic_resolve(10::INTEGER), dynamic_resolve('duck'::VARCHAR)",
        Parameters::None,
    )?;
    let chunk = query.next_chunk()?.expect("expected a result chunk");
    assert_eq!(chunk.row_count()?, 1);
    let integer = chunk.get_vector_at::<i32>(0)?;
    let text = chunk.get_vector_at::<String>(1)?;
    assert_eq!(integer.logical_type(), &i32::logical_type(&conn)?);
    assert_eq!(text.logical_type(), &String::logical_type(&conn)?);
    assert_eq!(integer.reader()?.get(0)?, Some(&10));
    assert_eq!(text.reader()?.get(0)?, Some("duck"));
    assert!(query.next_chunk()?.is_none());

    Ok(())
}

#[test]
fn test_scalar_any_requires_resolver() -> crate::Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;
    let any = LogicalType::create_from_id(&conn, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ANY, Parameters::None)?;
    let error = ScalarFunctionBuilder::new(
        "missing_resolver",
        SignatureBuilder::new()
            .param(FunctionParameter::with_type("input", any.clone()))
            .return_type(any),
        DynamicResolve,
    )
    .register(&conn)
    .expect_err("ANY return must require a resolver");
    assert!(
        error
            .message
            .contains("ANY return type requires a resolve types callback"),
        "{error}"
    );
    Ok(())
}

#[derive(PartialEq)]
struct NonDefaultState(i32);

struct OptionalLifecycle {
    bind: bool,
    init: bool,
}

impl ScalarBindCallbacks for OptionalLifecycle {
    fn bind(&self, _context: &Context, _arguments: BindArguments<'_>) -> Result<Self::BindData> {
        assert!(self.bind);
        Ok(NonDefaultState(10))
    }
}

impl ScalarInitCallbacks for OptionalLifecycle {
    fn init(&self, bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        assert!(self.init);
        assert_eq!(bind_data.map(|data| data.0), self.bind.then_some(10));
        Ok(NonDefaultState(20))
    }
}

impl ScalarCallbacks for OptionalLifecycle {
    type BindData = NonDefaultState;
    type InitData = NonDefaultState;

    fn exec(
        &self,
        bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        assert_eq!(bind_data.map(|data| data.0), self.bind.then_some(10));
        assert_eq!(init_data.map(|data| data.0), self.init.then_some(20));
        let mut output = output.cast::<i32>()?;
        output.set_size(input.row_count()?)?;
        for i in 0..input.row_count()? {
            output.writer()?.set(i, Some(42))?;
        }
        Ok(())
    }
}

#[test]
fn test_scalar_optional_bind_and_init() -> Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let conn = instance.connect()?;

    for (name, bind, init) in [
        ("neither", false, false),
        ("bind_only", true, false),
        ("init_only", false, true),
        ("both_callbacks", true, true),
    ] {
        let mut builder = ScalarFunctionBuilder::new(
            name,
            SignatureBuilder::new().return_type(i32::logical_type(&conn)?),
            OptionalLifecycle { bind, init },
        );
        if bind {
            builder = builder.with_bind();
        }
        if init {
            builder = builder.with_init();
        }
        builder.register(&conn)?;
        let sql = format!("SELECT {name}()");
        let chunk = conn
            .query(sql.as_str(), Parameters::None)?
            .next_chunk()?
            .expect("expected a result chunk");
        assert_eq!(chunk.get_vector_at::<i32>(0)?.reader()?.get(0)?, Some(&42));
    }
    Ok(())
}
