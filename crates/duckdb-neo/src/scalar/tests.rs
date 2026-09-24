use crate::{
    DuckDBType, Parameters, connection::Context, environment::Environment, environment::StorageLocation,
    signature::Parameter,
};

use super::*;

struct ScalarWithData {
    base_data: Vec<i32>,
}

use crate::types::FromValue;

impl ScalarCallbacks for ScalarWithData {
    type BindData = Vec<i32>;
    type InitData = i32;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData> {
        assert_eq!(arguments[0].logical_type, LogicalType::from_text(context, "INTEGER")?);
        assert!(
            arguments[0]
                .value
                .as_ref()
                .is_some_and(|v| i32::from_value(v).unwrap() == Some(2))
        );

        Ok(vec![1, 2, 3])
    }

    fn init(&self, bind_data: Option<&Self::BindData>, _context: &Context) -> Result<Self::InitData> {
        Ok(bind_data.unwrap().iter().sum())
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &Context,

        input: &VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut output: Vector<'_, i32> = output.cast::<i32>()?;

        let in_vector = input.get_vector_at::<i32>(0)?;
        let in_data = in_vector
            .iter()?
            .next()
            .expect("Expected at least one value in input vector")
            .expect("Expected input value to be non-null");

        output.set_size(1)?;
        output.write(
            0,
            Some(self.base_data.iter().map(|x| x * in_data).sum::<i32>() - *init_data.unwrap()),
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
        _input: &VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut output: Vector<'_, i32> = output.cast::<i32>()?;

        output.set_size(1)?;
        output.write(0, Some(42))?;
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
        _input: &VectorCollection,
        _output: Vector<'_, Unknown>,
    ) -> Result<()> {
        panic!("This function panics");
    }
}

#[test]
fn test_scalar_bind_init_user_data() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env.open(StorageLocation::InMemory).expect("Failed to open database");
    let conn = db.connect().expect("Failed to connect");

    ScalarFunctionBuilder::new(
        "custom_scalar",
        SignatureBuilder::new(
            [Parameter::normal("multiplier", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        ScalarWithData {
            base_data: vec![1, 2, 3],
        },
    )
    .register(&conn)
    .expect("Failed to register scalar function");

    let result = conn
        .query("SELECT custom_scalar(2)", Parameters::None)
        .expect("Failed to execute query");

    for chunk in result {
        let chunk = chunk.expect("Failed to get result chunk");
        let vector = chunk.get_vector_at::<i32>(0).expect("Failed to get vector from chunk");
        let mut reader = vector.iter().unwrap();

        let data = reader
            .next()
            .expect("Expected at least one value in output vector")
            .expect("Expected output value to be non-null");

        assert!(*data == 6, "Expected value 6, got {}", data);
    }

    Ok(())
}

#[test]
fn test_scalar_panic() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    // DuckDB only invalidates the database when the internal error is raised on the
    // client thread; if a worker thread raises it first, only the transaction is
    // invalidated. A single thread makes the panic always run on the client thread.
    // This has to be set before attaching: a later `SET threads = 1` still
    // occasionally lets a worker thread execute the query.
    let db = env.instance().expect("Failed to create instance");
    db.set_option("threads", "1").expect("Failed to set threads");
    db.attach(&StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    ScalarFunctionBuilder::new(
        "panic_func",
        SignatureBuilder::new(Vec::new(), i32::logical_type(&conn)?),
        BasicScalarPanicFunction,
    )
    .register(&conn)
    .expect("Failed to register scalar function");

    let statements = conn.parse("SELECT panic_func()").expect("Failed to parse query");
    for stmt in statements {
        let stmt = stmt.expect("Failed to get statement");

        let mut result = conn.query(stmt, Parameters::None).expect("Failed to execute statement");

        let error;

        loop {
            if let Err(e) = result.step() {
                error = Some(e);
                break;
            }
        }

        assert!(error.is_some(), "Expected error when executing panic function");
    }
    let statements = conn.parse("SELECT 42").unwrap();

    for statement in statements {
        let stmt = statement.expect("Failed to get statement");

        let result = conn.query(stmt, Parameters::None);

        assert!(
            result.is_err(),
            "Expected error when executing panic function. DuckDB should be locked."
        );
    }

    Ok(())
}

#[test]
fn test_invalid_scalar_function_registration() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    let result = ScalarFunctionBuilder::new(
        "basic",
        SignatureBuilder::new(
            [
                Parameter::normal("", i32::logical_type(&conn)?),
                Parameter::normal("", i32::logical_type(&conn)?),
            ],
            i32::logical_type(&conn)?,
        ),
        BasicScalarFunction,
    )
    .register(&conn);

    assert!(
        result.is_err(),
        "Expected error when registering invalid scalar function"
    );

    Ok(())
}

#[test]
fn test_scalar_building() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    ScalarFunctionBuilder::new(
        "basic",
        SignatureBuilder::new(
            [Parameter::normal("input", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        BasicScalarFunction,
    )
    .register(&conn)
    .expect("Failed to register scalar function");

    let result = conn
        .query("SELECT basic(10)", Parameters::None)
        .expect("Failed to execute query");

    for chunk in result {
        let chunk = chunk.expect("Failed to get result chunk");

        let vector = chunk.get_vector_at::<i32>(0).expect("Failed to get vector from chunk");

        assert!(vector.len() == 1, "Expected vector size 1, got {}", vector.len());

        let value = vector.iter().unwrap().next().unwrap();

        assert!(value == Some(&42), "Expected value 42, got {:?}", value);
    }

    Ok(())
}

#[test]
fn test_scalar_property() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "basic",
        SignatureBuilder::new(
            [Parameter::normal("input", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        BasicScalarFunction,
    )
    .set_property(FunctionProperty::HasSpecialNullHandling(false))
    .register(&conn)?;

    for chunk in conn.query("SELECT basic(NULL)", Parameters::None)? {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i32>(0)?;

        assert!(vector.len() == 1, "Expected vector size 1, got {}", vector.len());

        let value = vector.iter()?.next().unwrap();

        assert!(value.is_none(), "Expected value NULL, got {:?}", value);
    }

    Ok(())
}

struct OverrideAbleScalar;

impl ScalarCallbacks for OverrideAbleScalar {
    type BindData = ();
    type InitData = ();

    fn bind(
        &self,
        context: &Context,
        _arguments: Vec<BindArgument>,
        result_type_handle: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData> {
        result_type_handle.override_return(i8::logical_type(context)?)?;
        Ok(())
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        _input: &VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let mut output = output.cast::<i8>()?;

        output.set_size(1)?;
        output.write(0, Some(42i8))?;

        Ok(())
    }
}

#[test]
fn test_scalar_override_result() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    ScalarFunctionBuilder::new(
        "override",
        SignatureBuilder::new(
            [Parameter::normal("in", i32::logical_type(&conn)?)],
            String::logical_type(&conn)?,
        ),
        OverrideAbleScalar {},
    )
    .register(&conn)?;

    let result = conn.query("SELECT override(42)", Parameters::None)?;

    for chunk in result {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i8>(0)?;

        assert!(vector.len() == 1, "Expected vector size 1, got {}", vector.len());

        let value = vector.iter()?.next().unwrap();

        assert!(value == Some(&42), "Expected value 42, got {:?}", value);
    }

    Ok(())
}

/// Records, per bind, whether the first argument was folded to a constant.
struct FoldProbe {
    folded: std::sync::Arc<std::sync::Mutex<Vec<bool>>>,
}

impl ScalarCallbacks for FoldProbe {
    type BindData = ();
    type InitData = ();

    fn bind(
        &self,
        _context: &Context,
        arguments: Vec<BindArgument>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData> {
        self.folded.lock().unwrap().push(arguments[0].value.is_some());
        Ok(())
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let input = input.get_vector_at::<i32>(0)?;
        let mut output = output.cast::<i32>()?;
        output.set_size(input.len())?;
        for (i, value) in input.iter()?.enumerate() {
            output.write(i, value.copied())?;
        }
        Ok(())
    }
}

#[test]
fn test_scalar_bind_unresolved_parameter() -> crate::Result<()> {
    let env = Environment::new()?;
    let db = env.open(StorageLocation::InMemory)?;
    let conn = db.connect()?;

    let folded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    ScalarFunctionBuilder::new(
        "fold_probe",
        SignatureBuilder::new(
            [Parameter::normal("x", i32::logical_type(&conn)?)],
            i32::logical_type(&conn)?,
        ),
        FoldProbe { folded: folded.clone() },
    )
    .register(&conn)?;

    // An unbound prepared parameter is not a constant, so binding must not fail.
    let statement = conn.parse("SELECT fold_probe(?::INTEGER)")?.next().unwrap()?;
    let prepared = statement.prepare(&conn, true)?;
    assert_eq!(folded.lock().unwrap().first(), Some(&false));

    let chunk = prepared
        .execute(Parameters::positional(&[&5_i32]))?
        .next()
        .expect("expected a result chunk")?;
    assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&5));

    // A literal argument still folds.
    folded.lock().unwrap().clear();
    conn.query("SELECT fold_probe(3)", Parameters::None)?.next().unwrap()?;
    assert_eq!(folded.lock().unwrap().first(), Some(&true));

    Ok(())
}
