use crate::{Context, DuckDBType, Environment, Parameters, StorageLocation, signature::Parameter};

use super::*;

struct ScalarWithData {
    base_data: Vec<i32>,
}

impl ScalarCallbacks for ScalarWithData {
    type BindData = Vec<i32>;
    type InitData = i32;
    type ResultType = i32;

    fn bind(
        &self,
        _context: Context,
        _metadata: BindMetadata,
        _result_type_handle: ResultTypeHandle,
    ) -> Result<Self::BindData> {
        Ok(vec![1, 2, 3])
    }

    fn init(
        &self,
        bind_data: Option<&Self::BindData>,
        _context: Context,
    ) -> Result<Self::InitData> {
        Ok(bind_data.unwrap().iter().sum())
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        init_data: Option<&Self::InitData>,
        _context: Context,

        input: &DataChunk,
        output: Vector<'_>,
    ) -> Result<()> {
        let mut output: Vector<'_, i32> = output.cast::<Self::ResultType>()?;

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
    type ResultType = i32;

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&Self::InitData>,
        _context: Context,
        _input: &DataChunk,
        output: Vector<'_>,
    ) -> Result<()> {
        let mut output: Vector<'_, i32> = output.cast::<Self::ResultType>()?;

        output.set_size(1)?;
        output.write(0, Some(42))?;
        Ok(())
    }
}

struct BasicScalarPanicFunction;

impl ScalarCallbacks for BasicScalarPanicFunction {
    type BindData = ();
    type InitData = ();
    type ResultType = i32;

    fn exec(
        &self,
        _init_data: Option<&Self::InitData>,
        _bind_data: Option<&Self::BindData>,
        _context: Context,
        _input: &DataChunk,
        _output: Vector<'_>,
    ) -> Result<()> {
        panic!("This function panics");
    }
}

#[test]
fn test_scalar_bind_init_user_data() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open database");
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
    .register_with_connection(&conn)
    .expect("Failed to register scalar function");

    let result = conn
        .query("SELECT custom_scalar(2)", Parameters::None)
        .expect("Failed to execute query");

    for chunk in result {
        let chunk = chunk.expect("Failed to get result chunk");
        let vector = chunk
            .get_vector_at::<i32>(0)
            .expect("Failed to get vector from chunk");
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
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    ScalarFunctionBuilder::new(
        "panic_func",
        SignatureBuilder::new(Vec::new(), i32::logical_type(&conn)?),
        BasicScalarPanicFunction,
    )
    .register_with_connection(&conn)
    .expect("Failed to register scalar function");

    let statements = conn
        .parse("SELECT panic_func()")
        .expect("Failed to parse query");
    for stmt in statements {
        let stmt = stmt.expect("Failed to get statement");

        let mut result = conn
            .query(stmt, Parameters::None)
            .expect("Failed to execute statement");

        let error;

        loop {
            if let Err(e) = result.step() {
                error = Some(e);
                break;
            }
        }

        assert!(
            error.is_some(),
            "Expected error when executing panic function"
        );
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
    .register_with_connection(&conn);

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
    .register_with_connection(&conn)
    .expect("Failed to register scalar function");

    let result = conn
        .query("SELECT basic(10)", Parameters::None)
        .expect("Failed to execute query");

    for chunk in result {
        let chunk = chunk.expect("Failed to get result chunk");

        let vector = chunk
            .get_vector_at::<i32>(0)
            .expect("Failed to get vector from chunk");

        assert!(
            vector.len() == 1,
            "Expected vector size 1, got {}",
            vector.len()
        );

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
    .register_with_connection(&conn)?;

    for chunk in conn.query("SELECT basic(NULL)", Parameters::None)? {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i32>(0)?;

        assert!(
            vector.len() == 1,
            "Expected vector size 1, got {}",
            vector.len()
        );

        let value = vector.iter()?.next().unwrap();

        assert!(value.is_none(), "Expected value NULL, got {:?}", value);
    }

    Ok(())
}

struct OverrideAbleScalar;

impl ScalarCallbacks for OverrideAbleScalar {
    type BindData = ();
    type InitData = ();
    type ResultType = i32;

    fn bind(
        &self,
        context: Context,
        _metadata: BindMetadata,
        result_type_handle: ResultTypeHandle,
    ) -> Result<Self::BindData> {
        result_type_handle.override_result_type(i8::logical_type(&context)?)?;
        Ok(())
    }

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&Self::InitData>,
        _context: Context,
        _input: &DataChunk,
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
    .register_with_connection(&conn)?;

    let result = conn.query("SELECT override(42)", Parameters::None)?;

    for chunk in result {
        let chunk = chunk?;

        let vector = chunk.get_vector_at::<i8>(0)?;

        assert!(
            vector.len() == 1,
            "Expected vector size 1, got {}",
            vector.len()
        );

        let value = vector.iter()?.next().unwrap();

        assert!(value == Some(&42), "Expected value 42, got {:?}", value);
    }

    Ok(())
}
