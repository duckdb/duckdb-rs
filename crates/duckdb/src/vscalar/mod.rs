use std::ffi::CString;

use function::{ScalarFunction, ScalarFunctionSet};
use libduckdb_sys::{
    duckdb_data_chunk, duckdb_function_info, duckdb_scalar_function_get_extra_info, duckdb_scalar_function_set_error,
    duckdb_vector,
};

use crate::{
    core::{DataChunkHandle, LogicalTypeHandle},
    inner_connection::InnerConnection,
    vtab::arrow::WritableVector,
    Connection,
};
mod function;

/// The duckdb Arrow scalar function interface
#[cfg(feature = "vscalar-arrow")]
pub mod arrow;

#[cfg(feature = "vscalar-arrow")]
pub use arrow::{ArrowFunctionSignature, ArrowScalarParams, VArrowScalar};

/// Duckdb scalar function trait
pub trait VScalar: Sized {
    /// State set at registration time. Persists for the lifetime of the catalog entry.
    /// Shared across worker threads and invocations — must not be modified during execution.
    /// Must be `'static` as it is stored in DuckDB and may outlive the current stack frame.
    type State: Sized + Send + Sync + 'static;
    /// The actual function
    ///
    /// # Safety
    ///
    /// This function is unsafe because it:
    ///
    /// - Dereferences multiple raw pointers (`func`).
    ///
    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>>;

    /// The possible signatures of the scalar function.
    /// These will result in DuckDB scalar function overloads.
    /// The invoke method should be able to handle all of these signatures.
    fn signatures() -> Vec<ScalarFunctionSignature>;

    /// Whether the scalar function is volatile.
    ///
    /// Volatile functions are re-evaluated for each row, even if they have no parameters.
    /// This is useful for functions that generate random or unique values, such as random
    /// number generators, UUID generators, or fake data generators.
    ///
    /// By default, DuckDB optimizes zero-argument scalar functions as constants, evaluating
    /// them only once. Returning true from this method prevents this optimization.
    ///
    /// # Default
    /// Returns `false` by default, meaning the function is not volatile.
    fn volatile() -> bool {
        false
    }
}

/// Duckdb scalar function parameters
pub enum ScalarParams {
    /// Exact parameters
    Exact(Vec<LogicalTypeHandle>),
    /// Variadic parameters
    Variadic(LogicalTypeHandle),
}

/// Duckdb scalar function signature
pub struct ScalarFunctionSignature {
    parameters: Option<ScalarParams>,
    return_type: LogicalTypeHandle,
}

impl ScalarFunctionSignature {
    /// Create an exact function signature
    pub fn exact(params: Vec<LogicalTypeHandle>, return_type: LogicalTypeHandle) -> Self {
        Self {
            parameters: Some(ScalarParams::Exact(params)),
            return_type,
        }
    }

    /// Create a variadic function signature
    pub fn variadic(param: LogicalTypeHandle, return_type: LogicalTypeHandle) -> Self {
        Self {
            parameters: Some(ScalarParams::Variadic(param)),
            return_type,
        }
    }
}

impl ScalarFunctionSignature {
    pub(crate) fn register_with_scalar(&self, f: &ScalarFunction) {
        f.set_return_type(&self.return_type);

        match &self.parameters {
            Some(ScalarParams::Exact(params)) => {
                for param in params.iter() {
                    f.add_parameter(param);
                }
            }
            Some(ScalarParams::Variadic(param)) => {
                f.add_variadic_parameter(param);
            }
            None => {
                // do nothing
            }
        }
    }
}

/// An interface to store and retrieve data during the function execution stage
#[derive(Debug)]
struct ScalarFunctionInfo(duckdb_function_info);

impl From<duckdb_function_info> for ScalarFunctionInfo {
    fn from(ptr: duckdb_function_info) -> Self {
        Self(ptr)
    }
}

impl ScalarFunctionInfo {
    pub unsafe fn get_extra_info<T>(&self) -> &T {
        &*(duckdb_scalar_function_get_extra_info(self.0).cast())
    }

    pub unsafe fn set_error(&self, error: &str) {
        let c_str = CString::new(error).unwrap();
        duckdb_scalar_function_set_error(self.0, c_str.as_ptr());
    }
}

unsafe extern "C" fn scalar_func<T>(info: duckdb_function_info, input: duckdb_data_chunk, mut output: duckdb_vector)
where
    T: VScalar,
{
    let info = ScalarFunctionInfo::from(info);
    let mut input = DataChunkHandle::new_unowned(input);
    let result = T::invoke(info.get_extra_info(), &mut input, &mut output);
    if let Err(e) = result {
        info.set_error(&e.to_string());
    }
}

impl Connection {
    /// Register the given ScalarFunction with default state.
    #[inline]
    pub fn register_scalar_function<S: VScalar>(&self, name: &str) -> crate::Result<()>
    where
        S::State: Default,
    {
        let set = ScalarFunctionSet::new(name);
        for signature in S::signatures() {
            let scalar_function = ScalarFunction::new(name)?;
            signature.register_with_scalar(&scalar_function);
            scalar_function.set_function(Some(scalar_func::<S>));
            if S::volatile() {
                scalar_function.set_volatile();
            }
            scalar_function.set_extra_info(S::State::default());
            set.add_function(scalar_function)?;
        }
        self.db.borrow_mut().register_scalar_function_set(set)
    }

    /// Register the given ScalarFunction with custom state.
    ///
    /// The state is cloned once per function signature (overload) and stored in DuckDB's catalog.
    #[inline]
    pub fn register_scalar_function_with_state<S: VScalar>(&self, name: &str, state: &S::State) -> crate::Result<()>
    where
        S::State: Clone,
    {
        let set = ScalarFunctionSet::new(name);
        for signature in S::signatures() {
            let scalar_function = ScalarFunction::new(name)?;
            signature.register_with_scalar(&scalar_function);
            scalar_function.set_function(Some(scalar_func::<S>));
            if S::volatile() {
                scalar_function.set_volatile();
            }
            scalar_function.set_extra_info(state.clone());
            set.add_function(scalar_function)?;
        }
        self.db.borrow_mut().register_scalar_function_set(set)
    }
}

impl InnerConnection {
    /// Register the given ScalarFunction with the current db
    pub fn register_scalar_function_set(&mut self, f: ScalarFunctionSet) -> crate::Result<()> {
        f.register_with_connection(self.con)
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use arrow::array::Array;
    use libduckdb_sys::duckdb_string_t;

    use crate::{
        core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
        types::DuckString,
        vtab::arrow::WritableVector,
        Connection,
    };

    use super::{ScalarFunctionSignature, VScalar};

    struct ErrorScalar {}

    impl VScalar for ErrorScalar {
        type State = ();

        unsafe fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            _: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let mut msg = input.flat_vector(0).as_slice_with_len::<duckdb_string_t>(input.len())[0];
            let string = DuckString::new(&mut msg).as_str();
            Err(format!("Error: {string}").into())
        }

        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Varchar.into()],
                LogicalTypeId::Varchar.into(),
            )]
        }
    }

    #[derive(Debug, Clone)]
    struct TestState {
        multiplier: usize,
        prefix: String,
    }

    impl Default for TestState {
        fn default() -> Self {
            Self {
                multiplier: 3,
                prefix: "default".to_string(),
            }
        }
    }

    struct EchoScalar {}

    impl VScalar for EchoScalar {
        type State = TestState;

        unsafe fn invoke(
            state: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let values = input.flat_vector(0);
            let values = values.as_slice_with_len::<duckdb_string_t>(input.len());
            let strings = values
                .iter()
                .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string())
                .take(input.len());
            let output = output.flat_vector();

            for s in strings {
                let res = format!("{}: {}", state.prefix, s.repeat(state.multiplier));
                output.insert(0, res.as_str());
            }
            Ok(())
        }

        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![ScalarFunctionSignature::exact(
                vec![LogicalTypeId::Varchar.into()],
                LogicalTypeId::Varchar.into(),
            )]
        }
    }

    struct Repeat {}

    impl VScalar for Repeat {
        type State = ();

        unsafe fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let output = output.flat_vector();
            let counts = input.flat_vector(1);
            let values = input.flat_vector(0);
            let values = values.as_slice_with_len::<duckdb_string_t>(input.len());
            let strings = values
                .iter()
                .map(|ptr| DuckString::new(&mut { *ptr }).as_str().to_string());
            let counts = counts.as_slice_with_len::<i32>(input.len());
            for (count, value) in counts.iter().zip(strings).take(input.len()) {
                output.insert(0, value.repeat((*count) as usize).as_str());
            }

            Ok(())
        }

        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![ScalarFunctionSignature::exact(
                vec![
                    LogicalTypeHandle::from(LogicalTypeId::Varchar),
                    LogicalTypeHandle::from(LogicalTypeId::Integer),
                ],
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
            )]
        }
    }

    #[test]
    fn test_scalar() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;

        // Test with default state
        {
            conn.register_scalar_function::<EchoScalar>("echo")?;

            let mut stmt = conn.prepare("select echo('x')")?;
            let mut rows = stmt.query([])?;

            while let Some(row) = rows.next()? {
                let res: String = row.get(0)?;
                assert_eq!(res, "default: xxx");
            }
        }

        // Test with custom state
        {
            conn.register_scalar_function_with_state::<EchoScalar>(
                "echo2",
                &TestState {
                    multiplier: 5,
                    prefix: "custom".to_string(),
                },
            )?;

            let mut stmt = conn.prepare("select echo2('y')")?;
            let mut rows = stmt.query([])?;

            while let Some(row) = rows.next()? {
                let res: String = row.get(0)?;
                assert_eq!(res, "custom: yyyyy");
            }
        }

        Ok(())
    }

    #[test]
    fn test_scalar_error() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<ErrorScalar>("error_udf")?;

        let mut stmt = conn.prepare("select error_udf('blurg') as hello")?;
        if let Err(err) = stmt.query([]) {
            assert!(err.to_string().contains("Error: blurg"));
        } else {
            panic!("Expected an error");
        }

        Ok(())
    }

    #[test]
    fn test_repeat_scalar() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;
        conn.register_scalar_function::<Repeat>("nobie_repeat")?;

        let batches = conn
            .prepare("select nobie_repeat('Ho ho ho 🎅🎄', 3) as message from range(5)")?
            .query_arrow([])?
            .collect::<Vec<_>>();

        for batch in batches.iter() {
            let array = batch.column(0);
            let array = array.as_any().downcast_ref::<::arrow::array::StringArray>().unwrap();
            for i in 0..array.len() {
                assert_eq!(array.value(i), "Ho ho ho 🎅🎄Ho ho ho 🎅🎄Ho ho ho 🎅🎄");
            }
        }

        Ok(())
    }

    // Counters for testing volatile functions
    use std::sync::atomic::{AtomicU64, Ordering};
    static VOLATILE_COUNTER: AtomicU64 = AtomicU64::new(0);
    static NON_VOLATILE_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct CounterScalar {}

    impl VScalar for CounterScalar {
        type State = ();

        unsafe fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let len = input.len();
            let mut output_vec = output.flat_vector();
            let data = output_vec.as_mut_slice::<i64>();

            for item in data.iter_mut().take(len) {
                *item = NON_VOLATILE_COUNTER.fetch_add(1, Ordering::SeqCst) as i64;
            }
            Ok(())
        }

        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![ScalarFunctionSignature::exact(
                vec![],
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            )]
        }
    }

    struct VolatileCounterScalar {}

    impl VScalar for VolatileCounterScalar {
        type State = ();

        unsafe fn invoke(
            _: &Self::State,
            input: &mut DataChunkHandle,
            output: &mut dyn WritableVector,
        ) -> Result<(), Box<dyn std::error::Error>> {
            let len = input.len();
            let mut output_vec = output.flat_vector();
            let data = output_vec.as_mut_slice::<i64>();

            for item in data.iter_mut().take(len) {
                *item = VOLATILE_COUNTER.fetch_add(1, Ordering::SeqCst) as i64;
            }
            Ok(())
        }

        fn signatures() -> Vec<ScalarFunctionSignature> {
            vec![ScalarFunctionSignature::exact(
                vec![],
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
            )]
        }

        fn volatile() -> bool {
            true
        }
    }

    #[test]
    fn test_volatile_scalar() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;

        VOLATILE_COUNTER.store(0, Ordering::SeqCst);
        conn.register_scalar_function::<VolatileCounterScalar>("volatile_counter")?;

        let values: Vec<i64> = conn
            .prepare("SELECT volatile_counter() FROM generate_series(1, 5)")?
            .query_map([], |row| row.get(0))?
            .collect::<Result<_, _>>()?;

        assert_eq!(values, [0, 1, 2, 3, 4]);

        Ok(())
    }

    #[test]
    fn test_non_volatile_scalar() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;

        NON_VOLATILE_COUNTER.store(0, Ordering::SeqCst);
        conn.register_scalar_function::<CounterScalar>("non_volatile_counter")?;

        // Constant folding should make every row identical
        let distinct_count: i64 = conn
            .prepare("SELECT COUNT(DISTINCT non_volatile_counter()) FROM generate_series(1, 5)")?
            .query_row([], |row| row.get(0))?;

        assert_eq!(distinct_count, 1);

        Ok(())
    }
}
