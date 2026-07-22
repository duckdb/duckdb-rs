//! Registered scalar functions implemented in Rust.
//!
//! DuckDB invokes these functions through C callbacks. A Rust panic must not
//! unwind through that interface: without containment, a faulty function can
//! abort the host process instead of failing its query. The registration
//! helpers in this module therefore convert callback panics and ordinary
//! errors into DuckDB query errors. They also contain failures while formatting
//! errors or destroying registered state, allowing later queries to keep using
//! the connection.
//!
//! These guarantees apply to functions registered through
//! [`Connection::register_scalar_function`] or
//! [`Connection::register_scalar_function_with_state`]. A panic can poison or
//! otherwise damage interior mutable [`VScalar::State`](crate::vscalar::VScalar::State),
//! which is reused by later invocations, so implementations must preserve or
//! restore its invariants during unwinding. Panics from state destruction are
//! contained and logged to standard error because DuckDB's destructor callback
//! cannot return an error. The panic hook runs before containment and may also
//! write to standard error; applications can customize it with
//! [`std::panic::set_hook`]. These guarantees require unwinding; a
//! `panic = "abort"` build aborts first.

use libduckdb_sys::{
    duckdb_data_chunk, duckdb_function_info, duckdb_scalar_function_get_extra_info, duckdb_scalar_function_set_error,
    duckdb_vector,
};
use std::ffi::CStr;

use crate::{
    Connection,
    arrow_interop::WritableVector,
    callback::{CallbackErrorSink, contain_callback},
    core::{DataChunkHandle, LogicalTypeHandle},
    inner_connection::InnerConnection,
};

use self::function::{ScalarFunction, ScalarFunctionSet};

mod function;

/// The duckdb Arrow scalar function interface
#[cfg(feature = "vscalar-arrow")]
pub mod arrow;

#[cfg(feature = "vscalar-arrow")]
pub use arrow::{ArrowFunctionSignature, ArrowScalarParams, VArrowScalar};

/// Duckdb scalar function trait
pub trait VScalar: Sized {
    /// State set at registration time. Persists for the lifetime of the catalog entry.
    /// Shared across worker threads and invocations; any interior mutation must be synchronized.
    /// Must be `'static` as it is stored in DuckDB and may outlive the current stack frame.
    type State: Sized + Send + Sync + 'static;
    /// The actual function.
    ///
    /// DuckDB guarantees that `input` and `output` stay live for the duration
    /// of this call. Implementations must populate `output` for rows
    /// `0..input.len()` and must not read or write beyond that range.
    ///
    /// This mirrors [`VTab::func`](crate::vtab::VTab::func): the method itself
    /// is safe, but `input` and `output` are accessed through the vector
    /// wrappers in [`crate::core`], several of whose accessors are `unsafe`.
    ///
    /// # Working with vectors
    ///
    /// When reaching for those `unsafe` accessors, implementations must uphold
    /// their contracts:
    ///
    /// - only read and write within the rows and column types DuckDB provided
    ///   for this invocation;
    /// - not retain `input`, `output`, or any vector/slice derived from them
    ///   past return;
    /// - not hold two writable wrappers over the same column at the same
    ///   time. The column accessors (`flat_vector`, `list_vector`, ...) take
    ///   `&self`, so safe code can obtain two wrappers over one column and
    ///   call `as_mut_slice` on each, yielding overlapping `&mut [T]` —
    ///   undefined behavior.
    ///
    /// See the callback containment contract in [`crate::vscalar`].
    fn invoke(
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
        unsafe { &*(duckdb_scalar_function_get_extra_info(self.0).cast()) }
    }
}

impl CallbackErrorSink for ScalarFunctionInfo {
    fn set_c_error(&self, error: &CStr) {
        unsafe { duckdb_scalar_function_set_error(self.0, error.as_ptr()) };
    }
}

unsafe extern "C" fn scalar_func<T>(info: duckdb_function_info, input: duckdb_data_chunk, mut output: duckdb_vector)
where
    T: VScalar,
{
    let info = ScalarFunctionInfo::from(info);
    contain_callback(&info, || unsafe {
        let mut input = DataChunkHandle::new_unowned(input);
        T::invoke(info.get_extra_info(), &mut input, &mut output)
    });
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
mod tests;
