//! User-defined aggregate functions and their execution state.
//!
//! Implement [`AggregateCallbacks`] to define binding, state initialization,
//! update, combine, finalization, and optional destruction. Register the
//! implementation with [`AggregateFunctionBuilder`]. During execution,
//! [`States`] provides borrowed access to DuckDB's per-group state pointers.

use std::{
    any::Any,
    collections::HashMap,
    ops::{Index, IndexMut},
};

use crate::ffi;

use crate::{
    Error, Result,
    bind_arguments::{BindArgument, BindMetadata, BindType},
    builder_helpers::{OpaqueHandle, get_bind_data, get_user_data, handle_unwind, into_opaque},
    check_api_call,
    connection::Context,
    data_chunk::VectorCollection,
    enums::FunctionProperty,
    handles::{AggregateFunctionBuilderHandle, AggregateFunctionBuilderLink},
    scalar::{FunctionBindHandles, ReturnTypeHandle},
    signature::SignatureBuilder,
    vector::{Unknown, Vector, VectorElement},
};

/// [`States`] is a view over the aggregate states DuckDB passes to a callback.
///
/// The same state is shared by every row of a group, so the underlying pointers may repeat.
/// Mutable access is therefore only offered one element at a time through [`IndexMut`].
pub struct States<'a, T> {
    states: &'a [*mut T],
}

impl<'a, T> States<'a, T> {
    /// # Safety
    ///
    /// Every pointer in `states` must point to a live, initialized `T` for the duration of `'a`.
    pub(crate) unsafe fn new(states: &'a [*mut T]) -> Self {
        States { states }
    }

    /// The number of states in the view. This matches the number of rows or groups DuckDB supplied.
    pub fn len(&self) -> usize {
        self.states.len()
    }

    /// Whether the view is empty.
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }

    /// The raw state pointers backing the view.
    pub fn as_ptrs(&self) -> &'a [*mut T] {
        self.states
    }

    /// Iterates over shared references to the aggregate states.
    pub fn iter(&self) -> StatesIter<'_, T> {
        StatesIter {
            states: self.states,
            index: 0,
        }
    }
}

impl<'a, T> Index<usize> for States<'a, T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        unsafe { &*self.states[index] }
    }
}

impl<'a, T> IndexMut<usize> for States<'a, T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        unsafe { &mut *self.states[index] }
    }
}

/// Iterates over aggregate state references.
pub struct StatesIter<'a, T> {
    states: &'a [*mut T],
    index: usize,
}

impl<'a, T> Iterator for StatesIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.states.len() {
            let state = unsafe { &*self.states[self.index] };
            self.index += 1;
            Some(state)
        } else {
            None
        }
    }
}

unsafe extern "C" fn bind_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let metadata = BindMetadata {
                bind_type: BindType::Aggregate(&info),
            };

            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_bind_get_user_data, info);

            let result = T::bind(
                user_data,
                Context(context),
                metadata.get_arguments()?,
                ReturnTypeHandle {
                    handle: FunctionBindHandles::Aggregate(&info),
                },
            )?;

            check_api_call!(
                ffi::duckdb_v2_aggregate_function_bind_set_bind_data,
                info,
                &mut into_opaque(result)
            )
        },
        err,
    );
}

unsafe extern "C" fn size_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_size_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_size_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_size_get_bind_data, info);

            let size = T::size(user_data, bind_data)?;

            // `init` writes a full `StateItem` into every state, so a smaller state would overflow.
            let min_size = size_of::<T::StateItem>();
            if size < min_size {
                return Err(Error::api_error(format!(
                    "aggregate state size {size} is smaller than size_of::<StateItem>() ({min_size})"
                )));
            }

            check_api_call!(ffi::duckdb_v2_aggregate_function_size_set_state_size, info, size as u64)
        },
        err,
    );
}

unsafe extern "C" fn init_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_init_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_init_get_user_data, info);
            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_init_get_bind_data, info);

            let state_count = check_api_call!(ffi::duckdb_v2_aggregate_function_init_get_state_count, info, RET)?;

            let states_ptr = check_api_call!(ffi::duckdb_v2_aggregate_function_init_get_states, info, RET)?;

            let states: &[*mut T::StateItem] =
                unsafe { std::slice::from_raw_parts(states_ptr as *mut *mut T::StateItem, state_count as usize) };

            for &state_ptr in states {
                let data = T::init(user_data, bind_data)?;

                unsafe {
                    state_ptr.write(data);
                }
            }

            Ok(())
        },
        err,
    );
}

unsafe extern "C" fn update_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_update_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_update_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_update_get_bind_data, info);

            let arg_count =
                check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_arg_count, info, RET)? as usize;
            let row_count =
                check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_row_count, info, RET)? as usize;

            let mut vector_collection = VectorCollection {
                handles: Vec::with_capacity(arg_count),
                is_writable: false,
                row_count,
            };

            for i in 0..arg_count {
                vector_collection.handles.push(check_api_call!(
                    ffi::duckdb_v2_aggregate_function_update_get_arg,
                    info,
                    i as u32,
                    RET
                )?);
            }

            let states_ptr = check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_states, info, RET)?;

            let row_count = check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_row_count, info, RET)?;

            let states_slice: &[*mut T::StateItem] =
                unsafe { std::slice::from_raw_parts(states_ptr as *const *mut T::StateItem, row_count as usize) };

            let states: States<'_, T::StateItem> = unsafe { States::new(states_slice) };

            T::update(user_data, bind_data, vector_collection, states)
        },
        err,
    );
}

unsafe extern "C" fn combine_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_combine_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_combine_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_combine_get_bind_data, info);

            let count: u64 = check_api_call!(ffi::duckdb_v2_aggregate_function_combine_get_state_count, info, RET)?;

            let source_ptr = check_api_call!(ffi::duckdb_v2_aggregate_function_combine_get_sources, info, RET)?;

            let target_ptr = check_api_call!(ffi::duckdb_v2_aggregate_function_combine_get_targets, info, RET)?;

            let source_slice: &[*mut T::StateItem] =
                unsafe { std::slice::from_raw_parts(source_ptr as *const *mut T::StateItem, count as usize) };

            let source: States<'_, T::StateItem> = unsafe { States::new(source_slice) };

            let target_slice: &[*mut T::StateItem] =
                unsafe { std::slice::from_raw_parts(target_ptr as *const *mut T::StateItem, count as usize) };

            let target: States<'_, T::StateItem> = unsafe { States::new(target_slice) };

            T::combine(user_data, bind_data, &source, target)
        },
        err,
    );
}

unsafe extern "C" fn finalize_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_finalize_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_finalize_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_finalize_get_bind_data, info);

            let count: u64 = check_api_call!(ffi::duckdb_v2_aggregate_function_finalize_get_state_count, info, RET)?;

            let states_ptr: *mut *mut std::ffi::c_void =
                check_api_call!(ffi::duckdb_v2_aggregate_function_finalize_get_states, info, RET)?;

            let states: &[&T::StateItem] =
                unsafe { std::slice::from_raw_parts(states_ptr as *const &T::StateItem, count as usize) };

            let result_vector_handle =
                check_api_call!(ffi::duckdb_v2_aggregate_function_finalize_get_result, info, RET)?;

            let result_vector = Vector::from_handle(&result_vector_handle, true)?;

            let result_offset: u64 =
                check_api_call!(ffi::duckdb_v2_aggregate_function_finalize_get_result_offset, info, RET)?;

            T::finalize(user_data, bind_data, states, result_vector, result_offset as usize)
        },
        err,
    );
}

unsafe extern "C" fn destroy_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_destroy_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_destroy_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_destroy_get_bind_data, info);

            let states_count: u64 =
                check_api_call!(ffi::duckdb_v2_aggregate_function_destroy_get_state_count, info, RET)?;

            let states_ptr: *mut *mut std::ffi::c_void =
                check_api_call!(ffi::duckdb_v2_aggregate_function_destroy_get_states, info, RET)?;

            let states: &[*mut T::StateItem] =
                unsafe { std::slice::from_raw_parts(states_ptr as *mut *mut T::StateItem, states_count as usize) };

            let states: States<'_, <T as AggregateCallbacks>::StateItem> = unsafe { States::new(states) };

            let user_destroy = T::destroy(user_data, bind_data, &states);

            if !matches!(user_destroy, Ok(true)) {
                for state in states.as_ptrs() {
                    unsafe {
                        state.drop_in_place();
                    }
                }
            }

            user_destroy.map(|_| ())
        },
        err,
    );
}

/// Builds and registers a user-defined aggregate function.
pub struct AggregateFunctionBuilder<T: AggregateCallbacks> {
    name: String,
    signature: SignatureBuilder,
    properties: HashMap<ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY, ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE>,
    user_data: OpaqueHandle<T>,
}

impl<T: AggregateCallbacks> AggregateFunctionBuilder<T> {
    /// Create a builder from a name, signature, and callback implementation.
    pub fn new(name: impl Into<String>, signature: SignatureBuilder, implementation: T) -> Self {
        Self {
            name: name.into(),
            signature,
            properties: HashMap::new(),
            user_data: OpaqueHandle::new(implementation),
        }
    }

    /// Set a DuckDB function property.
    pub fn set_property(mut self, item: FunctionProperty) -> Self {
        let (key, value) = item.into();
        self.properties.insert(key, value);
        self
    }

    fn build(&self, handle: &AggregateFunctionBuilderHandle) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_name,
            **handle,
            &mut (&self.name).into()
        )?;

        let signature = check_api_call!(ffi::duckdb_v2_aggregate_function_get_signature, **handle, RET)?;

        self.signature.build(&signature)?;

        for (key, value) in &self.properties {
            check_api_call!(ffi::duckdb_v2_aggregate_function_set_property, **handle, *key, *value)?;
        }

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_user_data,
            **handle,
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_bind_callback,
            **handle,
            Some(bind_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_init_callback,
            **handle,
            Some(init_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_size_callback,
            **handle,
            Some(size_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_update_callback,
            **handle,
            Some(update_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_combine_callback,
            **handle,
            Some(combine_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_finalize_callback,
            **handle,
            Some(finalize_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_destroy_callback,
            **handle,
            Some(destroy_callback::<T>)
        )?;

        Ok(())
    }

    /// Register through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: AggregateFunctionBuilderLink>(self, link: &C) -> Result<()> {
        let handle = link.create_aggregate_function_handle()?;
        self.build(&handle)?;

        check_api_call!(ffi::duckdb_v2_aggregate_function_register, *handle)
    }
}

/// Callback lifecycle for a user-defined aggregate function.
///
/// DuckDB binds each call site, creates one state per group, updates states
/// from input batches, combines partial states, and finalizes them into result
/// vectors. [`Self::destroy`] may override automatic Rust state cleanup.
pub trait AggregateCallbacks: Send + Sync + 'static {
    /// Data shared from binding through execution.
    type BindData: Any + Send + Sync;
    /// Mutable state stored for each aggregate group.
    type StateItem: Any + Send + Sync;
    /// The aggregate's declared input element type.
    type IncomingType: VectorElement;

    /// **Bind:** validate a call site and create data shared by later phases.
    fn bind(
        &self,
        context: Context,
        metadata: Vec<BindArgument>,
        result_type_handle: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData>;

    /// **Size:** return the allocation size of one aggregate state.
    ///
    /// May be larger than `size_of::<StateItem>()` but never smaller; a smaller
    /// size fails the query.
    fn size(&self, _bind_data: Option<&Self::BindData>) -> Result<usize> {
        Ok(size_of::<Self::StateItem>())
    }

    /// **Initialize:** create one empty aggregate state.
    fn init(&self, bind_data: Option<&Self::BindData>) -> Result<Self::StateItem>;

    /// **Update:** apply an input batch to its corresponding aggregate states.
    ///
    /// Rows of the same group share a state, so the view may contain the same
    /// state more than once. Mutable access is offered one state at a time
    /// through [`States`]' [`IndexMut`](std::ops::IndexMut) implementation.
    fn update(
        &self,
        bind_data: Option<&Self::BindData>,
        data: VectorCollection,
        states: States<'_, Self::StateItem>,
    ) -> Result<()>;
    /// **Combine:** merge partial source states into target states.
    ///
    /// The source view is read-only; the target view offers mutable access one
    /// state at a time through [`States`]' [`IndexMut`](std::ops::IndexMut)
    /// implementation.
    fn combine(
        &self,
        bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        target: States<'_, Self::StateItem>,
    ) -> Result<()>;
    /// **Finalize:** write aggregate states to the result vector.
    fn finalize(
        &self,
        bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> Result<()>;

    /// **Destroy:** optionally clean up states manually.
    ///
    /// Return `true` only after destroying every state to skip automatic Rust
    /// cleanup.
    fn destroy(&self, _bind_data: Option<&Self::BindData>, _states: &States<'_, Self::StateItem>) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
