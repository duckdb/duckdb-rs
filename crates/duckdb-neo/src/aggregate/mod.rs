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
    panic::AssertUnwindSafe,
};

use crate::{
    FromRaw,
    builder_helpers::ArgCount,
    ffi,
    handles::{AggregateFunctionBuilderHandle, AggregateFunctionBuilderLink},
    logical_type::LogicalType,
};

use crate::{
    Error, Result,
    bind_arguments::BindArguments,
    builder_helpers::{OpaqueHandle, get_bind_data, get_user_data, handle_unwind, into_opaque_eq},
    check_api_call,
    connection::{Context, FFILink},
    data_chunk::{RowCount, VectorCollection},
    enums::AggregateFunctionProperty,
    raw::RawExt,
    signature::SignatureBuilder,
    vector::{Access, Unknown, Vector},
};

/// Callback-scoped control over an aggregate function's resolved result type.
#[derive(Debug)]
pub struct ReturnTypeHandle<'a> {
    pub(crate) handle: &'a ffi::duckdb_v2_aggregate_function_bind_info_handle,
}

impl<'a> ReturnTypeHandle<'a> {
    /// Override the result type for the current bound call site.
    ///
    /// For example, a function declared to return `ANY` can derive a concrete
    /// type from its bound arguments. DuckDB copies the returned logical
    /// type, and the override is valid only during binding.
    pub fn override_return(&self, return_type: LogicalType) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_aggregate_function_bind_set_return_type,
            *self.handle,
            return_type.raw()
        )
    }
}

/// [`States`] is a view over the aggregate states DuckDB passes to a callback.
///
/// The same state is shared by every row of a group, so the underlying pointers may repeat.
/// Mutable access is therefore only offered one element at a time through [`IndexMut`].
#[derive(Debug)]
pub struct States<'a, T> {
    states: &'a [*mut T],
}

impl<'a, T> States<'a, T> {
    /// # Safety
    /// Pointers must be live, aligned, initialized `T`s with no external references throughout `'a`.
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
            states: self.states.iter(),
        }
    }
}

impl<'a, T> Index<usize> for States<'a, T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        // SAFETY: States::new guarantees live, aligned, initialized pointers.
        unsafe { &*self.states[index] }
    }
}

impl<'a, T> IndexMut<usize> for States<'a, T> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        // SAFETY: the exclusive view borrow prevents access through repeated pointers.
        unsafe { &mut *self.states[index] }
    }
}

/// Iterates over aggregate state references.
#[derive(Debug)]
pub struct StatesIter<'a, T> {
    states: std::slice::Iter<'a, *mut T>,
}

impl<'a, T> Iterator for StatesIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        // SAFETY: States::new requires each pointer to remain live during iteration.
        self.states.next().map(|&state| unsafe { &*state })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.states.size_hint()
    }
}

impl<T> DoubleEndedIterator for StatesIter<'_, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // SAFETY: States::new requires each pointer to remain live during iteration.
        self.states.next_back().map(|&state| unsafe { &*state })
    }
}

impl<T> ExactSizeIterator for StatesIter<'_, T> {}

impl<'s, T> IntoIterator for &'s States<'_, T> {
    type Item = &'s T;
    type IntoIter = StatesIter<'s, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe fn state_slice<'a, T>(ptr: *const T, count: usize) -> &'a [T] {
    if count == 0 || ptr.is_null() {
        &[]
    } else {
        // SAFETY: callers provide a valid state array for the callback's duration.
        unsafe { std::slice::from_raw_parts(ptr, count) }
    }
}

unsafe extern "C" fn bind_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_function_bind_info_handle,
    result: ffi::duckdb_v2_aggregate_function_bind_info_handle,
    context: ffi::duckdb_v2_context_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let arguments = BindArguments { handle: &info };

            let user_data = get_user_data!(ffi::duckdb_v2_function_bind_get_user_data, info);

            let result = T::bind(
                user_data,
                &unsafe { Context::from_raw(context) },
                arguments,
                ReturnTypeHandle { handle: &result },
            )?;

            check_api_call!(
                ffi::duckdb_v2_function_bind_set_bind_data,
                info,
                &mut into_opaque_eq(result)
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

            // States are 8-byte aligned, so `init` and `States` would access a more
            // strictly aligned `StateItem` through a misaligned pointer.
            let align = align_of::<T::StateItem>();
            if align > 8 {
                return Err(Error::api_error(format!(
                    "align_of::<StateItem>() ({align}) exceeds the 8-byte alignment of aggregate states"
                )));
            }

            // Window operators place states `size` bytes apart, so it must keep them aligned.
            let size = size.next_multiple_of(8);

            check_api_call!(ffi::duckdb_v2_aggregate_function_size_set_state_size, info, size as u64)
        },
        err,
    );
}

unsafe extern "C" fn init_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_init_info_handle,
    _err: *mut ffi::duckdb_v2_error_info_handle,
) {
    // DuckDB runs `destroy_callback` on every state even when init fails, and
    // that drops each state in place. Returning early would leave states
    // uninitialized, so any failure here (error or panic) aborts instead.
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_init_get_user_data, info);
        let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_init_get_bind_data, info);

        let state_count = check_api_call!(ffi::duckdb_v2_aggregate_function_init_get_state_count, info, RET)?;

        let states_ptr = check_api_call!(ffi::duckdb_v2_aggregate_function_init_get_states, info, RET)?;

        // SAFETY: DuckDB supplies state_count allocated state pointers for this callback.
        let states: &[*mut T::StateItem] =
            unsafe { state_slice(states_ptr as *const *mut T::StateItem, state_count as usize) };

        for &state_ptr in states {
            // SAFETY: size_callback checks size/alignment, and each fresh state is initialized once.
            unsafe {
                state_ptr.write(T::init(user_data, bind_data));
            }
        }

        Ok(())
    }));

    match result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            eprintln!("aggregate state initialization failed: {}", e.message);
            std::process::abort();
        }
        Err(_) => {
            eprintln!("aggregate state initialization panicked");
            std::process::abort();
        }
    }
}

unsafe extern "C" fn update_callback<T: AggregateCallbacks>(
    info: ffi::duckdb_v2_aggregate_function_update_info_handle,
    err: *mut ffi::duckdb_v2_error_info_handle,
) {
    handle_unwind(
        || {
            let user_data = get_user_data!(ffi::duckdb_v2_aggregate_function_update_get_user_data, info);

            let bind_data = get_bind_data!(ffi::duckdb_v2_aggregate_function_update_get_bind_data, info);

            let row_count =
                check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_row_count, info, RET)? as usize;

            let arg_count = ArgCount::from_func(|pf, pv, nf, nv| {
                check_api_call!(
                    ffi::duckdb_v2_aggregate_function_update_get_arg_count,
                    info,
                    pf,
                    pv,
                    nf,
                    nv
                )
            })?;

            let mut handles = Vec::with_capacity(arg_count.total());
            for i in 0..arg_count.total() {
                handles.push(check_api_call!(
                    ffi::duckdb_v2_aggregate_function_update_get_arg,
                    info,
                    i as u64,
                    RET
                )?);
            }
            let mut vector_collection = VectorCollection::new(handles, Access::Exclusive, RowCount::Fixed(row_count));

            let states_ptr = check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_states, info, RET)?;

            let row_count = check_api_call!(ffi::duckdb_v2_aggregate_function_update_get_row_count, info, RET)?;

            // SAFETY: DuckDB supplies row_count state pointers valid for this callback.
            let states_slice: &[*mut T::StateItem] =
                unsafe { state_slice(states_ptr as *const *mut T::StateItem, row_count as usize) };

            // SAFETY: init_callback initialized these states, which DuckDB exclusively lends here.
            let states: States<'_, T::StateItem> = unsafe { States::new(states_slice) };

            T::update(user_data, bind_data, &mut vector_collection, states)
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

            // SAFETY: DuckDB supplies count source state pointers valid for this callback.
            let source_slice: &[*mut T::StateItem] =
                unsafe { state_slice(source_ptr as *const *mut T::StateItem, count as usize) };

            // SAFETY: initialized source states are lent exclusively and are disjoint from targets.
            let source: States<'_, T::StateItem> = unsafe { States::new(source_slice) };

            // SAFETY: DuckDB supplies count target state pointers valid for this callback.
            let target_slice: &[*mut T::StateItem] =
                unsafe { state_slice(target_ptr as *const *mut T::StateItem, count as usize) };

            // SAFETY: initialized target states are lent exclusively and are disjoint from sources.
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

            // SAFETY: DuckDB lends count initialized, aligned states for shared access during finalization.
            let states: &[&T::StateItem] = unsafe { state_slice(states_ptr as *const &T::StateItem, count as usize) };

            let result_vector_handle =
                check_api_call!(ffi::duckdb_v2_aggregate_function_finalize_get_result, info, RET)?;

            let mut result_vector = Vector::from_handle(&result_vector_handle, Access::Writable)?;

            let result_offset =
                check_api_call!(ffi::duckdb_v2_aggregate_function_finalize_get_result_offset, info, RET)? as usize;

            // Window operators finalize into a result vector whose size they set
            // only afterwards, so grow it to cover the rows written here.
            let required_len = result_offset + states.len();
            if result_vector.len() < required_len {
                result_vector.set_size(required_len)?;
            }

            T::finalize(user_data, bind_data, states, result_vector, result_offset)
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

            // SAFETY: DuckDB supplies states_count state pointers valid for this callback.
            let states: &[*mut T::StateItem] =
                unsafe { state_slice(states_ptr as *const *mut T::StateItem, states_count as usize) };

            // SAFETY: the initialized states are exclusively lent to their final destruction callback.
            let states: States<'_, <T as AggregateCallbacks>::StateItem> = unsafe { States::new(states) };

            let user_destroy = T::destroy(user_data, bind_data, &states);

            if !matches!(user_destroy, Ok(true)) {
                for state in states.as_ptrs() {
                    // SAFETY: DuckDB supplies each initialized state once; user_destroy did not claim cleanup.
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
#[derive(Debug)]
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
    pub fn set_property(mut self, item: impl Into<AggregateFunctionProperty>) -> Self {
        let (key, value) = item.into().into();
        self.properties.insert(key, value);
        self
    }

    fn build(&mut self, handle: &AggregateFunctionBuilderHandle, link: &dyn FFILink) -> Result<()> {
        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_name,
            handle.raw(),
            &mut (&self.name).into()
        )?;

        let signature = check_api_call!(ffi::duckdb_v2_aggregate_function_get_signature, handle.raw(), RET)?;

        let docs = check_api_call!(ffi::duckdb_v2_aggregate_function_get_docs, handle.raw(), RET)?;

        std::mem::take(&mut self.signature).build(link, &signature, &docs)?;

        for (key, value) in &self.properties {
            check_api_call!(
                ffi::duckdb_v2_aggregate_function_set_property,
                handle.raw(),
                *key,
                *value
            )?;
        }

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_user_data,
            handle.raw(),
            &mut self.user_data.to_handle()
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_bind_callback,
            handle.raw(),
            Some(bind_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_init_callback,
            handle.raw(),
            Some(init_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_size_callback,
            handle.raw(),
            Some(size_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_update_callback,
            handle.raw(),
            Some(update_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_combine_callback,
            handle.raw(),
            Some(combine_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_finalize_callback,
            handle.raw(),
            Some(finalize_callback::<T>)
        )?;

        check_api_call!(
            ffi::duckdb_v2_aggregate_function_set_destroy_callback,
            handle.raw(),
            Some(destroy_callback::<T>)
        )
    }

    /// Register through a connection or extension, consuming the builder.
    #[allow(private_bounds)]
    pub fn register<C: AggregateFunctionBuilderLink + FFILink>(mut self, link: &C) -> Result<()> {
        let handle = link.create_aggregate_function_handle()?;

        self.build(&handle, link)?;

        link.register_aggregate_function(handle)
    }
}

/// Callback lifecycle for a user-defined aggregate function.
///
/// DuckDB binds each call site, creates one state per group, updates states
/// from input batches, combines partial states, and finalizes them into result
/// vectors. [`Self::destroy`] may override automatic Rust state cleanup.
pub trait AggregateCallbacks: Send + Sync + 'static {
    /// Data shared from binding through execution.
    type BindData: Any + Send + Sync + PartialEq;
    /// Mutable state stored for each aggregate group.
    type StateItem: Any + Send + Sync;

    /// **Bind:** validate a call site and create data shared by later phases.
    fn bind(
        &self,
        context: &Context,
        arguments: BindArguments<'_>,
        result_type_handle: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData>;

    /// **Size:** return the allocation size of one aggregate state.
    ///
    /// May be larger than `size_of::<StateItem>()` but never smaller; a smaller
    /// size fails the query. It is rounded up to a multiple of 8.
    fn size(&self, _bind_data: Option<&Self::BindData>) -> Result<usize> {
        Ok(size_of::<Self::StateItem>())
    }

    /// **Initialize:** create one empty aggregate state.
    ///
    /// Infallible: validate in [`Self::bind`] instead. A panic here aborts the
    /// process, because DuckDB destroys every state afterwards and a state left
    /// uninitialized cannot be dropped safely.
    fn init(&self, bind_data: Option<&Self::BindData>) -> Self::StateItem;

    /// **Update:** input row `i` belongs to `states[i]`; rows in one group share a state.
    /// Repeated states allow mutable access only one at a time through [`States`]' [`IndexMut`].
    fn update(
        &self,
        bind_data: Option<&Self::BindData>,
        data: &mut VectorCollection,
        states: States<'_, Self::StateItem>,
    ) -> Result<()>;

    /// **Combine:** merge partial source states into target states.
    ///
    /// The source view is read-only; the target view offers mutable access one
    /// state at a time through [`States`]' [`IndexMut`]
    /// implementation.
    fn combine(
        &self,
        bind_data: Option<&Self::BindData>,
        source: &States<'_, Self::StateItem>,
        target: States<'_, Self::StateItem>,
    ) -> Result<()>;

    /// **Finalize:** write aggregate states to the result vector.
    ///
    /// State `i` belongs in row `result_offset + i`; the result vector always
    /// covers `result_offset + states.len()` rows.
    fn finalize(
        &self,
        bind_data: Option<&Self::BindData>,
        states: &[&Self::StateItem],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> Result<()>;

    /// Return `Ok(true)` only after destroying every state to skip automatic cleanup.
    /// An `Err` is ignored but still cleans up; a panic skips cleanup and leaks the states.
    fn destroy(&self, _bind_data: Option<&Self::BindData>, _states: &States<'_, Self::StateItem>) -> Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;
