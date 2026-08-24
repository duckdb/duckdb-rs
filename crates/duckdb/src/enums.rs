use crate::ffi;

#[repr(u32)]
/// How consistently a function maps the same inputs to the same result.
pub enum FunctionPropertyStability {
    /// The same inputs always produce the same result.
    Consistent = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT as u32,
    /// The result may differ for each row, as with `random()`.
    Volatile = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE as u32,
    /// The result is stable within one query but may change between queries.
    ConsistentWithinQuery = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY as u32,
}

impl From<FunctionPropertyStability> for ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
    fn from(value: FunctionPropertyStability) -> Self {
        match value {
            FunctionPropertyStability::Consistent => ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT,
            FunctionPropertyStability::Volatile => ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE,
            FunctionPropertyStability::ConsistentWithinQuery => ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY,
        }
    }
}

#[repr(u32)]
/// How a function handles collations on its arguments.
pub enum FunctionPropertyCollation {
    /// Combine input collations and propagate them to the result.
    Propagate = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE as u32,
    /// Apply combinable collations to inputs before invoking the function.
    PushCombineable = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE as u32,
    /// Ignore argument collations.
    Ignore = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE as u32,
}

impl From<FunctionPropertyCollation> for ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
    fn from(value: FunctionPropertyCollation) -> Self {
        match value {
            FunctionPropertyCollation::Propagate => ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE,
            FunctionPropertyCollation::PushCombineable => ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE,
            FunctionPropertyCollation::Ignore => ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE,
        }
    }
}

/// An optimizer property to apply when registering a function.
pub enum FunctionProperty {
    /// Set how stable the function's result is across rows and queries.
    Stability(FunctionPropertyStability),
    /// Whether the function receives rows containing null arguments.
    HasSpecialNullHandling(bool),
    /// Whether the function may raise a runtime error.
    IsFallible(bool),
    /// Set how the function handles argument collations.
    CollationHandling(FunctionPropertyCollation),
    /// Whether an aggregate's result depends on input order.
    AggregateOrderDependent(bool),
    /// Whether `DISTINCT` can affect an aggregate's result.
    AggregateDistinctDependent(bool),
}

impl From<FunctionProperty>
    for (
        ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY,
        ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE,
    )
{
    fn from(property: FunctionProperty) -> Self {
        match property {
            FunctionProperty::Stability(stability) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
                stability.into(),
            ),
            FunctionProperty::HasSpecialNullHandling(has_special_null_handling) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
                if has_special_null_handling {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL
                } else {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_DEFAULT
                },
            ),
            FunctionProperty::IsFallible(is_fallible) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY,
                if is_fallible {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_FALLIBLE
                } else {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_FALLIBILITY_INFALLIBLE
                },
            ),
            FunctionProperty::CollationHandling(collation_handling) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING,
                collation_handling.into(),
            ),
            FunctionProperty::AggregateOrderDependent(is_order_dependent) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
                if is_order_dependent {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES
                } else {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO
                },
            ),
            FunctionProperty::AggregateDistinctDependent(is_distinct_dependent) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT,
                if is_distinct_dependent {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_YES
                } else {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_AGG_DISTINCT_DEPENDENT_NO
                },
            ),
        }
    }
}
