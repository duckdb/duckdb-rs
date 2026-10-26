//! Enumerations mirrored from the C API, including token classes and function properties.

use crate::{builder_helpers::ffi_enum_redeclaration, ffi};

ffi_enum_redeclaration! {
    /// DuckDB's logical type identifier, independent of type parameters and aliases.
    pub enum LogicalTypeID <- ffi::DUCKDB_V2_LOGICAL_TYPE_ID {
        /// Invalid or unset type.
        Invalid = DUCKDB_V2_LOGICAL_TYPE_ID_INVALID,
        /// The type of a NULL constant.
        SqlNull = DUCKDB_V2_LOGICAL_TYPE_ID_SQLNULL,
        /// An unresolved parameter's type.
        Unknown = DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN,
        /// A function-signature wildcard accepting any type.
        Any = DUCKDB_V2_LOGICAL_TYPE_ID_ANY,
        /// A logical type carried as a value.
        Type = DUCKDB_V2_LOGICAL_TYPE_ID_TYPE,
        Boolean = DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN,
        TinyInt = DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT,
        SmallInt = DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT,
        Integer = DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER,
        BigInt = DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
        Date = DUCKDB_V2_LOGICAL_TYPE_ID_DATE,
        Time = DUCKDB_V2_LOGICAL_TYPE_ID_TIME,
        TimestampSec = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC,
        TimestampMs = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS,
        Timestamp = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP,
        TimestampNs = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS,
        Decimal = DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL,
        Float = DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT,
        Double = DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
        Varchar = DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR,
        Blob = DUCKDB_V2_LOGICAL_TYPE_ID_BLOB,
        Interval = DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL,
        UTinyInt = DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT,
        USmallInt = DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT,
        UInteger = DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER,
        UBigInt = DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT,
        TimestampTz = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ,
        TimestampTzNs = DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS,
        TimeTz = DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ,
        TimeNs = DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS,
        Bit = DUCKDB_V2_LOGICAL_TYPE_ID_BIT,
        BigNum = DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM,
        UHugeInt = DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT,
        HugeInt = DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT,
        Uuid = DUCKDB_V2_LOGICAL_TYPE_ID_UUID,
        Geometry = DUCKDB_V2_LOGICAL_TYPE_ID_GEOMETRY,
        Struct = DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT,
        List = DUCKDB_V2_LOGICAL_TYPE_ID_LIST,
        Map = DUCKDB_V2_LOGICAL_TYPE_ID_MAP,
        Enum = DUCKDB_V2_LOGICAL_TYPE_ID_ENUM,
        Union = DUCKDB_V2_LOGICAL_TYPE_ID_UNION,
        Array = DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY,
        Variant = DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT,
        /// An unnamed struct with positional fields.
        Tuple = DUCKDB_V2_LOGICAL_TYPE_ID_TUPLE,
    }
}

ffi_enum_redeclaration! {
    /// The lexical class of a token, as reported by [`SqlTokenIterator`](crate::tokens::SqlTokenIterator).
    pub enum TokenType <- ffi::DUCKDB_V2_TOKEN_TYPE {
        /// FFI failure sentinel; [`SqlTokenIterator`](crate::tokens::SqlTokenIterator) returns an error instead.
        Invalid = DUCKDB_V2_TOKEN_TYPE_INVALID,
        /// A keyword of the connection's grammar.
        Keyword = DUCKDB_V2_TOKEN_TYPE_KEYWORD,
        /// A bare or double-quoted identifier, quotes included.
        Identifier = DUCKDB_V2_TOKEN_TYPE_IDENTIFIER,
        /// A quoted or dollar-quoted string, delimiters included.
        StringLiteral = DUCKDB_V2_TOKEN_TYPE_STRING_LITERAL,
        /// A numeric literal.
        NumberLiteral = DUCKDB_V2_TOKEN_TYPE_NUMBER_LITERAL,
        /// An operator or punctuation run other than the statement terminator.
        Operator = DUCKDB_V2_TOKEN_TYPE_OPERATOR,
        /// A line or block comment, delimiters included.
        Comment = DUCKDB_V2_TOKEN_TYPE_COMMENT,
        /// A statement-terminating semicolon.
        Terminator = DUCKDB_V2_TOKEN_TYPE_TERMINATOR,
        /// FFI exhaustion sentinel; [`SqlTokenIterator`](crate::tokens::SqlTokenIterator) returns `None` instead.
        EndOfInput = DUCKDB_V2_TOKEN_TYPE_END_OF_INPUT,
    }
}

#[repr(u32)]
/// How consistently a function maps the same inputs to the same result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionPropertyStability {
    /// The same inputs always produce the same result.
    Consistent = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT as u32,
    /// The result may differ for each row, as with `random()`.
    Volatile = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE as u32,
    /// The result is stable within one query but may change between queries.
    ConsistentWithinQuery =
        ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY as u32,
}

impl From<FunctionPropertyStability> for ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
    fn from(value: FunctionPropertyStability) -> Self {
        match value {
            FunctionPropertyStability::Consistent => {
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT
            }
            FunctionPropertyStability::Volatile => {
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE
            }
            FunctionPropertyStability::ConsistentWithinQuery => {
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_CONSISTENT_WITHIN_QUERY
            }
        }
    }
}

#[repr(u32)]
/// How a function handles collations on its arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionPropertyCollation {
    /// Combine input collations and propagate them to the result.
    Propagate = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE as u32,
    /// Apply combinable collations to inputs before invoking the function.
    PushCombinable =
        ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE as u32,
    /// Ignore argument collations.
    Ignore = ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE as u32,
}

impl From<FunctionPropertyCollation> for ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE {
    fn from(value: FunctionPropertyCollation) -> Self {
        match value {
            FunctionPropertyCollation::Propagate => {
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PROPAGATE
            }
            FunctionPropertyCollation::PushCombinable => {
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE
            }
            FunctionPropertyCollation::Ignore => {
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_IGNORE
            }
        }
    }
}

/// An optimizer property to apply when registering a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionProperty {
    /// Set result stability across rows and queries; defaults to [`FunctionPropertyStability::Consistent`].
    Stability(FunctionPropertyStability),
    /// Whether null arguments reach the function; defaults to `false` (NULL in, NULL out).
    HasSpecialNullHandling(bool),
    /// Whether the function may raise runtime errors; defaults to `true`.
    /// Setting `false` promises callbacks never return `Err` or panic; doing so becomes an internal error.
    IsFallible(bool),
    /// Set argument collation handling; defaults to [`FunctionPropertyCollation::Propagate`].
    CollationHandling(FunctionPropertyCollation),
}

/// An optimizer property to apply when registering an aggregate function.
///
/// Wraps the shared [`FunctionProperty`] values and adds the aggregate-only ones,
/// which DuckDB rejects on other function types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggregateFunctionProperty {
    /// A property shared by every function type.
    Shared(FunctionProperty),
    /// Whether an aggregate's result depends on input order; defaults to `true`.
    AggregateOrderDependent(bool),
    /// Whether `DISTINCT` can affect an aggregate's result; defaults to `true`.
    AggregateDistinctDependent(bool),
}

impl From<FunctionProperty> for AggregateFunctionProperty {
    fn from(property: FunctionProperty) -> Self {
        AggregateFunctionProperty::Shared(property)
    }
}

impl From<AggregateFunctionProperty>
    for (
        ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY,
        ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE,
    )
{
    fn from(property: AggregateFunctionProperty) -> Self {
        match property {
            AggregateFunctionProperty::Shared(property) => property.into(),
            AggregateFunctionProperty::AggregateOrderDependent(is_order_dependent) => (
                ffi::DUCKDB_V2_FUNCTION_PROPERTY_KEY::DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
                if is_order_dependent {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES
                } else {
                    ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_NO
                },
            ),
            AggregateFunctionProperty::AggregateDistinctDependent(is_distinct_dependent) => (
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_logical_type_id_round_trip() -> crate::Result<()> {
        use LogicalTypeID::*;

        for id in [
            Invalid,
            SqlNull,
            Unknown,
            Any,
            Type,
            Boolean,
            TinyInt,
            SmallInt,
            Integer,
            BigInt,
            Date,
            Time,
            TimestampSec,
            TimestampMs,
            Timestamp,
            TimestampNs,
            Decimal,
            Float,
            Double,
            Varchar,
            Blob,
            Interval,
            UTinyInt,
            USmallInt,
            UInteger,
            UBigInt,
            TimestampTz,
            TimestampTzNs,
            TimeTz,
            TimeNs,
            Bit,
            BigNum,
            UHugeInt,
            HugeInt,
            Uuid,
            Geometry,
            Struct,
            List,
            Map,
            Enum,
            Union,
            Array,
            Variant,
            Tuple,
        ] {
            let raw = ffi::DUCKDB_V2_LOGICAL_TYPE_ID::from(id);
            assert_eq!(id as u32, raw as u32);
            assert_eq!(LogicalTypeID::try_from(raw)?, id);
        }
        Ok(())
    }

    #[test]
    fn test_logical_type_id_rejects_ffi_sentinel() {
        let error =
            LogicalTypeID::try_from(ffi::DUCKDB_V2_LOGICAL_TYPE_ID::DUCKDB_V2_LOGICAL_TYPE_ID_MAX_ENUM).unwrap_err();
        assert!(error.to_string().contains("Unknown LogicalTypeID"));
    }

    #[test]
    fn test_push_combinable() {
        assert_eq!(
            ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::from(FunctionPropertyCollation::PushCombinable),
            ffi::DUCKDB_V2_FUNCTION_PROPERTY_VALUE::DUCKDB_V2_FUNCTION_PROPERTY_COLLATION_HANDLING_PUSH_COMBINABLE
        );
    }

    #[test]
    fn test_property_traits() {
        fn assert_traits<T: Copy + Eq + std::hash::Hash>() {}

        assert_traits::<LogicalTypeID>();
        assert_traits::<FunctionPropertyStability>();
        assert_traits::<FunctionPropertyCollation>();
        assert_traits::<FunctionProperty>();
        assert_traits::<AggregateFunctionProperty>();
    }
}
