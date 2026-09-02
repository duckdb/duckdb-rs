//! Read-only introspection of bound expression trees.

use libduckdb_sys as ffi;

use crate::{Result, builder_helpers::ffi_enum_redeclaration, check_api_call, logical_type::LogicalType, value::Value};

ffi_enum_redeclaration! {
    /// The concrete implementation class of an expression node.
    ///
    /// Only a subset of this enum appears in callback trees exposed by this API.
    #[allow(missing_docs)]
    pub enum ExpressionClass <- ffi::DUCKDB_V2_EXPRESSION_CLASS {
    Invalid = DUCKDB_V2_EXPRESSION_CLASS_INVALID,
    Aggregate = DUCKDB_V2_EXPRESSION_CLASS_AGGREGATE,
    Case = DUCKDB_V2_EXPRESSION_CLASS_CASE,
    Cast = DUCKDB_V2_EXPRESSION_CLASS_CAST,
    ColumnRef = DUCKDB_V2_EXPRESSION_CLASS_COLUMN_REF,
    Comparison = DUCKDB_V2_EXPRESSION_CLASS_COMPARISON,
    Conjunction = DUCKDB_V2_EXPRESSION_CLASS_CONJUNCTION,
    Constant = DUCKDB_V2_EXPRESSION_CLASS_CONSTANT,
    Default = DUCKDB_V2_EXPRESSION_CLASS_DEFAULT,
    Function = DUCKDB_V2_EXPRESSION_CLASS_FUNCTION,
    Operator = DUCKDB_V2_EXPRESSION_CLASS_OPERATOR,
    Star = DUCKDB_V2_EXPRESSION_CLASS_STAR,
    Subquery = DUCKDB_V2_EXPRESSION_CLASS_SUBQUERY,
    Window = DUCKDB_V2_EXPRESSION_CLASS_WINDOW,
    Parameter = DUCKDB_V2_EXPRESSION_CLASS_PARAMETER,
    Collate = DUCKDB_V2_EXPRESSION_CLASS_COLLATE,
    Lambda = DUCKDB_V2_EXPRESSION_CLASS_LAMBDA,
    PositionalReference = DUCKDB_V2_EXPRESSION_CLASS_POSITIONAL_REFERENCE,
    Between = DUCKDB_V2_EXPRESSION_CLASS_BETWEEN,
    LambdaRef = DUCKDB_V2_EXPRESSION_CLASS_LAMBDA_REF,
    Type = DUCKDB_V2_EXPRESSION_CLASS_TYPE,
    BoundAggregate = DUCKDB_V2_EXPRESSION_CLASS_BOUND_AGGREGATE,
    BoundCase = DUCKDB_V2_EXPRESSION_CLASS_BOUND_CASE,
    LegacyBoundCast = DUCKDB_V2_EXPRESSION_CLASS_LEGACY_BOUND_CAST,
    BoundColumnRef = DUCKDB_V2_EXPRESSION_CLASS_BOUND_COLUMN_REF,
    LegacyBoundComparison = DUCKDB_V2_EXPRESSION_CLASS_LEGACY_BOUND_COMPARISON,
    BoundConjunction = DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONJUNCTION,
    BoundConstant = DUCKDB_V2_EXPRESSION_CLASS_BOUND_CONSTANT,
    BoundDefault = DUCKDB_V2_EXPRESSION_CLASS_BOUND_DEFAULT,
    BoundFunction = DUCKDB_V2_EXPRESSION_CLASS_BOUND_FUNCTION,
    BoundOperator = DUCKDB_V2_EXPRESSION_CLASS_BOUND_OPERATOR,
    BoundParameter = DUCKDB_V2_EXPRESSION_CLASS_BOUND_PARAMETER,
    BoundRef = DUCKDB_V2_EXPRESSION_CLASS_BOUND_REF,
    BoundSubquery = DUCKDB_V2_EXPRESSION_CLASS_BOUND_SUBQUERY,
    BoundWindow = DUCKDB_V2_EXPRESSION_CLASS_BOUND_WINDOW,
    LegacyBoundBetween = DUCKDB_V2_EXPRESSION_CLASS_LEGACY_BOUND_BETWEEN,
    BoundUnnest = DUCKDB_V2_EXPRESSION_CLASS_BOUND_UNNEST,
    BoundLambda = DUCKDB_V2_EXPRESSION_CLASS_BOUND_LAMBDA,
    BoundLambdaRef = DUCKDB_V2_EXPRESSION_CLASS_BOUND_LAMBDA_REF,
    BoundExpression = DUCKDB_V2_EXPRESSION_CLASS_BOUND_EXPRESSION,
    BoundExpanded = DUCKDB_V2_EXPRESSION_CLASS_BOUND_EXPANDED,
    }
}

ffi_enum_redeclaration! {
    /// The semantic operation represented by an expression node.
    ///
    /// Only a subset of this enum appears in callback trees exposed by this API.
    #[allow(missing_docs)]
    pub enum ExpressionType <- ffi::DUCKDB_V2_EXPRESSION_TYPE {
    Invalid = DUCKDB_V2_EXPRESSION_TYPE_INVALID,
    OperatorCast = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_CAST,
    OperatorNot = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_NOT,
    OperatorIsNull = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL,
    OperatorIsNotNull = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NOT_NULL,
    OperatorUnpack = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_UNPACK,
    CompareEqual = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL,
    CompareNotEqual = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOTEQUAL,
    CompareLessThan = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN,
    CompareGreaterThan = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHAN,
    CompareLessThanOrEqualTo = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
    CompareGreaterThanOrEqualTo = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
    CompareIn = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_IN,
    CompareNotIn = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_IN,
    CompareDistinctFrom = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_DISTINCT_FROM,
    CompareBetween = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_BETWEEN,
    CompareNotBetween = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_BETWEEN,
    CompareNotDistinctFrom = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_DISTINCT_FROM,
    ConjunctionAnd = DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND,
    ConjunctionOr = DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR,
    ValueConstant = DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT,
    ValueParameter = DUCKDB_V2_EXPRESSION_TYPE_VALUE_PARAMETER,
    ValueTuple = DUCKDB_V2_EXPRESSION_TYPE_VALUE_TUPLE,
    ValueTupleAddress = DUCKDB_V2_EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS,
    ValueNull = DUCKDB_V2_EXPRESSION_TYPE_VALUE_NULL,
    ValueVector = DUCKDB_V2_EXPRESSION_TYPE_VALUE_VECTOR,
    ValueScalar = DUCKDB_V2_EXPRESSION_TYPE_VALUE_SCALAR,
    ValueDefault = DUCKDB_V2_EXPRESSION_TYPE_VALUE_DEFAULT,
    Aggregate = DUCKDB_V2_EXPRESSION_TYPE_AGGREGATE,
    BoundAggregate = DUCKDB_V2_EXPRESSION_TYPE_BOUND_AGGREGATE,
    GroupingFunction = DUCKDB_V2_EXPRESSION_TYPE_GROUPING_FUNCTION,
    WindowAggregate = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_AGGREGATE,
    WindowFunction = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_FUNCTION,
    WindowRank = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_RANK,
    WindowRankDense = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_RANK_DENSE,
    WindowNtile = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_NTILE,
    WindowPercentRank = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_PERCENT_RANK,
    WindowCumeDist = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_CUME_DIST,
    WindowRowNumber = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_ROW_NUMBER,
    WindowFirstValue = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_FIRST_VALUE,
    WindowLastValue = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_LAST_VALUE,
    WindowLead = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_LEAD,
    WindowLag = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_LAG,
    WindowNthValue = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_NTH_VALUE,
    WindowFill = DUCKDB_V2_EXPRESSION_TYPE_WINDOW_FILL,
    Function = DUCKDB_V2_EXPRESSION_TYPE_FUNCTION,
    BoundFunction = DUCKDB_V2_EXPRESSION_TYPE_BOUND_FUNCTION,
    CaseExpr = DUCKDB_V2_EXPRESSION_TYPE_CASE_EXPR,
    OperatorNullif = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_NULLIF,
    OperatorCoalesce = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_COALESCE,
    ArrayExtract = DUCKDB_V2_EXPRESSION_TYPE_ARRAY_EXTRACT,
    ArraySlice = DUCKDB_V2_EXPRESSION_TYPE_ARRAY_SLICE,
    StructExtract = DUCKDB_V2_EXPRESSION_TYPE_STRUCT_EXTRACT,
    ArrayConstructor = DUCKDB_V2_EXPRESSION_TYPE_ARRAY_CONSTRUCTOR,
    Arrow = DUCKDB_V2_EXPRESSION_TYPE_ARROW,
    OperatorTry = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_TRY,
    Subquery = DUCKDB_V2_EXPRESSION_TYPE_SUBQUERY,
    Star = DUCKDB_V2_EXPRESSION_TYPE_STAR,
    TableStar = DUCKDB_V2_EXPRESSION_TYPE_TABLE_STAR,
    Placeholder = DUCKDB_V2_EXPRESSION_TYPE_PLACEHOLDER,
    ColumnRef = DUCKDB_V2_EXPRESSION_TYPE_COLUMN_REF,
    FunctionRef = DUCKDB_V2_EXPRESSION_TYPE_FUNCTION_REF,
    TableRef = DUCKDB_V2_EXPRESSION_TYPE_TABLE_REF,
    LambdaRef = DUCKDB_V2_EXPRESSION_TYPE_LAMBDA_REF,
    Type = DUCKDB_V2_EXPRESSION_TYPE_TYPE,
    Cast = DUCKDB_V2_EXPRESSION_TYPE_CAST,
    BoundRef = DUCKDB_V2_EXPRESSION_TYPE_BOUND_REF,
    BoundColumnRef = DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF,
    BoundUnnest = DUCKDB_V2_EXPRESSION_TYPE_BOUND_UNNEST,
    Collate = DUCKDB_V2_EXPRESSION_TYPE_COLLATE,
    Lambda = DUCKDB_V2_EXPRESSION_TYPE_LAMBDA,
    PositionalReference = DUCKDB_V2_EXPRESSION_TYPE_POSITIONAL_REFERENCE,
    BoundLambdaRef = DUCKDB_V2_EXPRESSION_TYPE_BOUND_LAMBDA_REF,
    BoundExpanded = DUCKDB_V2_EXPRESSION_TYPE_BOUND_EXPANDED,
    }
}

/// A borrowed node in DuckDB's bound expression tree.
///
/// The lifetime ties the node and its children to the callback data that
/// exposes them. Use [`Expression::class`] before calling class-specific
/// accessors; traversal and general type information work for every node.
pub struct Expression<'a> {
    /// The borrowed DuckDB expression handle.
    pub handle: ffi::duckdb_v2_expression_handle,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Expression<'a> {
    /// Return the child at `index` in DuckDB's traversal order.
    pub fn child(&self, index: usize) -> Result<Expression<'a>> {
        let handle = check_api_call!(ffi::duckdb_v2_expression_get_child, self.handle, index as u64, RET)?;

        Ok(Expression {
            handle,
            _marker: std::marker::PhantomData,
        })
    }

    /// Return the number of child expressions.
    pub fn child_count(&self) -> Result<usize> {
        let count = check_api_call!(ffi::duckdb_v2_expression_get_child_count, self.handle, RET)?;

        Ok(count as usize)
    }

    /// Return all child expressions in DuckDB's traversal order.
    pub fn children(&self) -> Result<Vec<Expression<'a>>> {
        let count = self.child_count()?;

        let mut children = Vec::with_capacity(count);
        for i in 0..count {
            let child = self.child(i)?;
            children.push(child);
        }

        Ok(children)
    }

    /// Return the bound expression class.
    pub fn class(&self) -> Result<ExpressionClass> {
        check_api_call!(ffi::duckdb_v2_expression_get_class, self.handle, RET)?.try_into()
    }

    /// Return the logical table and column indexes of a bound column reference.
    pub fn column_binding(&self) -> Result<(u64, u64)> {
        let mut table_index: u64 = 0;
        let mut column_index: u64 = 0;

        check_api_call!(
            ffi::duckdb_v2_expression_get_column_binding,
            self.handle,
            &mut table_index,
            &mut column_index,
        )?;

        Ok((table_index, column_index))
    }

    /// Return the owned value of a bound constant expression.
    pub fn get_constant_value(&self) -> Result<Value> {
        Ok(Value {
            handle: check_api_call!(ffi::duckdb_v2_expression_get_constant_value, self.handle, RET)?,
        })
    }

    /// Return the registered name of a bound function expression.
    ///
    /// Comparison operators may return an internal name such as
    /// `__comparison`; use [`Expression::expression_type`] to distinguish their
    /// semantic operation.
    pub fn function_name(&self) -> Result<String> {
        let name: ffi::duckdb_v2_str = check_api_call!(ffi::duckdb_v2_expression_get_function_name, self.handle, RET)?;

        let name: &str = name.into();
        Ok(name.to_string())
    }

    /// Return the input chunk column index of a physical bound reference.
    pub fn reference_index(&self) -> Result<u64> {
        let index = check_api_call!(ffi::duckdb_v2_expression_get_reference_index, self.handle, RET)?;

        Ok(index)
    }

    /// Return an owned copy of the expression's result type.
    pub fn return_type(&self) -> Result<LogicalType> {
        let handle = check_api_call!(ffi::duckdb_v2_expression_get_return_type, self.handle, RET)?;

        Ok(LogicalType { handle })
    }

    /// Return the expression's semantic operation.
    pub fn expression_type(&self) -> Result<ExpressionType> {
        check_api_call!(ffi::duckdb_v2_expression_get_type, self.handle, RET)?.try_into()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {

    use crate::{
        Context, DuckDBType, ToValue,
        expression::{ExpressionClass, ExpressionType},
        signature::SignatureBuilder,
        table_function::{TableFunctionBuilder, TableFunctionCallbacks},
    };

    struct TableFunctionTest;

    impl TableFunctionCallbacks for TableFunctionTest {
        type BindData = ();
        type GlobalState = ();
        type LocalState = ();

        fn bind(
            &self,
            context: Context,
            _metadata: crate::bind_arguments::BindArguments,
            bind_handle: crate::table_function::BindFunctionHandle,
        ) -> crate::Result<(Self::BindData, Option<crate::table_function::TableFunctionCardinality>)> {
            for i in 0..3 {
                bind_handle.add_result_column(format!("val{}", i).as_str(), i32::logical_type(&context)?)?;
            }

            Ok(((), None))
        }

        fn exec(
            &self,
            _bind_data: Option<&Self::BindData>,
            _global_state: Option<&Self::GlobalState>,
            _local_state: Option<&mut Self::LocalState>,
            _context: Context,
            output: crate::data_chunk::DataChunk,
        ) -> crate::Result<()> {
            let mut vec = output.get_vector_at::<i32>(0)?;

            vec.set_size(0)?;

            Ok(())
        }

        fn pushdown_complex_filter(
            &self,
            _bind_data: Option<&Self::BindData>,
            context: Context,
            column_data: crate::table_function::FilterColumnData,
        ) -> crate::Result<()> {
            assert_eq!(column_data.get_column_count()?, 3);

            let expression_api = column_data.get_expression(0)?;

            dbg!(expression_api.child_count()?);

            expression_api.function_name()?;

            assert_eq!(expression_api.function_name()?, "!=");
            assert_eq!(expression_api.expression_type()?, ExpressionType::CompareNotEqual,);
            assert_eq!(expression_api.child_count()?, 2);
            assert_eq!(
                expression_api.child(0)?.expression_type()?,
                ExpressionType::BoundColumnRef
            );
            assert_eq!(
                expression_api.child(1)?.expression_type()?,
                ExpressionType::ValueConstant
            );

            assert_eq!(expression_api.class()?, ExpressionClass::BoundFunction);

            assert_eq!(expression_api.return_type()?, bool::logical_type(&context)?);

            // TODO: Assert a different return for column_binding.
            assert_eq!(expression_api.child(0)?.column_binding()?, (0, 0));
            let expected_value = 10_i32.value(&context)?;

            assert_eq!(
                expression_api.child(1)?.get_constant_value()?.dbg_string()?,
                expected_value.dbg_string()?,
            );

            panic!("I dont want to return, I want to panic!!");
        }
    }

    #[test]
    pub fn test_expression_with_panic() -> crate::Result<()> {
        let env = crate::Environment::new()?;
        let db = env.open(crate::StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let signature = SignatureBuilder::without_return_type(vec![]);

        TableFunctionBuilder::new("test_function", signature, TableFunctionTest {}).register_with_connection(&conn)?;

        use crate::Parameters;

        let result = conn.query(
            "SELECT val0, val1, val2 from test_function() where val1 != $1",
            Parameters::named(&[("", &10)]),
        );

        let err_message = result.as_ref().err().map(|e| e.message.clone());

        assert!(
            err_message.clone().is_some_and(
                |e| e.starts_with("INTERNAL Error: Panic occurred: I dont want to return, I want to panic!!")
            ),
            "Expected panic error message, got {:?}",
            err_message
        );

        Ok(())
    }
}
