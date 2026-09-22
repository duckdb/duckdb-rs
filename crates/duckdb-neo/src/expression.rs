//! Read-only introspection of bound expression trees.

use crate::ffi;

use crate::{
    Result, builder_helpers::ffi_enum_redeclaration, cast::CastMode, check_api_call, logical_type::LogicalType,
    qualified_name::QualifiedName, value::Value,
};

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
    CompareNotDistinctFrom = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_DISTINCT_FROM,
    ConjunctionAnd = DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND,
    ConjunctionOr = DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR,
    ValueConstant = DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT,
    ValueParameter = DUCKDB_V2_EXPRESSION_TYPE_VALUE_PARAMETER,
    BoundFunction = DUCKDB_V2_EXPRESSION_TYPE_BOUND_FUNCTION,
    CaseExpr = DUCKDB_V2_EXPRESSION_TYPE_CASE_EXPR,
    OperatorCoalesce = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_COALESCE,
    BoundColumnRef = DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF,
    MaxEnum = DUCKDB_V2_EXPRESSION_TYPE_MAX_ENUM,
    }
}

/// A borrowed node in DuckDB's bound expression tree.
///
/// The lifetime ties the node and its children to the callback data that
/// exposes them.
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

    /// Return whether this node is a regular `CAST` or a `TRY_CAST`.
    ///
    /// Returns an error if the node is not a cast.
    pub fn cast_mode(&self) -> Result<CastMode> {
        check_api_call!(ffi::duckdb_v2_expression_cast_get_mode, self.handle, RET)?.try_into()
    }

    /// Return the owned value of a bound constant expression.
    ///
    /// Returns an error if the node is not a constant.
    pub fn get_constant_value(&self) -> Result<Value> {
        Ok(Value {
            handle: check_api_call!(ffi::duckdb_v2_expression_constant_get_value, self.handle, RET)?,
        })
    }

    /// Return the registered name of a bound function expression.
    ///
    /// Comparisons return their operator, such as `<`. Casts and `BETWEEN`
    /// return internal names; use [`Self::expression_type`] to distinguish them.
    /// Returns an error for nodes that do not represent function calls.
    pub fn function_name(&self) -> Result<String> {
        let name: ffi::duckdb_v2_str = check_api_call!(ffi::duckdb_v2_expression_function_get_name, self.handle, RET)?;

        let name: &str = name.into();
        Ok(name.to_string())
    }

    /// Return the function's owned name, qualified by catalog and schema where known.
    ///
    /// Accepts the same node types as [`Self::function_name`] and returns an
    /// error for other nodes.
    pub fn qname(&self) -> Result<QualifiedName> {
        Ok(QualifiedName {
            handle: check_api_call!(ffi::duckdb_v2_expression_function_get_qname, self.handle, RET)?,
        })
    }

    /// Return the column reference's index in the operator's current column list.
    ///
    /// Returns an error if the node is not a column reference.
    pub fn reference_index(&self) -> Result<usize> {
        let index = check_api_call!(ffi::duckdb_v2_expression_column_ref_get_index, self.handle, RET)?;

        Ok(index as usize)
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
        DuckDBType, ToValue,
        bind_arguments::BindArgument,
        connection::Context,
        data_chunk::DataChunkRef,
        environment::{Environment, StorageLocation},
        expression::ExpressionType,
        signature::SignatureBuilder,
        table_function::{ExecColumnInfo, TableFunctionBuilder, TableFunctionCallbacks},
    };

    struct TableFunctionTest;

    impl TableFunctionCallbacks for TableFunctionTest {
        type BindData = ();
        type GlobalState = ();
        type LocalState = ();

        fn bind(
            &self,
            context: Context,
            _metadata: Vec<BindArgument>,
            bind_handle: crate::table_function::BindFunctionHandle<'_>,
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
            output: DataChunkRef<'_>,
            _column_info: ExecColumnInfo<'_>,
        ) -> crate::Result<()> {
            let mut vec = output.get_vector_at::<i32>(0)?;

            vec.set_size(0)?;

            Ok(())
        }
    }

    impl crate::table_function::TableFilterPushdownCallbacks for TableFunctionTest {
        fn pushdown_filter(
            &self,
            _bind_data: Option<&Self::BindData>,
            context: Context,
            column_data: crate::table_function::PushdownData<'_>,
        ) -> crate::Result<()> {
            assert_eq!(column_data.get_column_count()?, 3);

            let expression_api = column_data.filter(0)?;

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

            assert_eq!(expression_api.return_type()?, bool::logical_type(&context)?);

            // TODO: Assert a different return for column_binding.
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
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let signature = SignatureBuilder::without_return_type(vec![]);

        TableFunctionBuilder::new("test_function", signature, TableFunctionTest {})
            .with_filter_pushdown()
            .register(&conn)?;

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
