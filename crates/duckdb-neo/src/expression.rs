//! Read-only introspection of bound expression trees.

use crate::ffi;

use crate::ffi_str::DuckDBStr;
use crate::{
    Result, builder_helpers::ffi_enum_redeclaration, cast::CastMode, check_api_call, logical_type::LogicalType,
    qualified_name::QualifiedName, value::Value,
};

ffi_enum_redeclaration! {
    /// The filter-predicate node types modelled by the API.
    /// Other node types report [`Self::Invalid`], but their children can still be walked.
    pub enum ExpressionType <- ffi::DUCKDB_V2_EXPRESSION_TYPE {
    /// An unmodelled node; its children can still be walked.
    Invalid = DUCKDB_V2_EXPRESSION_TYPE_INVALID,
    /// A cast. One child: the value being cast.
    OperatorCast = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_CAST,
    /// Logical `NOT`. One child.
    OperatorNot = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_NOT,
    /// `IS NULL`. One child.
    OperatorIsNull = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL,
    /// `IS NOT NULL`. One child.
    OperatorIsNotNull = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NOT_NULL,
    /// `=`. Two children.
    CompareEqual = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL,
    /// `<>`. Two children.
    CompareNotEqual = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOTEQUAL,
    /// `<`. Two children.
    CompareLessThan = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN,
    /// `>`. Two children.
    CompareGreaterThan = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHAN,
    /// `<=`. Two children.
    CompareLessThanOrEqualTo = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO,
    /// `>=`. Two children.
    CompareGreaterThanOrEqualTo = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO,
    /// `IN`. The tested value followed by the candidates.
    CompareIn = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_IN,
    /// `NOT IN`. The tested value followed by the candidates.
    CompareNotIn = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_IN,
    /// `IS DISTINCT FROM`. Two children.
    CompareDistinctFrom = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_DISTINCT_FROM,
    /// `BETWEEN`. Three children: the tested value, lower bound, and upper bound.
    CompareBetween = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_BETWEEN,
    /// `IS NOT DISTINCT FROM`. Two children.
    CompareNotDistinctFrom = DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_DISTINCT_FROM,
    /// Logical `AND`. Two or more children.
    ConjunctionAnd = DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND,
    /// Logical `OR`. Two or more children.
    ConjunctionOr = DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR,
    /// A constant. No children; read it with [`Expression::get_constant_value`].
    ValueConstant = DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT,
    /// An unresolved prepared statement parameter. No children.
    ValueParameter = DUCKDB_V2_EXPRESSION_TYPE_VALUE_PARAMETER,
    /// A scalar function call. Its children are its arguments.
    BoundFunction = DUCKDB_V2_EXPRESSION_TYPE_BOUND_FUNCTION,
    /// `CASE`. Each `WHEN` condition followed by its `THEN` result, then the `ELSE` result.
    CaseExpr = DUCKDB_V2_EXPRESSION_TYPE_CASE_EXPR,
    /// `COALESCE`. One or more children.
    OperatorCoalesce = DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_COALESCE,
    /// A column reference. No children; read it with [`Expression::reference_index`].
    BoundColumnRef = DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF,
    }
}

/// A borrowed node in DuckDB's bound expression tree.
///
/// The lifetime ties the node and its children to the callback data that
/// exposes them.
#[derive(Debug)]
pub struct Expression<'a> {
    /// The borrowed DuckDB expression handle.
    pub(crate) handle: ffi::duckdb_v2_expression_handle,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Expression<'a> {
    /// Return the child at `index` in the order described by [`ExpressionType`].
    /// Returns an error if `index` is out of bounds.
    pub fn child(&self, index: usize) -> Result<Expression<'a>> {
        check_api_call!(ffi::duckdb_v2_expression_get_child, self.handle, index as u64, RET).map(|handle| Expression {
            handle,
            _marker: std::marker::PhantomData,
        })
    }

    /// Return the number of child expressions.
    pub fn child_count(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_expression_get_child_count, self.handle, RET).map(|count| count as usize)
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
        check_api_call!(ffi::duckdb_v2_expression_constant_get_value, self.handle, RET).map(|handle| Value { handle })
    }

    /// Return the registered name of a bound function expression.
    ///
    /// Comparisons return their operator, such as `<`. Casts and `BETWEEN`
    /// return internal names; use [`Self::expression_type`] to distinguish them.
    /// Returns an error for nodes that do not represent function calls.
    pub fn function_name(&self) -> Result<String> {
        check_api_call!(ffi::duckdb_v2_expression_function_get_name, self.handle, RET)
            .map(|name| DuckDBStr::from_raw(name).as_str().unwrap_or_default().to_owned())
    }

    /// Return the function's owned name, qualified by catalog and schema where known.
    ///
    /// Accepts the same node types as [`Self::function_name`] and returns an
    /// error for other nodes.
    pub fn qname(&self) -> Result<QualifiedName> {
        check_api_call!(ffi::duckdb_v2_expression_function_get_qname, self.handle, RET)
            .map(|handle| QualifiedName { handle })
    }

    /// Return the column reference's operator-local index, or an error for other nodes.
    /// Resolve it with [`PushdownData::get_column_index`](crate::table_function::PushdownData::get_column_index).
    pub fn reference_index(&self) -> Result<usize> {
        check_api_call!(ffi::duckdb_v2_expression_column_ref_get_index, self.handle, RET).map(|index| index as usize)
    }

    /// Return an owned copy of the result type; for a cast, this is the target type.
    pub fn return_type(&self) -> Result<LogicalType> {
        check_api_call!(ffi::duckdb_v2_expression_get_return_type, self.handle, RET)
            .map(|handle| LogicalType { handle })
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

    #[test]
    fn expression_type_rejects_internal_sentinel() {
        assert!(
            ExpressionType::try_from(crate::ffi::DUCKDB_V2_EXPRESSION_TYPE::DUCKDB_V2_EXPRESSION_TYPE_MAX_ENUM)
                .is_err()
        );
    }

    struct TableFunctionTest;

    impl TableFunctionCallbacks for TableFunctionTest {
        type BindData = ();
        type GlobalState = ();
        type LocalState = ();

        fn bind(
            &self,
            context: &Context,
            _metadata: Vec<BindArgument>,
            bind_handle: crate::table_function::BindFunctionHandle<'_>,
        ) -> crate::Result<Self::BindData> {
            for i in 0..3 {
                bind_handle.add_result_column(format!("val{}", i).as_str(), &i32::logical_type(context)?)?;
            }

            Ok(())
        }

        fn exec(
            &self,
            _bind_data: &Self::BindData,
            _global_state: Option<&Self::GlobalState>,
            _local_state: Option<&mut Self::LocalState>,
            _context: &Context,
            mut output: DataChunkRef<'_>,
            _column_info: ExecColumnInfo<'_>,
        ) -> crate::Result<()> {
            let mut vec = output.get_vector_at_mut::<i32>(0)?;

            vec.set_size(0)?;

            Ok(())
        }
    }

    impl crate::table_function::TableFilterPushdownCallbacks for TableFunctionTest {
        fn pushdown_filter(
            &self,
            _bind_data: &Self::BindData,
            context: &Context,
            pushdown_data: crate::table_function::PushdownData<'_>,
        ) -> crate::Result<()> {
            assert_eq!(pushdown_data.count()?, 3);

            let expression_api = pushdown_data.filter(0)?;

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

            assert_eq!(expression_api.return_type()?, bool::logical_type(context)?);

            // TODO: Assert a different return for column_binding.
            let expected_value = 10_i32.value(context)?;

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
        let instance = env.open(StorageLocation::InMemory)?;
        let conn = instance.connect()?;

        let signature = SignatureBuilder::new();

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
