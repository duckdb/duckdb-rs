//! `merge_struct(i, s, m, u, d)`: packs an `INTEGER`, `VARCHAR`, `MAP`, `UNION` and `DECIMAL`
//! into one `STRUCT`, keeping each argument's NULLs in its field.

use duckdb_neo::{
    AsDuckDBUnion, Error, Parameters, Result,
    connection::{Connection, Context},
    data_chunk::VectorCollection,
    enums::FunctionProperty,
    logical_type::LogicalType,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::{Decimal, Map, Struct, UncheckedString},
    vector::{Unknown, Vector},
};

use super::{nulls::Nulls, readable};

pub const MAP_TYPE: &str = "MAP(VARCHAR, INTEGER)";
pub const UNION_TYPE: &str = "UNION(num INTEGER, str VARCHAR)";
pub type Amount = Decimal<18, 3>;

#[derive(AsDuckDBUnion)]
enum UnionInput<S> {
    #[duckdb(rename = "num")]
    Num(Option<i32>),
    #[duckdb(rename = "str")]
    Str(Option<S>),
}

pub struct MergeStruct;

impl ScalarCallbacks for MergeStruct {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&()>,
        _init_data: Option<&mut ()>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let rows = input.row_count()?;
        let [ints, strings, maps, unions, amounts] = input.get_vector_array_mut([0, 1, 2, 3, 4])?;
        let ints = readable::<i32>(ints)?;
        let strings = readable::<UncheckedString>(strings)?;
        let maps = readable::<Map<UncheckedString, i32>>(maps)?;
        let unions = readable::<UnionInput<UncheckedString>>(unions)?;
        let amounts = readable::<Amount>(amounts)?;

        let mut output = output.cast::<Struct>()?;
        output.set_size(rows)?;
        // SAFETY: every field of every row is written below.
        unsafe { output.writer()?.fill_validity(true)? };
        let mut fields = output
            .children_mut()
            .ok_or_else(|| Error::invalid_input("merge_struct output has no fields"))?;

        {
            let mut field = fields.get_vector_at_mut::<i32>(0)?;
            let mut writer = field.writer()?;
            for (row, value) in ints.reader()?.iter().enumerate() {
                writer.set(row, value)?;
            }
        }
        {
            let mut field = fields.get_vector_at_mut::<String>(1)?;
            let mut writer = field.writer()?;
            for (row, value) in strings.reader()?.iter().enumerate() {
                writer.set(row, value)?;
            }
        }
        {
            let maps = maps.reader()?;
            let mut field = fields.get_vector_at_mut::<Map<&str, i32>>(2)?;
            let entries = (0..maps.len())
                .map(|row| Ok(maps.get_list_entry(row)?.map_or(0, |entry| entry.length() as usize)))
                .sum::<Result<usize>>()?;
            // SAFETY: the entries the rows below leave unwritten are trimmed afterwards.
            unsafe { field.set_child_size(entries)? };
            {
                let mut writer = field.writer()?;
                let mut scratch = Map(Vec::new());
                for (row, map) in maps.iter().enumerate() {
                    scratch.0.clear();
                    if let Some(map) = map {
                        scratch.0.extend(map.iter().map(|(key, value)| (key, value.copied())));
                        writer.set(row, Some(&scratch))?;
                    } else {
                        writer.set(row, None)?;
                    }
                }
            }
            let written = field.child_offset();
            // SAFETY: trims the child to the entries written above.
            unsafe { field.set_child_size(written)? };
        }
        {
            let mut field = fields.get_vector_at_mut::<UnionInputRef<'_, UncheckedString>>(3)?;
            let mut writer = field.writer()?;
            for (row, value) in unions.reader()?.iter().enumerate() {
                writer.set(row, value.as_ref())?;
            }
        }
        {
            let mut field = fields.get_vector_at_mut::<Amount>(4)?;
            let mut writer = field.writer()?;
            for (row, value) in amounts.reader()?.iter().enumerate() {
                writer.set(row, value)?;
            }
        }
        Ok(())
    }
}

pub fn register(conn: &Connection) -> Result<()> {
    let output = format!("STRUCT(i INTEGER, s VARCHAR, m {MAP_TYPE}, u {UNION_TYPE}, d DECIMAL(18, 3))");
    let signature = SignatureBuilder::new()
        .param(FunctionParameter::new::<i32>("i"))
        .param(FunctionParameter::new::<String>("s"))
        .param(FunctionParameter::with_type(
            "m",
            LogicalType::from_text(conn, MAP_TYPE)?,
        ))
        .param(FunctionParameter::with_type(
            "u",
            LogicalType::from_text(conn, UNION_TYPE)?,
        ))
        .param(FunctionParameter::new::<Amount>("d"))
        .return_type(LogicalType::from_text(conn, &output)?);
    ScalarFunctionBuilder::new("merge_struct", signature, MergeStruct)
        .set_property(FunctionProperty::HasSpecialNullHandling(true))
        .register(conn)
}

/// Create `scalar_input` using the configured NULL percentage, including nested values.
pub fn create_input(conn: &Connection, rows: usize) -> Result<()> {
    create_input_with_nulls(conn, rows, Nulls::from_env()?)
}

pub fn create_input_with_nulls(conn: &Connection, rows: usize, nulls: Nulls) -> Result<()> {
    let map = format!(
        "MAP {{'a': {}, 'b': {}, 'key_' || (i % 7): {}}}",
        nulls.expression("i::INTEGER", 5),
        nulls.expression("(i * 2)::INTEGER", 6),
        nulls.expression("(i % 7)::INTEGER", 7),
    );
    let union = format!(
        "CASE WHEN i % 2 = 0 THEN union_value(num := {})::{UNION_TYPE}
              ELSE union_value(str := {})::{UNION_TYPE} END",
        nulls.expression("i::INTEGER", 8),
        nulls.expression("'union_string_' || i", 9),
    );
    conn.execute(
        format!(
            "CREATE OR REPLACE TABLE scalar_input AS
             SELECT {} AS i, {} AS s, {} AS m, {} AS u, {} AS d
             FROM range({rows}) t(i)",
            nulls.expression("i::INTEGER", 0),
            nulls.expression("'value_' || i", 1),
            nulls.expression(&map, 2),
            nulls.expression(&union, 3),
            nulls.expression("(i / 1000)::DECIMAL(18, 3)", 4),
        )
        .as_str(),
        Parameters::None,
    )?;
    Ok(())
}

pub const QUERY: &str = "SELECT merge_struct(i, s, m, u, d) FROM scalar_input";

/// Check `merge_struct` against its arguments over `scalar_input`, comparing MAP entries unordered.
pub fn verify(conn: &Connection) -> Result<()> {
    let mismatches = super::query_i64(
        conn,
        "SELECT count(*) FROM (SELECT merge_struct(i, s, m, u, d) AS r, * FROM scalar_input)
         WHERE r IS NULL OR r.i IS DISTINCT FROM i OR r.s IS DISTINCT FROM s OR r.u IS DISTINCT FROM u
            OR union_tag(r.u) IS DISTINCT FROM union_tag(u)
            OR r.d IS DISTINCT FROM d
            OR list_sort(map_entries(r.m)) IS DISTINCT FROM list_sort(map_entries(m))",
    )?;
    super::ensure(mismatches == 0, || format!("merge_struct differs on {mismatches} rows"))
}
