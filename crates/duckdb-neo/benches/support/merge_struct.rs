//! `merge_struct(i, s, m, u, d)`: packs an `INTEGER`, `VARCHAR`, `MAP`, `UNION` and `DECIMAL`
//! into one `STRUCT`, keeping each argument's NULLs in its field.

use std::collections::HashMap;

use duckdb_neo::{
    Error, Parameters, Result,
    connection::{Connection, Context},
    data_chunk::VectorCollection,
    enums::FunctionProperty,
    logical_type::LogicalType,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    types::{Decimal, Map, Struct, UncheckedString, Union, UnionWriter},
    vector::{Unknown, Vector},
};

use super::readable;

pub const MAP_TYPE: &str = "MAP(VARCHAR, INTEGER)";
pub const UNION_TYPE: &str = "UNION(num INTEGER, str VARCHAR)";
pub type Amount = Decimal<18, 3>;

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
        let unions = readable::<Union>(unions)?;
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
                writer.set_ref(row, value)?;
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
            let mut field = fields.get_vector_at_mut::<Map<String, i32>>(2)?;
            let entries = maps.iter().flatten().map(|map| map.len()).sum();
            // SAFETY: the entries the rows below leave unwritten are trimmed afterwards.
            unsafe { field.set_child_size(entries)? };
            {
                let mut writer = field.writer()?;
                for (row, map) in maps.iter().enumerate() {
                    let map = map
                        .map(|map| {
                            map.iter()
                                .map(|(key, value)| Ok((key.ok_or_else(null_key)?, value.copied())))
                                .collect::<Result<HashMap<_, _>>>()
                        })
                        .transpose()?;
                    writer.set(row, map)?;
                }
            }
            let written = field.child_offset();
            // SAFETY: trims the child to the entries written above.
            unsafe { field.set_child_size(written)? };
        }
        {
            let mut field = fields.get_vector_at_mut::<Union>(3)?;
            let mut writer = field.writer()?;
            for (row, value) in unions.reader()?.iter().enumerate() {
                let value = match value {
                    None => None,
                    Some(value) if value.member() == 0 => {
                        Some(UnionWriter::set_value::<i32>(0, value.get::<i32>(0)?.copied()))
                    }
                    Some(value) => Some(UnionWriter::set_value::<String>(1, value.get::<UncheckedString>(1)?)),
                };
                writer.set(row, value)?;
            }
        }
        {
            let mut field = fields.get_vector_at_mut::<Amount>(4)?;
            let mut writer = field.writer()?;
            for (row, value) in amounts.reader()?.iter().enumerate() {
                writer.set_ref(row, value)?;
            }
        }
        Ok(())
    }
}

fn null_key() -> Error {
    Error::invalid_input("MAP key is NULL")
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

/// Create `scalar_input` with `rows` rows of `merge_struct` arguments, about 1% of them NULL.
pub fn create_input(conn: &Connection, rows: usize) -> Result<()> {
    conn.execute(
        format!(
            "CREATE OR REPLACE TABLE scalar_input AS
             SELECT i::INTEGER AS i,
                    CASE WHEN i % 97 = 0 THEN NULL ELSE 'value_' || i END AS s,
                    MAP {{'a': i::INTEGER, 'b': (i * 2)::INTEGER, 'key_' || (i % 7): (i % 7)::INTEGER}} AS m,
                    CASE WHEN i % 2 = 0 THEN union_value(num := i::INTEGER)::{UNION_TYPE}
                         ELSE union_value(str := 'union_string_' || i)::{UNION_TYPE} END AS u,
                    CASE WHEN i % 89 = 0 THEN NULL ELSE (i / 1000)::DECIMAL(18, 3) END AS d
             FROM range({rows}) t(i)"
        )
        .as_str(),
        Parameters::None,
    )?;
    Ok(())
}

pub const QUERY: &str = "SELECT merge_struct(i, s, m, u, d) FROM scalar_input";

/// Check `merge_struct` against its arguments over `scalar_input`. MAP entries compare unordered,
/// since the typed MAP writer takes a `HashMap`.
pub fn verify(conn: &Connection) -> Result<()> {
    let mismatches = super::query_i64(
        conn,
        "SELECT count(*) FROM (SELECT merge_struct(i, s, m, u, d) AS r, * FROM scalar_input)
         WHERE r.i IS DISTINCT FROM i OR r.s IS DISTINCT FROM s OR r.u IS DISTINCT FROM u
            OR r.d IS DISTINCT FROM d
            OR list_sort(map_entries(r.m)) IS DISTINCT FROM list_sort(map_entries(m))",
    )?;
    super::ensure(mismatches == 0, || format!("merge_struct differs on {mismatches} rows"))
}
