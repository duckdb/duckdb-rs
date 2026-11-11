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
    vector::{ChildVector, Unknown, Vector, VectorElement, WritableVectorElement},
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
        // SAFETY: every field of every row is written below.
        unsafe {
            output.set_size_uninit(rows)?;
            output.writer()?.fill_validity(true)?;
        }
        let mut fields = output
            .children_mut()
            .ok_or_else(|| Error::invalid_input("merge_struct output has no fields"))?;

        copy_column(&ints, &mut fields.get_vector_at_mut::<i32>(0)?)?;
        copy_column(&strings, &mut fields.get_vector_at_mut::<String>(1)?)?;
        copy_maps(&maps, &mut fields.get_vector_at_mut::<Map<&str, i32>>(2)?, rows)?;
        copy_unions(
            &unions,
            &mut fields.get_vector_at_mut::<UnionInputRef<'_, UncheckedString>>(3)?,
            rows,
        )?;
        copy_column(&amounts, &mut fields.get_vector_at_mut::<Amount>(4)?)?;
        Ok(())
    }
}

#[inline(never)]
fn copy_column<'s, S, T>(source: &'s Vector<'_, S>, target: &mut ChildVector<'_, T>) -> Result<()>
where
    S: VectorElement<Ref<'s> = &'s T::Write> + 's,
    T: WritableVectorElement,
    T::Write: 's,
{
    let mut writer = target.writer()?;
    for (row, value) in source.reader()?.iter().enumerate() {
        writer.set(row, value)?;
    }
    Ok(())
}

#[inline(never)]
fn copy_maps(
    maps: &Vector<'_, Map<UncheckedString, i32>>,
    field: &mut ChildVector<'_, Map<&str, i32>>,
    rows: usize,
) -> Result<()> {
    let parent = maps.reader()?;
    let input_entries = maps.children().unwrap().get_untyped_vector_at(0)?;
    let input_fields = input_entries.children().unwrap();
    let keys = input_fields.get_vector_at::<UncheckedString>(0)?.reader()?;
    let values = input_fields.get_vector_at::<i32>(1)?.reader()?;
    let entries = (0..parent.len())
        .map(|row| Ok(parent.get_list_entry(row)?.map_or(0, |entry| entry.length() as usize)))
        .sum::<Result<usize>>()?;
    super::ensure(parent.len() == rows, || {
        "MAP row count differs from the input chunk".to_owned()
    })?;
    let mut writer = field.map_writer()?;
    writer.reserve(entries)?;
    for row in 0..parent.len() {
        let Some(entry) = parent.get_list_entry(row)? else {
            writer.set_null(row)?;
            continue;
        };
        for index in entry.offset() as usize..(entry.offset() + entry.length()) as usize {
            let key = keys
                .get(index)?
                .ok_or_else(|| Error::invalid_input("MAP key is NULL"))?;
            writer.push(key, values.get(index)?)?;
        }
        writer.finish_row(row)?;
    }
    writer.finish()
}

#[inline(never)]
fn copy_unions(
    unions: &Vector<'_, UnionInput<UncheckedString>>,
    field: &mut ChildVector<'_, UnionInputRef<'_, UncheckedString>>,
    rows: usize,
) -> Result<()> {
    let parent = unions.reader()?;
    let children = unions.children().unwrap();
    let tags = children.get_vector_at::<u8>(0)?.reader()?;
    let numbers = children.get_vector_at::<i32>(1)?.reader()?;
    let strings = children.get_vector_at::<UncheckedString>(2)?.reader()?;
    super::ensure(parent.len() == rows, || {
        "UNION row count differs from the input chunk".to_owned()
    })?;

    let mut writer = field.union_writer::<(i32, String)>()?;
    for row in 0..parent.len() {
        if parent.is_null(row)? {
            writer.set_null(row)?;
            continue;
        }
        match tags.get(row)? {
            Some(&0) => writer.set::<0>(row, numbers.get(row)?)?,
            Some(&1) => writer.set::<1>(row, strings.get(row)?)?,
            _ => return Err(Error::invalid_input("invalid UNION tag")),
        }
    }
    Ok(())
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
