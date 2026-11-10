//! `merge_struct(i, s, m, u, d)`: packs an `INTEGER`, `VARCHAR`, `MAP`, `UNION` and `DECIMAL`
//! into one `STRUCT`, keeping each argument's NULLs in its field.

use duckdb_neo::{
    AsDuckDBUnion, Error, Parameters, Result,
    connection::{Connection, Context},
    data_chunk::VectorCollection,
    enums::FunctionProperty,
    ffi,
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
            let parent = maps.reader()?;
            let input_entries = maps.children().unwrap().get_untyped_vector_at(0)?;
            let input_fields = input_entries.children().unwrap();
            let keys = input_fields.get_vector_at::<UncheckedString>(0)?.reader()?;
            let values = input_fields.get_vector_at::<i32>(1)?.reader()?;
            let mut field = fields.get_vector_at_mut::<Map<&str, i32>>(2)?;
            let entries = (0..parent.len())
                .map(|row| Ok(parent.get_list_entry(row)?.map_or(0, |entry| entry.length() as usize)))
                .sum::<Result<usize>>()?;
            super::ensure(parent.len() == rows, || {
                "MAP row count differs from the input chunk".to_owned()
            })?;
            // SAFETY: every reserved entry is written through the key/value writers below.
            unsafe { field.set_child_size(entries)? };
            let (entry_data, parent_validity) = {
                let mut writer = field.writer()?;
                let data = writer.as_mut_ptr().cast::<ffi::duckdb_v2_list_entry>();
                // SAFETY: every row descriptor is written below; NULL rows are invalidated.
                unsafe {
                    writer.fill_validity(true)?;
                    (data, writer.validity_mut()?.as_mut_ptr())
                }
            };
            let mut written = 0;
            {
                let mut children = field.children_mut().unwrap();
                let mut output_entries = children.get_vector_at_mut::<Struct>(0)?;
                // SAFETY: MAP entries are non-NULL structs, with both fields initialized below.
                unsafe { output_entries.writer()?.fill_validity(true)? };
                let mut output_fields = output_entries.children_mut().unwrap();
                let [key_vector, value_vector] = output_fields.get_vector_array_mut([0, 1])?;
                let mut key_vector = key_vector.cast::<String>()?;
                let mut value_vector = value_vector.cast::<i32>()?;
                let mut key_field = key_vector.writer()?;
                let mut value_field = value_vector.writer()?;
                for row in 0..parent.len() {
                    let offset = written;
                    let entry = parent.get_list_entry(row)?;
                    if let Some(entry) = entry {
                        for index in entry.offset()..entry.offset() + entry.length() {
                            let key = keys
                                .get(index as usize)?
                                .ok_or_else(|| Error::invalid_input("MAP key is NULL"))?;
                            key_field.set(written, Some(key))?;
                            value_field.set(written, values.get(index as usize)?)?;
                            written += 1;
                        }
                    } else {
                        // SAFETY: the in-bounds parent's mask is stable; MAP NULLs do not null shared entries.
                        unsafe { *parent_validity.add(row / 64) &= !(1u64 << (row % 64)) };
                    }
                    // SAFETY: this in-bounds descriptor covers only the initialized entries just written.
                    unsafe {
                        entry_data.add(row).write(ffi::duckdb_v2_list_entry {
                            offset: offset as u64,
                            length: (written - offset) as u64,
                        });
                    }
                }
            }
            field.set_child_offset(written)?;
        }
        {
            let mut field = fields.get_vector_at_mut::<UnionInputRef<'_, UncheckedString>>(3)?;
            let parent = unions.reader()?;
            let children = unions.children().unwrap();
            let tags = children.get_vector_at::<u8>(0)?.reader()?;
            let numbers = children.get_vector_at::<i32>(1)?.reader()?;
            let strings = children.get_vector_at::<UncheckedString>(2)?.reader()?;
            super::ensure(parent.len() == rows, || {
                "UNION row count differs from the input chunk".to_owned()
            })?;

            let parent_validity = {
                let mut writer = field.writer()?;
                // SAFETY: every row and its members are written or nulled below.
                unsafe {
                    writer.fill_validity(true)?;
                    writer.validity_mut()?.as_mut_ptr()
                }
            };
            let mut output_children = field.children_mut().unwrap();
            let [tag_vector, number_vector, string_vector] = output_children.get_vector_array_mut([0, 1, 2])?;
            let mut tag_vector = tag_vector.cast::<u8>()?;
            let mut number_vector = number_vector.cast::<i32>()?;
            let mut string_vector = string_vector.cast::<String>()?;
            let mut tag_field = tag_vector.writer()?;
            let mut number_field = number_vector.writer()?;
            let mut string_field = string_vector.writer()?;

            for row in 0..parent.len() {
                if parent.is_null(row)? {
                    // SAFETY: the parent's mask stays allocated; the in-bounds row and all members become NULL.
                    unsafe { *parent_validity.add(row / 64) &= !(1u64 << (row % 64)) };
                    tag_field.set(row, None)?;
                    number_field.set(row, None)?;
                    string_field.set(row, None)?;
                    continue;
                }
                match tags.get(row)? {
                    Some(&0) => {
                        tag_field.set(row, Some(&0))?;
                        number_field.set(row, numbers.get(row)?)?;
                        string_field.set(row, None)?;
                    }
                    Some(&1) => {
                        tag_field.set(row, Some(&1))?;
                        number_field.set(row, None)?;
                        string_field.set(row, strings.get(row)?)?;
                    }
                    _ => return Err(Error::invalid_input("invalid UNION tag")),
                }
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
