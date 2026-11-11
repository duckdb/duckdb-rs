//! `dash_split(input, n)`: a table function that splits `input` on `-` into at most `n` parts,
//! like [`str::splitn`], producing one `(idx, part)` row per part.

use std::sync::Mutex;

use duckdb_neo::{
    DuckDBType, Error, Parameters, Result,
    bind_arguments::BindArgument,
    connection::{Connection, Context},
    data_chunk::DataChunkRef,
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder, TableFunctionCallbacks,
    },
    types::UncheckedString,
};

pub struct DashSplit;

pub struct SplitBind {
    input: String,
    limit: usize,
}

/// Where the next part starts; `None` once every part was produced.
pub struct SplitScan {
    cursor: Mutex<Option<(usize, usize)>>,
}

impl TableFunctionCallbacks for DashSplit {
    type BindData = SplitBind;
    type GlobalState = SplitScan;
    /// Byte ranges of the parts in the current chunk.
    type LocalState = Vec<(usize, usize)>;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let constant = |index: usize| {
            arguments[index]
                .value
                .as_ref()
                .ok_or_else(|| Error::invalid_input("dash_split arguments must be constant"))
        };
        let input = constant(0)?.get::<String>()?;
        let limit = match constant(1)?.get::<i64>()? {
            Some(limit) => {
                usize::try_from(limit).map_err(|_| Error::invalid_input("dash_split n must not be negative"))?
            }
            None => 0,
        };
        // NULL arguments produce no rows.
        let (input, limit) = match input {
            Some(input) => (input, limit),
            None => (String::new(), 0),
        };

        bind_handle.add_result_column("idx", &i64::logical_type(context)?)?;
        bind_handle.add_result_column("part", &String::logical_type(context)?)?;
        let parts = input.bytes().filter(|&byte| byte == b'-').count() + 1;
        bind_handle.set_cardinality(parts.min(limit), true)?;
        Ok(SplitBind { input, limit })
    }

    fn init_global_state(
        &self,
        bind_data: &Self::BindData,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        let cursor = (bind_data.limit > 0).then_some((0, 0));
        Ok((
            Some(SplitScan {
                cursor: Mutex::new(cursor),
            }),
            Some(1),
        ))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(Some(Vec::new()))
    }

    fn exec(
        &self,
        bind_data: &Self::BindData,
        global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
        column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let scan = global_state.expect("init_global_state sets the scan");
        let parts = local_state.expect("init_local_state sets the part buffer");
        let input = bind_data.input.as_str();
        let capacity = output.capacity()?;
        parts.clear();

        let first_index = {
            let mut cursor = scan.cursor.lock().unwrap();
            let first_index = cursor.map_or(0, |(_, index)| index);
            while parts.len() < capacity {
                let Some((start, index)) = *cursor else { break };
                let separator = (index + 1 < bind_data.limit)
                    .then(|| input[start..].find('-'))
                    .flatten();
                *cursor = match separator {
                    Some(len) => {
                        parts.push((start, start + len));
                        Some((start + len + 1, index + 1))
                    }
                    None => {
                        parts.push((start, input.len()));
                        None
                    }
                };
            }
            first_index
        };

        for position in 0..column_info.count()? {
            match column_info.get_column_index(position)? {
                0 => {
                    let mut vector = output.get_vector_at_mut::<i64>(position)?;
                    // SAFETY: the loop below writes every row.
                    unsafe { vector.set_size_uninit(parts.len())? };
                    let mut writer = vector.writer()?;
                    for row in 0..parts.len() {
                        writer.set(row, &((first_index + row) as i64))?;
                    }
                }
                _ => {
                    let mut vector = output.get_vector_at_mut::<String>(position)?;
                    // SAFETY: the loop below writes every row.
                    unsafe { vector.set_size_uninit(parts.len())? };
                    let mut writer = vector.writer()?;
                    for (row, &(start, end)) in parts.iter().enumerate() {
                        writer.set(row, &input[start..end])?;
                    }
                }
            }
        }
        Ok(())
    }
}

pub fn register(conn: &Connection) -> Result<()> {
    let signature = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("input"))
        .param(FunctionParameter::new::<i64>("n"));
    TableFunctionBuilder::new("dash_split", signature, DashSplit)
        .with_projection_pushdown(true)
        .register(conn)
}

/// Store a string of `rows` dash-separated parts in the `split_input` variable; a third
/// of the parts are too long to be inlined.
pub fn create_input(conn: &Connection, rows: usize) -> Result<()> {
    conn.execute(
        format!(
            "SET VARIABLE split_input = (
                 SELECT string_agg(CASE WHEN i % 3 = 0 THEN 'long_segment_' || i ELSE 'p' || i END, '-' ORDER BY i)
                 FROM range({rows}) t(i))"
        )
        .as_str(),
        Parameters::None,
    )?;
    Ok(())
}

pub fn query(rows: usize) -> String {
    format!("SELECT idx, part FROM dash_split(getvariable('split_input'), {rows})")
}

/// Check `dash_split` against [`str::splitn`] for edge cases and `split_input`.
pub fn verify(conn: &Connection) -> Result<()> {
    for (input, limit) in [
        ("a-b-c", 2),
        ("a-b-c", 3),
        ("a-b-c", 10),
        ("-a--b-", 10),
        ("", 1),
        ("abc", 0),
        ("a-b", 1),
    ] {
        let sql = format!("SELECT idx, part FROM dash_split('{input}', {limit}) ORDER BY idx");
        let expected: Vec<_> = input
            .splitn(limit, '-')
            .enumerate()
            .map(|(i, p)| (i as i64, p.to_owned()))
            .collect();
        let actual = collect_parts(conn, &sql)?;
        super::ensure(actual == expected, || {
            format!("dash_split({input:?}, {limit}) returned {actual:?}, expected {expected:?}")
        })?;
    }
    let parts = super::query_string(conn, "SELECT string_agg(part, ',') FROM dash_split('a-b-c', 3)")?;
    super::ensure(parts == "a,b,c", || format!("projecting only part returned {parts:?}"))?;
    let rows = super::query_i64(conn, "SELECT count(*) FROM dash_split(NULL, 3)")?;
    super::ensure(rows == 0, || format!("dash_split(NULL, 3) returned {rows} rows"))?;

    let input = super::query_string(conn, "SELECT getvariable('split_input')")?;
    let expected = input.split('-').count();
    let actual = collect_parts(conn, &query(expected))?;
    verify_parts(&input, &actual)?;
    let count = super::query_i64(conn, &format!("SELECT count(*) FROM ({})", query(expected)))?;
    super::ensure(count as usize == expected, || {
        format!("count(*) returned {count}, expected {expected}")
    })
}

fn verify_parts(input: &str, actual: &[(i64, String)]) -> Result<()> {
    let expected = input.split('-').enumerate().map(|(index, part)| (index as i64, part));
    super::ensure(
        actual.iter().map(|(index, part)| (*index, part.as_str())).eq(expected),
        || "dash_split(split_input) returned different indices or parts".to_owned(),
    )
}

fn collect_parts(conn: &Connection, sql: &str) -> Result<Vec<(i64, String)>> {
    let mut parts = Vec::new();
    let mut result = conn.query(sql, Parameters::None)?;
    while let Some(chunk) = result.next_chunk()? {
        let indexes = chunk.get_vector_at::<i64>(0)?.reader()?;
        let values = chunk.get_vector_at::<UncheckedString>(1)?.reader()?;
        for (index, value) in indexes.iter().zip(values.iter()) {
            parts.push((*index.unwrap(), value.unwrap().to_owned()));
        }
    }
    Ok(parts)
}

#[cfg(test)]
mod tests {
    #[test]
    fn verify_parts_rejects_restarted_indices() {
        let input = (0..5_000)
            .map(|index| format!("p{index}"))
            .collect::<Vec<_>>()
            .join("-");
        let mut actual: Vec<_> = input
            .split('-')
            .enumerate()
            .map(|(index, part)| (index as i64, part.to_owned()))
            .collect();
        super::verify_parts(&input, &actual).unwrap();
        for (index, _) in &mut actual {
            *index %= 2_048;
        }
        assert!(super::verify_parts(&input, &actual).is_err());
    }
}
