//! `row_hash(s, d, b)`: an aggregate that combines `VARCHAR`, `DOUBLE` and `BOOLEAN` rows into
//! one `UBIGINT` hash. Rows are hashed independently and summed, so the result does not
//! depend on row order or on how DuckDB partitions the groups.

use std::collections::HashMap;

use duckdb_neo::{
    DuckDBType, Error, Parameters, Result,
    aggregate::{AggregateCallbacks, AggregateFunctionBuilder, ReturnTypeHandle, States},
    bind_arguments::BindArguments,
    connection::{Connection, Context},
    data_chunk::VectorCollection,
    enums::AggregateFunctionProperty,
    signature::{FunctionParameter, SignatureBuilder},
    types::UncheckedString,
    vector::{CombinedVectorIterator, Unknown, Vector},
};

use super::readable;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        hash = (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Hash one row; a NULL field hashes differently from every value.
pub fn hash_row(s: Option<&str>, d: Option<f64>, b: Option<bool>) -> u64 {
    let mut hash = FNV_OFFSET;
    hash = match s {
        Some(s) => fnv(fnv(hash, &[1]), s.as_bytes()),
        None => fnv(hash, &[0]),
    };
    hash = match d {
        Some(d) => fnv(fnv(hash, &[1]), &d.to_bits().to_le_bytes()),
        None => fnv(hash, &[0]),
    };
    hash = fnv(hash, &[b.map_or(0, |b| 1 + u8::from(b))]);
    // The splitmix64 finalizer spreads FNV's weak low bits before rows are summed.
    hash = (hash ^ (hash >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    hash = (hash ^ (hash >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    hash ^ (hash >> 31)
}

#[derive(Debug, Default)]
pub struct HashState {
    hash: u64,
    rows: u64,
}

pub struct RowHash;

impl AggregateCallbacks for RowHash {
    type BindData = ();
    type StateItem = HashState;

    fn bind(
        &self,
        _context: &Context,
        _arguments: BindArguments<'_>,
        _result_type_handle: ReturnTypeHandle<'_>,
    ) -> Result<Self::BindData> {
        Ok(())
    }

    fn init(&self, _bind_data: Option<&()>) -> Self::StateItem {
        HashState::default()
    }

    fn update(
        &self,
        _bind_data: Option<&()>,
        data: &mut VectorCollection,
        mut states: States<'_, HashState>,
    ) -> Result<()> {
        let [s, d, b] = data.get_vector_array_mut([0, 1, 2])?;
        let (s, d, b) = (
            readable::<UncheckedString>(s)?,
            readable::<f64>(d)?,
            readable::<bool>(b)?,
        );
        let rows = CombinedVectorIterator::new((s.reader()?, d.reader()?, b.reader()?))?;
        for (row, (s, d, b)) in rows.enumerate() {
            let state = &mut states[row];
            state.hash = state.hash.wrapping_add(hash_row(s, d.copied(), b.copied()));
            state.rows += 1;
        }
        Ok(())
    }

    fn combine(
        &self,
        _bind_data: Option<&()>,
        source: &States<'_, HashState>,
        mut target: States<'_, HashState>,
    ) -> Result<()> {
        for index in 0..target.len() {
            let source = &source[index];
            let target = &mut target[index];
            target.hash = target.hash.wrapping_add(source.hash);
            target.rows += source.rows;
        }
        Ok(())
    }

    fn finalize(
        &self,
        _bind_data: Option<&()>,
        states: &[&HashState],
        result: Vector<'_, Unknown>,
        result_offset: usize,
    ) -> Result<()> {
        let mut result = result.cast::<u64>()?;
        let mut writer = result.writer()?;
        for (index, state) in states.iter().enumerate() {
            writer.set(result_offset + index, (state.rows > 0).then_some(state.hash))?;
        }
        Ok(())
    }
}

pub fn register(conn: &Connection) -> Result<()> {
    let signature = SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("s"))
        .param(FunctionParameter::new::<f64>("d"))
        .param(FunctionParameter::new::<bool>("b"))
        .return_type(u64::logical_type(conn)?);
    AggregateFunctionBuilder::new("row_hash", signature, RowHash)
        .set_property(AggregateFunctionProperty::AggregateOrderDependent(false))
        .register(conn)
}

/// Number of groups in `agg_input`.
pub const GROUPS: usize = 1024;

/// Create `agg_input` with `rows` rows of `row_hash` arguments in [`GROUPS`] groups.
pub fn create_input(conn: &Connection, rows: usize) -> Result<()> {
    conn.execute(
        format!(
            "CREATE OR REPLACE TABLE agg_input AS
             SELECT (i % {GROUPS})::INTEGER AS g,
                    CASE WHEN i % 89 = 0 THEN NULL ELSE 'key_' || (i * 7919 % 100003) END AS s,
                    CASE WHEN i % 97 = 0 THEN NULL ELSE i * 0.25 END::DOUBLE AS d,
                    CASE WHEN i % 101 = 0 THEN NULL ELSE i % 3 = 0 END AS b
             FROM range({rows}) t(i)"
        )
        .as_str(),
        Parameters::None,
    )?;
    Ok(())
}

pub const UNGROUPED_QUERY: &str = "SELECT row_hash(s, d, b) FROM agg_input";
pub const GROUPED_QUERY: &str = "SELECT g, row_hash(s, d, b) FROM agg_input GROUP BY g";

/// Check `row_hash` over `agg_input` against [`hash_row`], both overall and per group.
pub fn verify(conn: &Connection) -> Result<()> {
    let mut expected = 0u64;
    let mut expected_groups = HashMap::new();
    let mut result = conn.query("SELECT g, s, d, b FROM agg_input", Parameters::None)?;
    while let Some(chunk) = result.next_chunk()? {
        let g = chunk.get_vector_at::<i32>(0)?.reader()?;
        let s = chunk.get_vector_at::<UncheckedString>(1)?.reader()?;
        let d = chunk.get_vector_at::<f64>(2)?.reader()?;
        let b = chunk.get_vector_at::<bool>(3)?.reader()?;
        for (g, s, d, b) in CombinedVectorIterator::new((g, s, d, b))? {
            let hash = hash_row(s, d.copied(), b.copied());
            expected = expected.wrapping_add(hash);
            let group = expected_groups.entry(g.copied()).or_insert(0u64);
            *group = group.wrapping_add(hash);
        }
    }
    drop(result);

    let ungrouped = super::query_u64(conn, UNGROUPED_QUERY)?;
    super::ensure(ungrouped == Some(expected), || {
        format!("row_hash returned {ungrouped:?} ungrouped, expected {expected}")
    })?;
    let mut grouped = Vec::new();
    let mut result = conn.query(GROUPED_QUERY, Parameters::None)?;
    while let Some(chunk) = result.next_chunk()? {
        let groups = chunk.get_vector_at::<i32>(0)?.reader()?;
        let hashes = chunk.get_vector_at::<u64>(1)?.reader()?;
        grouped.extend(CombinedVectorIterator::new((groups, hashes))?.map(|(g, hash)| (g.copied(), hash.copied())));
    }
    drop(result);
    verify_groups(expected_groups, &grouped)?;
    let empty = super::query_u64(conn, "SELECT row_hash(s, d, b) FROM agg_input WHERE false")?;
    super::ensure(empty.is_none(), || format!("row_hash over no rows returned {empty:?}"))
}

fn verify_groups(mut expected: HashMap<Option<i32>, u64>, actual: &[(Option<i32>, Option<u64>)]) -> Result<()> {
    for &(group, hash) in actual {
        let expected_hash = expected.remove(&group).ok_or_else(|| {
            Error::invalid_input(format!("row_hash returned unexpected or duplicate group {group:?}"))
        })?;
        super::ensure(hash == Some(expected_hash), || {
            format!("row_hash returned {hash:?} for group {group:?}, expected {expected_hash}")
        })?;
    }
    super::ensure(expected.is_empty(), || {
        format!("row_hash omitted {} groups", expected.len())
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn verify_groups_rejects_swapped_hashes() {
        use super::{HashMap, verify_groups};

        let expected = HashMap::from([(Some(0), 100), (Some(1), 200)]);
        verify_groups(expected.clone(), &[(Some(1), Some(200)), (Some(0), Some(100))]).unwrap();
        assert!(verify_groups(expected, &[(Some(0), Some(200)), (Some(1), Some(100))]).is_err());
    }

    #[test]
    fn verify_groups_rejects_invalid_groups() {
        use super::{HashMap, verify_groups};

        let expected = HashMap::from([(Some(0), 100), (Some(1), 200)]);
        for actual in [
            vec![(Some(0), Some(100))],
            vec![(Some(0), Some(100)), (Some(0), Some(200))],
            vec![(Some(0), Some(100)), (Some(2), Some(200))],
            vec![(Some(0), None), (Some(1), Some(200))],
            vec![(None, Some(100)), (Some(1), Some(200))],
        ] {
            assert!(verify_groups(expected.clone(), &actual).is_err(), "accepted {actual:?}");
        }
    }
}
