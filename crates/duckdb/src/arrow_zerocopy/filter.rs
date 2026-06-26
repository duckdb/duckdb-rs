//! Filter buffer decoder and evaluator for the zero-copy Arrow registration.
//!
//! The C++ shim serialises DuckDB's `TableFilterSet` into a little-endian byte buffer (the
//! "wire format"); this module decodes it into a `FilterNode` tree and evaluates it against
//! a projected `RecordBatch`, producing a `BooleanArray` mask for `filter_record_batch`.

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, RecordBatch, Scalar, StringArray, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::compute::kernels::{boolean, cmp};
use arrow::compute::{is_not_null, is_null};
use arrow::datatypes::DataType;

// ──────────────────────────────────────────────────────────────────────────────
// Public types
// ──────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) enum FilterError {
    Unhandled(String),
}

impl std::fmt::Display for FilterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterError::Unhandled(s) => write!(f, "unhandled filter: {s}"),
        }
    }
}

#[derive(Debug)]
pub(crate) enum ScalarVal {
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
    Utf8(String),
    Null,
    Unsupported,
}

#[derive(Debug)]
pub(crate) enum CmpOp {
    Eq,
    Ne,
    Lt,
    Gt,
    Le,
    Ge,
}

#[derive(Debug)]
pub(crate) enum FilterNode {
    Compare { path: Vec<usize>, op: CmpOp, val: ScalarVal },
    IsNull { path: Vec<usize> },
    IsNotNull { path: Vec<usize> },
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    In { path: Vec<usize>, vals: Vec<ScalarVal> },
    Unhandled(u32),
}

// ──────────────────────────────────────────────────────────────────────────────
// Decoder
// ──────────────────────────────────────────────────────────────────────────────

struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn read_u8(&mut self) -> u8 {
        let v = self.buf.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        v
    }

    fn read_u32(&mut self) -> u32 {
        let b0 = self.read_u8() as u32;
        let b1 = self.read_u8() as u32;
        let b2 = self.read_u8() as u32;
        let b3 = self.read_u8() as u32;
        b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)
    }

    fn read_i64(&mut self) -> i64 {
        let mut v: u64 = 0;
        for i in 0..8u64 {
            v |= (self.read_u8() as u64) << (i * 8);
        }
        v as i64
    }

    fn read_u64(&mut self) -> u64 {
        let mut v: u64 = 0;
        for i in 0..8u64 {
            v |= (self.read_u8() as u64) << (i * 8);
        }
        v
    }

    fn read_f64(&mut self) -> f64 {
        let bits = self.read_u64();
        f64::from_bits(bits)
    }

    fn read_path(&mut self) -> Vec<usize> {
        let depth = self.read_u32() as usize;
        (0..depth).map(|_| self.read_u32() as usize).collect()
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn read_scalar(&mut self) -> ScalarVal {
        if self.remaining() == 0 {
            return ScalarVal::Unsupported;
        }
        match self.read_u8() {
            0 => ScalarVal::I64(self.read_i64()),
            1 => ScalarVal::U64(self.read_u64()),
            2 => ScalarVal::F64(self.read_f64()),
            3 => ScalarVal::Bool(self.read_u8() != 0),
            4 => {
                let len = self.read_u32() as usize;
                let bytes = self.buf.get(self.pos..self.pos + len).unwrap_or(&[]).to_vec();
                self.pos += len;
                match String::from_utf8(bytes) {
                    Ok(s) => ScalarVal::Utf8(s),
                    Err(_) => ScalarVal::Unsupported,
                }
            }
            5 => ScalarVal::Null,
            _ => ScalarVal::Unsupported,
        }
    }

    fn read_node(&mut self) -> FilterNode {
        self.read_node_inner(0)
    }

    fn read_node_inner(&mut self, depth: usize) -> FilterNode {
        if depth > 64 {
            return FilterNode::Unhandled(255);
        }
        if self.remaining() == 0 {
            return FilterNode::Unhandled(255);
        }
        match self.read_u8() {
            0 => {
                let path = self.read_path();
                let op_byte = self.read_u8();
                let op = match op_byte {
                    0 => CmpOp::Eq,
                    1 => CmpOp::Ne,
                    2 => CmpOp::Lt,
                    3 => CmpOp::Gt,
                    4 => CmpOp::Le,
                    5 => CmpOp::Ge,
                    _ => {
                        // op is unknown, still read the scalar to advance the cursor
                        let _val = self.read_scalar();
                        return FilterNode::Unhandled(255);
                    }
                };
                let val = self.read_scalar();
                FilterNode::Compare { path, op, val }
            }
            1 => FilterNode::IsNull { path: self.read_path() },
            2 => FilterNode::IsNotNull { path: self.read_path() },
            3 => {
                let n = self.read_u32() as usize;
                let children = (0..n).map(|_| self.read_node_inner(depth + 1)).collect();
                FilterNode::And(children)
            }
            4 => {
                let n = self.read_u32() as usize;
                let children = (0..n).map(|_| self.read_node_inner(depth + 1)).collect();
                FilterNode::Or(children)
            }
            5 => {
                let path = self.read_path();
                let n = self.read_u32() as usize;
                let vals = (0..n).map(|_| self.read_scalar()).collect();
                FilterNode::In { path, vals }
            }
            255 => FilterNode::Unhandled(self.read_u32()),
            tag => FilterNode::Unhandled(tag as u32),
        }
    }
}

/// Decode the wire-format filter buffer into a `FilterNode` tree.
/// An empty buffer means "keep all rows" → returns `And([])`.
pub(crate) fn decode(buf: &[u8]) -> FilterNode {
    if buf.is_empty() {
        return FilterNode::And(vec![]);
    }
    let mut cursor = Cursor::new(buf);
    cursor.read_node()
}

// ──────────────────────────────────────────────────────────────────────────────
// Evaluator
// ──────────────────────────────────────────────────────────────────────────────

/// Resolve a column path to its leaf array plus an optional "ancestor-valid" mask.
/// `path[0]` selects the projected column; each subsequent index descends into a
/// `StructArray` child. The returned mask (when `Some`) is `true` where every ancestor
/// struct is non-null at that row; `None` means no ancestor struct carried nulls.
fn resolve_path(
    batch: &RecordBatch,
    path: &[usize],
) -> Result<(ArrayRef, Option<BooleanArray>), FilterError> {
    let root = *path
        .first()
        .ok_or_else(|| FilterError::Unhandled("empty column path".into()))?;
    if root >= batch.num_columns() {
        return Err(FilterError::Unhandled(format!("column index {root} out of range")));
    }
    let mut arr: ArrayRef = batch.column(root).clone();
    let mut ancestor_valid: Option<BooleanArray> = None;
    for &idx in &path[1..] {
        let sa = arr.as_any().downcast_ref::<StructArray>().ok_or_else(|| {
            FilterError::Unhandled(format!("path descends into non-struct: {:?}", arr.data_type()))
        })?;
        if let Some(nulls) = sa.nulls() {
            // Collect per-logical-element validity (true = non-null). `iter()` applies the
            // NullBuffer's offset/len, so this is correct even for a sliced StructArray
            // (a user may register a sliced batch) — unlike cloning the raw inner buffer.
            let this_valid: BooleanArray = nulls.iter().collect();
            ancestor_valid = Some(match ancestor_valid {
                Some(prev) => boolean::and(&prev, &this_valid).map_err(|e| FilterError::Unhandled(e.to_string()))?,
                None => this_valid,
            });
        }
        let child = sa
            .columns()
            .get(idx)
            .ok_or_else(|| FilterError::Unhandled(format!("struct child index {idx} out of range")))?;
        arr = child.clone();
    }
    Ok((arr, ancestor_valid))
}

/// Evaluate a filter node against a projected `RecordBatch`, returning a boolean mask.
/// Column paths in the `FilterNode` refer to positions in the projected batch.
pub(crate) fn evaluate(node: &FilterNode, batch: &RecordBatch) -> Result<BooleanArray, FilterError> {
    match node {
        FilterNode::Unhandled(k) => Err(FilterError::Unhandled(format!("TableFilterType {k}"))),

        FilterNode::And(children) => {
            let mut acc = BooleanArray::from(vec![true; batch.num_rows()]);
            for c in children {
                let child_mask = evaluate(c, batch)?;
                acc = boolean::and(&acc, &child_mask).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            }
            Ok(acc)
        }

        FilterNode::Or(children) => {
            let mut acc = BooleanArray::from(vec![false; batch.num_rows()]);
            for c in children {
                let child_mask = evaluate(c, batch)?;
                acc = boolean::or(&acc, &child_mask).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            }
            Ok(acc)
        }

        FilterNode::IsNull { path } => {
            let (col, valid) = resolve_path(batch, path)?;
            let mut m = is_null(col.as_ref()).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            if let Some(v) = valid {
                let not_v = boolean::not(&v).map_err(|e| FilterError::Unhandled(e.to_string()))?;
                m = boolean::or(&m, &not_v).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            }
            Ok(m)
        }

        FilterNode::IsNotNull { path } => {
            let (col, valid) = resolve_path(batch, path)?;
            let mut m = is_not_null(col.as_ref()).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            if let Some(v) = valid {
                m = boolean::and(&m, &v).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            }
            Ok(m)
        }

        FilterNode::In { path, vals } => {
            // OR of equality comparisons; NULLs in the column are never matched (DuckDB semantics).
            let (col_arr, valid) = resolve_path(batch, path)?;
            let mut acc = BooleanArray::from(vec![false; batch.num_rows()]);
            for val in vals {
                let eq_mask = compare(&col_arr, &CmpOp::Eq, val)?;
                // `compare` with a non-null scalar returns a BooleanArray where null column
                // entries produce null in the mask — coerce them to false so they don't match.
                let n = eq_mask.len();
                let eq_no_null = BooleanArray::from(
                    (0..n)
                        .map(|i| eq_mask.is_valid(i) && eq_mask.value(i))
                        .collect::<Vec<bool>>(),
                );
                acc = boolean::or(&acc, &eq_no_null).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            }
            if let Some(v) = valid {
                acc = boolean::and(&acc, &v).map_err(|e| FilterError::Unhandled(e.to_string()))?;
            }
            Ok(acc)
        }

        FilterNode::Compare { path, op, val } => {
            let (col, valid) = resolve_path(batch, path)?;
            let mask = compare(&col, op, val)?;
            match valid {
                Some(v) => boolean::and(&mask, &v).map_err(|e| FilterError::Unhandled(e.to_string())),
                None => Ok(mask),
            }
        }
    }
}

/// Build a one-element typed scalar matching the column's DataType from the incoming `val`,
/// then dispatch the comparison kernel.
///
/// The column DataType drives the cast so that the arrow-compute kernel sees matching types
/// on both sides (e.g. `Int32Array` column + `Int32Array` scalar, not Int32 vs Int64).
/// DuckDB pushes scalars as i64/u64/f64/bool/utf8; we widen/narrow to the column type.
/// Any scalar/column-type combo we haven't explicitly mapped → `FilterError` (never silently wrong).
fn compare(col: &ArrayRef, op: &CmpOp, val: &ScalarVal) -> Result<BooleanArray, FilterError> {
    if matches!(val, ScalarVal::Null) {
        // NULL comparison always yields false (SQL semantics).
        return Ok(BooleanArray::from(vec![false; col.len()]));
    }
    if matches!(val, ScalarVal::Unsupported) {
        return Err(FilterError::Unhandled("unsupported scalar type: Unsupported".into()));
    }

    let col_type = col.data_type();

    match (col_type, val) {
        // ── Signed integers (DuckDB pushes as I64) ─────────────────────────────
        (DataType::Int64, ScalarVal::I64(v)) => dispatch(op, col, &Scalar::new(Int64Array::from(vec![*v]))),
        // Narrowing uses checked `try_from`: an out-of-range pushed constant must fail loud
        // (→ error_stream), never silently wrap and produce a wrong row set.
        (DataType::Int32, ScalarVal::I64(v)) => {
            let n = i32::try_from(*v)
                .map_err(|_| FilterError::Unhandled(format!("i64 value {v} out of range for Int32 column")))?;
            dispatch(op, col, &Scalar::new(Int32Array::from(vec![n])))
        }
        (DataType::Int16, ScalarVal::I64(v)) => {
            let n = i16::try_from(*v)
                .map_err(|_| FilterError::Unhandled(format!("i64 value {v} out of range for Int16 column")))?;
            dispatch(op, col, &Scalar::new(Int16Array::from(vec![n])))
        }
        (DataType::Int8, ScalarVal::I64(v)) => {
            let n = i8::try_from(*v)
                .map_err(|_| FilterError::Unhandled(format!("i64 value {v} out of range for Int8 column")))?;
            dispatch(op, col, &Scalar::new(Int8Array::from(vec![n])))
        }

        // ── Unsigned integers (DuckDB pushes as U64) ───────────────────────────
        (DataType::UInt64, ScalarVal::U64(v)) => dispatch(op, col, &Scalar::new(UInt64Array::from(vec![*v]))),
        (DataType::UInt32, ScalarVal::U64(v)) => {
            let n = u32::try_from(*v)
                .map_err(|_| FilterError::Unhandled(format!("u64 value {v} out of range for UInt32 column")))?;
            dispatch(op, col, &Scalar::new(UInt32Array::from(vec![n])))
        }
        (DataType::UInt16, ScalarVal::U64(v)) => {
            let n = u16::try_from(*v)
                .map_err(|_| FilterError::Unhandled(format!("u64 value {v} out of range for UInt16 column")))?;
            dispatch(op, col, &Scalar::new(UInt16Array::from(vec![n])))
        }
        (DataType::UInt8, ScalarVal::U64(v)) => {
            let n = u8::try_from(*v)
                .map_err(|_| FilterError::Unhandled(format!("u64 value {v} out of range for UInt8 column")))?;
            dispatch(op, col, &Scalar::new(UInt8Array::from(vec![n])))
        }

        // ── Floats (DuckDB pushes as F64) ──────────────────────────────────────
        (DataType::Float64, ScalarVal::F64(v)) => dispatch(op, col, &Scalar::new(Float64Array::from(vec![*v]))),
        // Float32: f64 → f32 cast is a deliberate precision round, not a correctness issue
        (DataType::Float32, ScalarVal::F64(v)) => dispatch(op, col, &Scalar::new(Float32Array::from(vec![*v as f32]))),

        // ── Boolean ────────────────────────────────────────────────────────────
        (DataType::Boolean, ScalarVal::Bool(v)) => dispatch(op, col, &Scalar::new(BooleanArray::from(vec![*v]))),

        // ── Strings ────────────────────────────────────────────────────────────
        (DataType::Utf8, ScalarVal::Utf8(v)) => dispatch(op, col, &Scalar::new(StringArray::from(vec![v.as_str()]))),
        // LargeUtf8: present for completeness; DuckDB's arrow_scan rarely emits it
        (DataType::LargeUtf8, ScalarVal::Utf8(v)) => {
            dispatch(op, col, &Scalar::new(LargeStringArray::from(vec![v.as_str()])))
        }

        // ── Anything else → clean error (never silently wrong) ─────────────────
        _ => Err(FilterError::Unhandled(format!(
            "unsupported column/scalar combination: {:?} / {:?}",
            col_type, val
        ))),
    }
}

fn dispatch(op: &CmpOp, l: &dyn arrow::array::Datum, r: &dyn arrow::array::Datum) -> Result<BooleanArray, FilterError> {
    let out = match op {
        CmpOp::Eq => cmp::eq(l, r),
        CmpOp::Ne => cmp::neq(l, r),
        CmpOp::Lt => cmp::lt(l, r),
        CmpOp::Gt => cmp::gt(l, r),
        CmpOp::Le => cmp::lt_eq(l, r),
        CmpOp::Ge => cmp::gt_eq(l, r),
    };
    out.map_err(|e| FilterError::Unhandled(e.to_string()))
}
