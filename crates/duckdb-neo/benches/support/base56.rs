//! The `base56` `COPY` format and its codec.
//!
//! A file starts with a header line naming the column types, e.g. `#base56 BIGINT,VARCHAR`,
//! followed by one line per row. Fields are separated by `,` and hold the base56 encoding
//! of the value's bytes: little-endian for numbers, UTF-8 for strings. `~` marks NULL.
//!
//! The codec encodes every 4 bytes as 6 digits and a trailing 1, 2 or 3 bytes as 2, 3 or 5
//! digits, so encoding is linear and every encoded length decodes unambiguously.

use std::{
    num::NonZeroUsize,
    ops::Range,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use duckdb_neo::{
    Error, Parameters, Result,
    column_data_collection::ColumnDataCollection,
    connection::{Connection, Context},
    copy_function::{
        CopyFromBindInfo, CopyFromCardinality, CopyFromFunctionCallbacks, CopyFunctionBuilder, CopyToBindInfo,
        CopyToFunctionCallbacks,
    },
    data_chunk::DataChunkRef,
    file::{File, FileBuilder, FileSystem},
    logical_type::{LogicalType, LogicalTypeID},
    types::UncheckedString,
    vector::{NativeElement, VectorReader, WritableNativeElement},
};

/// Digits and letters without the look-alikes `0`, `1`, `I`, `O`, `l` and `o`.
pub const ALPHABET: &[u8; 56] = b"23456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnpqrstuvwxyz";

const NULL_FIELD: u8 = b'~';
const FIELD_SEPARATOR: u8 = b',';
const ROW_SEPARATOR: u8 = b'\n';
const HEADER_PREFIX: &str = "#base56 ";

#[cfg(test)]
#[path = "delimiter_checks.rs"]
mod delimiter_checks;

/// Encoded digits for a tail of 0 to 3 bytes.
const TAIL_DIGITS: [usize; 4] = [0, 2, 3, 5];

const DECODE: [u8; 256] = {
    let mut table = [u8::MAX; 256];
    let mut digit = 0;
    while digit < ALPHABET.len() {
        table[ALPHABET[digit] as usize] = digit as u8;
        digit += 1;
    }
    table
};

/// Append the base56 encoding of `bytes` to `out`.
pub fn encode_into(bytes: &[u8], out: &mut Vec<u8>) {
    let (blocks, tail) = bytes.as_chunks::<4>();
    out.reserve(blocks.len() * 6 + TAIL_DIGITS[tail.len()]);
    for block in blocks {
        push_digits(u64::from(u32::from_le_bytes(*block)), 6, out);
    }
    if !tail.is_empty() {
        let mut padded = [0; 4];
        padded[..tail.len()].copy_from_slice(tail);
        push_digits(u64::from(u32::from_le_bytes(padded)), TAIL_DIGITS[tail.len()], out);
    }
}

/// Append the bytes encoded by `digits` to `out`.
pub fn decode_into(digits: &[u8], out: &mut Vec<u8>) -> Result<()> {
    let (blocks, tail) = digits.as_chunks::<6>();
    let tail_bytes = TAIL_DIGITS
        .iter()
        .position(|&len| len == tail.len())
        .ok_or_else(|| Error::invalid_input(format!("invalid base56 length {}", digits.len())))?;
    out.reserve(blocks.len() * 4 + tail_bytes);
    for block in blocks {
        out.extend_from_slice(&read_digits(block, 4)?.to_le_bytes());
    }
    if tail_bytes > 0 {
        out.extend_from_slice(&read_digits(tail, tail_bytes)?.to_le_bytes()[..tail_bytes]);
    }
    Ok(())
}

fn push_digits(mut value: u64, count: usize, out: &mut Vec<u8>) {
    let start = out.len();
    out.reserve(count);
    // SAFETY: capacity covers all `count` slots; the length changes only after they are initialized.
    unsafe {
        let data = out.as_mut_ptr().add(start);
        for index in (0..count).rev() {
            data.add(index).write(ALPHABET[(value % 56) as usize]);
            value /= 56;
        }
        out.set_len(start + count);
    }
}

fn read_digits(digits: &[u8], bytes: usize) -> Result<u32> {
    let mut value: u64 = 0;
    for &digit in digits {
        let decoded = DECODE[digit as usize];
        if decoded == u8::MAX {
            return Err(Error::invalid_input(format!(
                "invalid base56 digit {:?}",
                digit as char
            )));
        }
        value = value * 56 + u64::from(decoded);
    }
    if value >> (bytes * 8) != 0 {
        return Err(Error::invalid_input("base56 block overflows its byte count"));
    }
    Ok(value as u32)
}

/// Register `base56` for both `COPY ... TO` and `COPY ... FROM`.
pub fn register(conn: &Connection) -> Result<()> {
    CopyFunctionBuilder::new("base56", Base56)
        .with_fallback_extension("b56")
        .register_to_and_from(conn)
}

/// Column types the format stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Column {
    Boolean,
    Integer,
    BigInt,
    Double,
    Varchar,
}

impl Column {
    fn from_logical_type(logical_type: &LogicalType) -> Result<Self> {
        Ok(match logical_type.id()? {
            LogicalTypeID::Boolean => Self::Boolean,
            LogicalTypeID::Integer => Self::Integer,
            LogicalTypeID::BigInt => Self::BigInt,
            LogicalTypeID::Double => Self::Double,
            LogicalTypeID::Varchar => Self::Varchar,
            _ => {
                return Err(Error::invalid_input(format!(
                    "base56 does not support {}",
                    logical_type.to_text()?
                )));
            }
        })
    }

    fn name(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "INTEGER",
            Self::BigInt => "BIGINT",
            Self::Double => "DOUBLE",
            Self::Varchar => "VARCHAR",
        }
    }
}

fn header(columns: &[Column]) -> String {
    let names: Vec<_> = columns.iter().map(|column| column.name()).collect();
    format!("{HEADER_PREFIX}{}\n", names.join(","))
}

/// The `base56` format; stateless, all state lives in DuckDB's callback data.
pub struct Base56;

/// One output file of `COPY ... TO`.
pub struct Base56Writer {
    file: Mutex<File>,
}

impl CopyToFunctionCallbacks for Base56 {
    type InitData = Base56Writer;
    type BindData = Vec<Column>;
    type BatchData = Vec<u8>;

    fn bind(&self, _context: &Context, bind_info: &CopyToBindInfo) -> Result<Self::BindData> {
        (0..bind_info.column_count()?)
            .map(|index| Column::from_logical_type(&bind_info.get_column(index)?.1))
            .collect()
    }

    fn init(&self, context: &Context, columns: &Self::BindData, file_path: &str) -> Result<Self::InitData> {
        let fs = FileSystem::new(context)?;
        let file = FileBuilder::new(&fs, file_path)?.write().create_new().open()?;
        write_all(&file, header(columns).as_bytes())?;
        Ok(Base56Writer { file: Mutex::new(file) })
    }

    fn batch(
        &self,
        context: &Context,
        columns: &Self::BindData,
        _writer: &Self::InitData,
        input: &ColumnDataCollection<'_>,
    ) -> Result<Self::BatchData> {
        let mut encoded = Vec::new();
        let mut scan = input.to_scan(context)?;
        while let Some(chunk) = scan.next_chunk()? {
            encode_chunk(columns, &chunk, &mut encoded)?;
        }
        Ok(encoded)
    }

    fn flush(
        &self,
        _context: &Context,
        _columns: &Self::BindData,
        writer: &Self::InitData,
        _input: &ColumnDataCollection<'_>,
        encoded: &mut Self::BatchData,
    ) -> Result<()> {
        write_all(&writer.file.lock().unwrap(), encoded)
    }

    fn finalize(&self, _context: &Context, _columns: &Self::BindData, writer: &Self::InitData) -> Result<()> {
        writer.file.lock().unwrap().close()
    }
}

fn write_all(file: &File, mut bytes: &[u8]) -> Result<()> {
    while !bytes.is_empty() {
        let written = file.write(bytes)?;
        if written == 0 {
            return Err(Error::api_error("base56 output file accepted no bytes"));
        }
        bytes = &bytes[written..];
    }
    Ok(())
}

enum ColumnReader<'a> {
    Boolean(VectorReader<'a, bool>),
    Integer(VectorReader<'a, i32>),
    BigInt(VectorReader<'a, i64>),
    Double(VectorReader<'a, f64>),
    Varchar(VectorReader<'a, UncheckedString>),
}

impl ColumnReader<'_> {
    fn encode_row(&self, row: usize, out: &mut Vec<u8>) -> Result<()> {
        let encoded = match self {
            Self::Boolean(reader) => reader.get(row)?.map(|value| encode_into(&[u8::from(*value)], out)),
            Self::Integer(reader) => reader.get(row)?.map(|value| encode_into(&value.to_le_bytes(), out)),
            Self::BigInt(reader) => reader.get(row)?.map(|value| encode_into(&value.to_le_bytes(), out)),
            Self::Double(reader) => reader.get(row)?.map(|value| encode_into(&value.to_le_bytes(), out)),
            Self::Varchar(reader) => reader.get(row)?.map(|value| encode_into(value.as_bytes(), out)),
        };
        if encoded.is_none() {
            out.push(NULL_FIELD);
        }
        Ok(())
    }
}

fn encode_chunk(columns: &[Column], chunk: &DataChunkRef<'_>, out: &mut Vec<u8>) -> Result<()> {
    let readers = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Ok(match column {
                Column::Boolean => ColumnReader::Boolean(chunk.get_vector_at(index)?.reader()?),
                Column::Integer => ColumnReader::Integer(chunk.get_vector_at(index)?.reader()?),
                Column::BigInt => ColumnReader::BigInt(chunk.get_vector_at(index)?.reader()?),
                Column::Double => ColumnReader::Double(chunk.get_vector_at(index)?.reader()?),
                Column::Varchar => ColumnReader::Varchar(chunk.get_vector_at(index)?.reader()?),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    for row in 0..chunk.row_count()? {
        for (index, reader) in readers.iter().enumerate() {
            if index > 0 {
                out.push(FIELD_SEPARATOR);
            }
            reader.encode_row(row, out)?;
        }
        out.push(ROW_SEPARATOR);
    }
    Ok(())
}

/// A `COPY ... FROM` read: the whole file, and the start of the first unclaimed row.
pub struct Base56Reader {
    data: Vec<u8>,
    next_row: AtomicUsize,
}

/// Field byte ranges of the rows one exec call decodes, and a decoding buffer.
#[derive(Default)]
pub struct Base56Batch {
    fields: Vec<Range<usize>>,
    bytes: Vec<u8>,
}

impl CopyFromFunctionCallbacks for Base56 {
    type BindData = (String, Vec<Column>);
    type LocalState = Base56Batch;
    type GlobalState = Base56Reader;

    fn bind(
        &self,
        _context: &Context,
        file_path: &str,
        bind_info: &CopyFromBindInfo,
    ) -> Result<(Self::BindData, Option<CopyFromCardinality>)> {
        let columns = (0..bind_info.column_count()?)
            .map(|index| Column::from_logical_type(&bind_info.get_column(index)?.1))
            .collect::<Result<_>>()?;
        Ok(((file_path.to_owned(), columns), None))
    }

    fn init_global_state(
        &self,
        (file_path, columns): &Self::BindData,
        context: &Context,
    ) -> Result<(Option<Self::GlobalState>, Option<NonZeroUsize>)> {
        let fs = FileSystem::new(context)?;
        let file = FileBuilder::new(&fs, file_path)?.read().open()?;
        let mut data = vec![0; file.size()? as usize];
        file.read_exact_at(0, &mut data)?;

        let expected = header(columns);
        if !data.starts_with(expected.as_bytes()) {
            let found = data.split(|&byte| byte == ROW_SEPARATOR).next().unwrap_or_default();
            return Err(Error::invalid_input(format!(
                "base56 header {:?} does not match the target columns {:?}",
                String::from_utf8_lossy(found),
                expected.trim_end()
            )));
        }
        let reader = Base56Reader {
            next_row: AtomicUsize::new(expected.len()),
            data,
        };
        Ok((Some(reader), None))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _reader: Option<&Self::GlobalState>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(Some(Base56Batch::default()))
    }

    fn exec(
        &self,
        (_, columns): &Self::BindData,
        reader: Option<&Self::GlobalState>,
        batch: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
    ) -> Result<()> {
        let reader = reader.expect("init_global_state sets the reader");
        let batch = batch.expect("init_local_state sets the batch");
        let rows = claim_rows(reader, output.capacity()?, columns.len(), &mut batch.fields)?;

        for (index, column) in columns.iter().enumerate() {
            let fields = batch.fields.iter().skip(index).step_by(columns.len());
            let fields = fields.map(|range| &reader.data[range.clone()]);
            let (output, bytes) = (&mut output, &mut batch.bytes);
            match column {
                Column::Boolean => decode_column::<bool>(output, index, rows, fields, bytes)?,
                Column::Integer => decode_column::<i32>(output, index, rows, fields, bytes)?,
                Column::BigInt => decode_column::<i64>(output, index, rows, fields, bytes)?,
                Column::Double => decode_column::<f64>(output, index, rows, fields, bytes)?,
                Column::Varchar => decode_strings(output, index, rows, fields, bytes)?,
            }
        }
        Ok(())
    }
}

#[inline]
fn find_byte(data: &[u8], byte: u8) -> Option<usize> {
    memchr::memchr(byte, data)
}

fn claim_rows(reader: &Base56Reader, capacity: usize, columns: usize, fields: &mut Vec<Range<usize>>) -> Result<usize> {
    fields.clear();
    let data = &reader.data;
    let mut start = reader.next_row.load(Ordering::Acquire);
    loop {
        let (mut end, mut rows) = (start, 0);
        while rows < capacity && end < data.len() {
            let line_end = find_byte(&data[end..], ROW_SEPARATOR);
            end = line_end.map_or(data.len(), |offset| end + offset + 1);
            rows += 1;
        }
        match reader
            .next_row
            .compare_exchange(start, end, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => return split_fields(data, start..end, columns, fields).map(|()| rows),
            // Another worker claimed these rows first.
            Err(current) => start = current,
        }
    }
}

fn split_fields(data: &[u8], rows: Range<usize>, columns: usize, fields: &mut Vec<Range<usize>>) -> Result<()> {
    let mut line_start = rows.start;
    while line_start < rows.end {
        let line_end =
            find_byte(&data[line_start..rows.end], ROW_SEPARATOR).map_or(rows.end, |offset| line_start + offset);
        let mut field_start = line_start;
        let mut count = 0;
        loop {
            let field_end = find_byte(&data[field_start..line_end], FIELD_SEPARATOR)
                .map_or(line_end, |offset| field_start + offset);
            fields.push(field_start..field_end);
            count += 1;
            if field_end == line_end {
                break;
            }
            field_start = field_end + 1;
        }
        if count != columns {
            return Err(Error::invalid_input(format!(
                "base56 row has {count} fields, expected {columns}"
            )));
        }
        line_start = line_end + 1;
    }
    Ok(())
}

trait FixedWidth: Sized {
    fn from_le_bytes(bytes: &[u8]) -> Result<Self>;
}

macro_rules! fixed_width {
    ($($type:ty),*) => {$(
        impl FixedWidth for $type {
            fn from_le_bytes(bytes: &[u8]) -> Result<Self> {
                bytes.try_into().map(<$type>::from_le_bytes).map_err(|_| {
                    Error::invalid_input(format!("base56 field has {} bytes, expected {}", bytes.len(), size_of::<$type>()))
                })
            }
        }
    )*};
}

fixed_width!(i32, i64, f64);

impl FixedWidth for bool {
    fn from_le_bytes(bytes: &[u8]) -> Result<Self> {
        match bytes {
            [0] => Ok(false),
            [1] => Ok(true),
            _ => Err(Error::invalid_input(format!("invalid base56 BOOLEAN {bytes:?}"))),
        }
    }
}

fn decode_column<'f, T>(
    output: &mut DataChunkRef<'_>,
    index: usize,
    rows: usize,
    fields: impl Iterator<Item = &'f [u8]>,
    bytes: &mut Vec<u8>,
) -> Result<()>
where
    T: FixedWidth + NativeElement + WritableNativeElement<Write = T>,
{
    let mut vector = output.get_vector_at_mut::<T>(index)?;
    // SAFETY: `split_fields` gives every row one field per column, and the loop writes each.
    unsafe { vector.set_size_uninit(rows)? };
    let mut writer = vector.writer()?;
    for (row, field) in fields.enumerate() {
        let value = match field {
            [NULL_FIELD] => None,
            digits => {
                bytes.clear();
                decode_into(digits, bytes)?;
                Some(T::from_le_bytes(bytes)?)
            }
        };
        writer.set(row, value.as_ref())?;
    }
    Ok(())
}

fn decode_strings<'f>(
    output: &mut DataChunkRef<'_>,
    index: usize,
    rows: usize,
    fields: impl Iterator<Item = &'f [u8]>,
    bytes: &mut Vec<u8>,
) -> Result<()> {
    let mut vector = output.get_vector_at_mut::<String>(index)?;
    // SAFETY: `split_fields` gives every row one field per column, and the loop writes each.
    unsafe { vector.set_size_uninit(rows)? };
    let mut writer = vector.writer()?;
    for (row, field) in fields.enumerate() {
        if field == [NULL_FIELD] {
            writer.set(row, None)?;
            continue;
        }
        bytes.clear();
        decode_into(field, bytes)?;
        let text = std::str::from_utf8(bytes).map_err(|error| Error::invalid_input(error.to_string()))?;
        writer.set(row, Some(text))?;
    }
    Ok(())
}

/// Create `copy_input` with the configured NULL percentage for every supported type.
pub fn create_input(conn: &Connection, rows: usize) -> Result<()> {
    create_input_with_nulls(conn, rows, super::nulls::Nulls::from_env()?)
}

pub fn create_input_with_nulls(conn: &Connection, rows: usize, nulls: super::nulls::Nulls) -> Result<()> {
    conn.execute(
        format!(
            "CREATE OR REPLACE TABLE copy_input AS
             SELECT {} AS id, {}::DOUBLE AS x, {} AS name, {} AS flag, {} AS bucket
             FROM range({rows}) t(i)",
            nulls.expression("i::BIGINT", 20),
            nulls.expression("i * 1.5", 21),
            nulls.expression("'name_' || i", 22),
            nulls.expression("i % 2 = 0", 23),
            nulls.expression("(i % 1000)::INTEGER", 24),
        )
        .as_str(),
        Parameters::None,
    )?;
    Ok(())
}

/// Create an empty table named `name` with the columns of `copy_input`.
pub fn create_target(conn: &Connection, name: &str) -> Result<()> {
    conn.execute(
        format!("CREATE OR REPLACE TABLE {name} AS FROM copy_input LIMIT 0").as_str(),
        Parameters::None,
    )?;
    Ok(())
}

pub fn copy_to_sql(path: &str) -> String {
    format!("COPY copy_input TO '{}' (FORMAT base56)", path.replace('\'', "''"))
}

pub fn copy_from_sql(table: &str, path: &str) -> String {
    format!("COPY {table} FROM '{}' (FORMAT base56)", path.replace('\'', "''"))
}

/// Check the codec, then round-trip `copy_input` through a file at `path`.
pub fn verify(conn: &Connection, path: &str) -> Result<()> {
    let mut encoded = Vec::new();
    let mut decoded = Vec::new();
    for len in 0..=13 {
        for bytes in [
            vec![0; len],
            vec![u8::MAX; len],
            (0..len as u8).map(|b| b.wrapping_mul(97)).collect(),
        ] {
            encoded.clear();
            decoded.clear();
            encode_into(&bytes, &mut encoded);
            decode_into(&encoded, &mut decoded)?;
            super::ensure(decoded == bytes, || {
                format!("base56 round trip of {bytes:?} returned {decoded:?}")
            })?;
            super::ensure(encoded.iter().all(|digit| ALPHABET.contains(digit)), || {
                format!("base56 encoding {encoded:?} uses digits outside the alphabet")
            })?;
        }
    }
    for invalid in [&b"2"[..], b"2222", b"222220", b"zzzzzz", b"zz"] {
        super::ensure(decode_into(invalid, &mut decoded).is_err(), || {
            format!("base56 accepted {:?}", String::from_utf8_lossy(invalid))
        })?;
    }

    conn.execute(copy_to_sql(path).as_str(), Parameters::None)?;
    create_target(conn, "copy_verify")?;
    conn.execute(copy_from_sql("copy_verify", path).as_str(), Parameters::None)?;
    let differences = super::query_i64(
        conn,
        "SELECT count(*) FROM ((FROM copy_input EXCEPT ALL FROM copy_verify)
                               UNION ALL (FROM copy_verify EXCEPT ALL FROM copy_input))",
    )?;
    conn.execute("DROP TABLE copy_verify", Parameters::None)?;
    super::ensure(differences == 0, || {
        format!("base56 round trip changed {differences} rows")
    })?;

    conn.execute("CREATE OR REPLACE TABLE copy_mismatch (id BIGINT)", Parameters::None)?;
    let error = conn
        .execute(copy_from_sql("copy_mismatch", path).as_str(), Parameters::None)
        .expect_err("a header for other columns must be rejected");
    conn.execute("DROP TABLE copy_mismatch", Parameters::None)?;
    super::ensure(error.message.contains("does not match the target columns"), || {
        error.to_string()
    })?;

    let error = conn
        .execute(
            copy_to_sql(path).replace("copy_input", "(SELECT 1::TINYINT)").as_str(),
            Parameters::None,
        )
        .expect_err("TINYINT must be rejected");
    super::ensure(error.message.contains("does not support TINYINT"), || error.to_string())
}
