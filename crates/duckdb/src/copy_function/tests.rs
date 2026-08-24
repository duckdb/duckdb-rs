use std::fs;

use crate::{
    Context, DuckDBType, Environment, Parameters, StorageLocation,
    column_data_collection::ColumnDataCollection,
    copy_function::{CopyFunctionBuilder, CopyFunctionCallbacks},
    data_chunk::DataChunk,
    file::{File, FileSystem},
    logical_type::LogicalTypeID,
};

struct RapidCopy {
    multiplier: f64,
}

impl CopyFunctionCallbacks for RapidCopy {
    type BindData = i32;
    type InitData = File;
    type BatchData = Vec<i32>;

    fn bind(
        &self,
        _context: Context,
        column_info: super::ColumnInfo,
    ) -> crate::Result<Self::BindData> {
        assert_eq!(column_info.len()?, 1);
        assert_eq!(column_info.get_column(0)?.0, "i");
        assert_eq!(
            column_info.get_column(0)?.1.type_id(),
            LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT
        );

        Ok(10)
    }

    fn init(
        &self,
        context: Context,
        _bind_data: &Self::BindData,
        file_path: &str,
    ) -> crate::Result<Self::InitData> {
        let fs = FileSystem::from_context(&context)?;

        let flags = libduckdb_sys::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_WRITE as u64
            | libduckdb_sys::DUCKDB_V2_FILE_FLAG::DUCKDB_V2_FILE_FLAG_CREATE as u64;
        let file = File::open(&fs, file_path, flags)?;

        Ok(file)
    }

    fn batch(
        &self,
        context: Context,
        _bind_data: &Self::BindData,
        _init_data: &Self::InitData,
        input: crate::column_data_collection::ColumnDataCollection,
    ) -> crate::Result<Self::BatchData> {
        let scanner = input.to_scan()?;
        let mut result = Vec::new();
        let to_append =
            ColumnDataCollection::from_context(&context, [i64::logical_type(&context)?])?
                .to_append()?;

        let data_chunk = DataChunk::create(&[i64::logical_type(&context)?], true)?;
        let mut vec = data_chunk.get_vector_at::<i64>(0)?;
        vec.set_size(1)?;
        vec.write(0, Some(10))?;

        to_append.append(&data_chunk)?;

        let mut scanner = scanner.to_append()?;
        scanner.combine(to_append.to_normal())?;

        let scanner = scanner.to_scan()?;

        for chunk in scanner {
            let chunk = chunk?;
            let in_vector = chunk.get_vector_at::<i64>(0)?;

            for item in in_vector.iter()?.flatten() {
                let mapped_value = ((*item) as f64 * self.multiplier) as i32;
                result.push(mapped_value);
            }
        }

        assert_eq!(result.len(), 10 + 1, "Expected 11 rows in batch data");

        Ok(result)
    }

    fn flush(
        &self,
        _context: Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
        batch_data: &Self::BatchData,
    ) -> crate::Result<()> {
        assert_eq!(batch_data.len(), 10 + 1, "Expected 11 rows in batch data");

        for &value in batch_data {
            let buffer = (value.to_string() + ",").into_bytes();
            let written = init_data.write(&buffer)?;

            assert_eq!(written, buffer.len(), "Failed to write all bytes to file");
        }

        init_data.sync()?;

        Ok(())
    }

    fn finalize(
        &self,
        _context: Context,
        _bind_data: &Self::BindData,
        init_data: &Self::InitData,
    ) -> crate::Result<()> {
        init_data.close()?;
        Ok(())
    }
}

#[test]
pub fn test_copy_function() -> crate::Result<()> {
    let env = Environment::new().expect("Failed to create environment");
    let db = env
        .open(StorageLocation::InMemory)
        .expect("Failed to open in-memory database");
    let conn = db.connect().expect("Failed to connect to database");

    CopyFunctionBuilder::new("rapidcopy", RapidCopy { multiplier: 1.5 })
        .register_with_connection(&conn)
        .expect("Failed to register copy function");

    conn.query(
        "COPY (SELECT i FROM range(0, $1) t(i)) TO 'out.txt' (FORMAT rapidcopy, USE_TMP_FILE FALSE)",
        Parameters::positional(&[&10]),
    )
    .expect("Failed to execute COPY query");

    let output = fs::read_to_string("out.txt").expect("Failed to read COPY output");
    assert_eq!(output, "0,1,3,4,6,7,9,10,12,13,15,");
    fs::remove_file("out.txt").expect("Failed to remove COPY output");

    Ok(())
}
