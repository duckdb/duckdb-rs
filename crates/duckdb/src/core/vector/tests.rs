use std::ffi::CString;

use crate::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use crate::ffi::duckdb_vector_size;

use super::list::MAX_VECTOR_SIZE;
use super::*;

#[test]
fn test_insert_string_values() {
    let chunk = DataChunkHandle::new(&[LogicalTypeId::Varchar.into()]);
    let vector = chunk.flat_vector(0);
    chunk.set_len(3);

    vector.insert(0, "first");
    vector.insert(1, &String::from("second"));
    let cstring = CString::new("third").unwrap();
    vector.insert(2, cstring);
}

#[test]
fn test_insert_byte_values() {
    let chunk = DataChunkHandle::new(&[LogicalTypeId::Blob.into()]);
    let vector = chunk.flat_vector(0);
    chunk.set_len(2);

    vector.insert(0, b"hello world".as_slice());
    vector.insert(
        1,
        &vec![0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64],
    );
}

#[test]
fn test_list_vector_get_entry() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    chunk.set_len(3);

    let mut list_vector = chunk.list_vector(0);

    list_vector.set_entry(0, 0, 2);
    list_vector.set_entry(1, 2, 1);
    list_vector.set_entry(2, 3, 2);

    assert_eq!(list_vector.get_entry(0), (0, 2));
    assert_eq!(list_vector.get_entry(1), (2, 1));
    assert_eq!(list_vector.get_entry(2), (3, 2));
}

#[test]
#[should_panic(expected = "exceeds vector capacity")]
fn test_flat_vector_row_is_null_checks_capacity() {
    let chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    let vector = chunk.flat_vector(0);

    vector.row_is_null(unsafe { duckdb_vector_size() });
}

#[test]
fn test_vector_try_row_is_null_rejects_row_beyond_capacity() {
    let row = unsafe { duckdb_vector_size() };
    let chunk = DataChunkHandle::new(&[
        LogicalTypeId::Integer.into(),
        LogicalTypeHandle::list(&LogicalTypeId::Integer.into()),
        LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2),
        LogicalTypeHandle::struct_type(&[("value", LogicalTypeId::Integer.into())]),
    ]);

    assert!(chunk.flat_vector(0).try_row_is_null(row).is_err());
    assert!(chunk.list_vector(1).try_row_is_null(row).is_err());
    assert!(chunk.array_vector(2).try_row_is_null(row).is_err());
    assert!(chunk.struct_vector(3).try_row_is_null(row).is_err());
}

#[test]
fn test_struct_vector_typed_child_accessors_smoke() {
    let int_type = LogicalTypeHandle::from(LogicalTypeId::Integer);
    let list_type = LogicalTypeHandle::list(&int_type);
    let array_type = LogicalTypeHandle::array(&int_type, 2);
    let nested_struct_type =
        LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Integer))]);
    let struct_type = LogicalTypeHandle::struct_type(&[
        ("items", list_type),
        ("pair", array_type),
        ("nested", nested_struct_type),
    ]);
    let chunk = DataChunkHandle::new(&[struct_type]);
    let vector = chunk.struct_vector(0);

    let list_child = vector.list_vector_child(0);
    assert!(list_child.is_empty());
    let array_child = vector.array_vector_child(1);
    assert_eq!(array_child.get_array_size(), 2);
    let struct_child = vector.struct_vector_child(2);
    assert_eq!(struct_child.num_children(), 1);
    assert_eq!(struct_child.child_name(0).to_str().unwrap(), "value");

    let flat_child = vector.child(1, 2);
    assert_eq!(flat_child.capacity(), 2);

    let child_count = unsafe { duckdb_vector_size() as usize } + 1;
    let struct_type = LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Integer))]);
    let list_type = LogicalTypeHandle::list(&struct_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);
    let child = list.struct_child(child_count);
    assert_eq!(child.num_children(), 1);
}

#[test]
#[should_panic(expected = "exceeds parent capacity")]
fn test_struct_vector_child_rejects_capacity_beyond_parent() {
    let struct_type = LogicalTypeHandle::struct_type(&[("value", LogicalTypeHandle::from(LogicalTypeId::Integer))]);
    let chunk = DataChunkHandle::new(&[struct_type]);
    let vector = chunk.struct_vector(0);
    let capacity = unsafe { duckdb_vector_size() as usize } + 1;

    vector.child(0, capacity);
}

#[test]
#[should_panic(expected = "exceeds backing capacity")]
fn test_array_vector_child_rejects_capacity_beyond_parent() {
    let array_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let chunk = DataChunkHandle::new(&[array_type]);
    let vector = chunk.array_vector(0);
    let capacity = (unsafe { duckdb_vector_size() as usize } + 1) * 2;

    vector.child(capacity);
}

#[test]
fn test_array_vector_set_child_uses_reserved_nested_capacity() -> Result<()> {
    let item_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let list_type = LogicalTypeHandle::list(&item_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);
    let child_count = unsafe { duckdb_vector_size() as usize } + 1;
    list.try_reserve(child_count)?;

    let array = list.array_child();
    let data = (0..(child_count * 2) as i32).collect::<Vec<_>>();
    unsafe { array.set_child(&data) };
    let child = array.child(data.len());
    let output = unsafe { child.as_slice_with_len::<i32>(data.len()) };

    assert_eq!(output[data.len() - 1], data[data.len() - 1]);

    Ok(())
}

#[test]
fn test_list_vector_set_len_reserves_list_child_storage() {
    let child_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let list_type = LogicalTypeHandle::list(&child_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);
    let child_count = unsafe { duckdb_vector_size() as usize } + 1;
    list.set_len(child_count);

    let mut child = list.list_child();
    child.set_entry(child_count - 1, 0, 0);

    assert_eq!(child.get_entry(child_count - 1), (0, 0));
    assert!(!child.row_is_null((child_count - 1) as u64));
}

#[test]
fn test_list_vector_set_len_reserves_array_child_storage() {
    let child_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let list_type = LogicalTypeHandle::list(&child_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);
    let child_count = unsafe { duckdb_vector_size() as usize } + 1;
    list.set_len(child_count);

    let child = list.array_child();

    assert!(!child.row_is_null((child_count - 1) as u64));
}

#[test]
fn test_list_vector_try_get_entry_rejects_row_beyond_capacity() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);
    let row = unsafe { duckdb_vector_size() as usize };

    let err = list.try_get_entry(row).unwrap_err();

    assert_eq!(
        err.to_string(),
        format!("list entry row {row} exceeds vector capacity {row}")
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn test_list_vector_try_set_len_rejects_oversized_capacity() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);
    let oversized = MAX_VECTOR_SIZE as usize + 1;

    let err = list.try_set_len(oversized).unwrap_err();

    assert_eq!(
        err.to_string(),
        format!(
            "cannot reserve {oversized} elements in DuckDB list vector: exceeds maximum vector size {MAX_VECTOR_SIZE}"
        )
    );
}

#[test]
fn test_list_vector_list_child_retains_default_capacity_before_len() {
    let child_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let list_type = LogicalTypeHandle::list(&child_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);

    let mut child = list.list_child();
    child.set_entry(0, 0, 0);

    assert_eq!(child.get_entry(0), (0, 0));
}

#[test]
fn test_list_vector_array_child_retains_default_capacity_before_len() {
    let child_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let list_type = LogicalTypeHandle::list(&child_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);

    let child = list.array_child();

    assert!(!child.row_is_null(0));
}
