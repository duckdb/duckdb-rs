use std::ffi::CString;

use super::*;
use crate::types::DuckString;
use crate::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    panic_utils::panic_payload,
};

pub(crate) unsafe fn set_list_entry_unchecked_for_test(
    chunk: &DataChunkHandle,
    column: usize,
    row: usize,
    offset: usize,
    length: usize,
) {
    let vector = unsafe { crate::ffi::duckdb_data_chunk_get_vector(chunk.get_ptr(), column as u64) };
    let entries = unsafe { crate::ffi::duckdb_vector_get_data(vector) }.cast::<crate::ffi::duckdb_list_entry>();
    unsafe {
        entries.add(row).write(crate::ffi::duckdb_list_entry {
            offset: offset as u64,
            length: length as u64,
        })
    };
}

#[test]
fn vector_wrappers_remain_ref_unwind_safe() {
    fn assert_ref_unwind_safe<T: std::panic::RefUnwindSafe>() {}

    assert_ref_unwind_safe::<FlatVector<'static>>();
    assert_ref_unwind_safe::<ListVector<'static>>();
    assert_ref_unwind_safe::<StructVector<'static>>();
    assert_ref_unwind_safe::<ArrayVector<'static>>();
}

#[test]
fn inserts_string_and_binary_values() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Varchar.into(), LogicalTypeId::Blob.into()]);
    chunk.set_len(3);

    {
        let mut vector = chunk.flat_vector(0);
        vector.insert(0, "first");
        vector.insert(1, &String::from("second"));
        vector.insert(2, CString::new("third").unwrap());
    }
    {
        let mut vector = chunk.flat_vector(1);
        vector.insert(0, b"hello".as_slice());
        vector.insert(1, &b"world".to_vec());
        vector.insert(2, b"!".as_slice());
    }
    unsafe { chunk.assume_initialized() };

    let strings = chunk.flat_vector(0);
    let strings = unsafe { strings.as_slice_with_len::<crate::ffi::duckdb_string_t>(3) };
    let observed = strings
        .iter()
        .map(|value| DuckString::new(&mut { *value }).as_str().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(observed, ["first", "second", "third"]);

    let blobs = chunk.flat_vector(1);
    let blobs = unsafe { blobs.as_slice_with_len::<crate::ffi::duckdb_string_t>(3) };
    let observed = blobs
        .iter()
        .map(|value| DuckString::new(&mut { *value }).as_bytes().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(observed, [b"hello".as_slice(), b"world".as_slice(), b"!".as_slice()]);
}

#[test]
fn list_entries_are_checked_once_by_native_vector() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);
    unsafe { list.set_child(&[0_i32; 5]) };
    list.set_entry(0, 3, 2);
    drop(list);
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };
    let list = ListVector::from_vector(chunk.initialized_vector(0, 1).unwrap()).unwrap();
    assert_eq!(list.try_get_entry(0).unwrap(), (3, 2));
    assert_eq!(
        list.try_get_entry(1).unwrap_err().to_string(),
        "list entry row 1 exceeds initialized vector length 1"
    );
}

#[test]
fn newly_allocated_chunk_payload_is_not_safely_readable() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    let list = chunk.list_vector(0);

    assert_eq!(
        list.try_get_entry(0).unwrap_err().to_string(),
        "DuckDB payload is still under construction; finish the writable view and call DataChunkHandle::assume_initialized before reading"
    );
}

#[test]
fn list_entries_cannot_exceed_reserved_child_storage() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);

    assert_eq!(
        list.try_set_entry(0, 0, 1).unwrap_err().to_string(),
        "list entry at row 0 ends at 1, beyond reserved or committed child capacity 0"
    );

    list.try_reserve(2).unwrap();
    list.set_entry(0, 1, 1);

    assert_eq!(
        list.try_set_entry(0, 1, 2).unwrap_err().to_string(),
        "list entry at row 0 ends at 3, beyond reserved or committed child capacity 2"
    );

    assert_eq!(
        list.try_set_entry(0, usize::MAX, 1).unwrap_err().to_string(),
        "DuckDB list entry at row 0 overflows usize"
    );
}

#[test]
fn dropped_list_reservations_are_not_reused_by_a_new_view() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);

    chunk.list_vector(0).try_reserve(2).unwrap();

    let mut list = chunk.list_vector(0);
    assert_eq!(
        list.try_set_entry(0, 0, 1).unwrap_err().to_string(),
        "list entry at row 0 ends at 1, beyond reserved or committed child capacity 0"
    );
}

#[test]
fn native_list_ranges_handle_sparse_null_and_malformed_entries() -> Result<()> {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = DataChunkHandle::new(&[list_type]);
    chunk.set_len(5);
    {
        let mut list = chunk.list_vector(0);
        list.try_set_len(8)?;
        unsafe { list.child(8).copy(&[0_i32; 8]) };

        list.set_entry(0, 5, 2);
        list.set_null(1);
        unsafe {
            set_list_entry_unchecked_for_test(&chunk, 0, 2, usize::MAX, 1);
            set_list_entry_unchecked_for_test(&chunk, 0, 3, 7, 2);
        }
        list.set_entry(4, usize::MAX, 0);
    }
    unsafe { chunk.assume_initialized() };
    let list = ListVector::from_vector(chunk.initialized_vector(0, 5)?)?;

    assert_eq!(list.try_get_range(0)?, Some(5..7));
    assert_eq!(list.try_get_range(1)?, None);
    assert_eq!(
        list.try_get_range(2).unwrap_err().to_string(),
        "DuckDB list entry at row 2 overflows usize"
    );
    assert_eq!(
        list.try_get_range(3).unwrap_err().to_string(),
        "DuckDB list entry at row 3 ends at 9, beyond child length 8"
    );
    assert_eq!(list.try_get_range(4)?, Some(0..0));
    Ok(())
}

#[test]
fn null_list_entries_do_not_expose_payload_bytes() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = DataChunkHandle::new(&[list_type]);
    {
        let mut list = chunk.list_vector(0);
        list.set_null(0);
        list.set_entry(0, 7, 3);
    }
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };
    let list = ListVector::from_vector(chunk.initialized_vector(0, 1).unwrap()).unwrap();

    assert_eq!(
        list.try_get_entry(0).unwrap_err().to_string(),
        "DuckDB list entry at row 0 is null"
    );
}

#[test]
fn validity_bounds_are_shared_by_all_family_adapters() {
    let row = unsafe { crate::ffi::duckdb_vector_size() };
    let chunk = DataChunkHandle::new(&[
        LogicalTypeId::Integer.into(),
        LogicalTypeHandle::list(&LogicalTypeId::Integer.into()),
        LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2),
        LogicalTypeHandle::struct_type(&[("value", LogicalTypeId::Integer.into())]),
    ]);

    let expected = format!("row index {row} exceeds vector capacity {}", chunk.capacity());
    assert_eq!(
        chunk.flat_vector(0).try_row_is_null(row).unwrap_err().to_string(),
        expected
    );
    assert_eq!(
        chunk.list_vector(1).try_row_is_null(row).unwrap_err().to_string(),
        expected
    );
    assert_eq!(
        chunk.array_vector(2).try_row_is_null(row).unwrap_err().to_string(),
        expected
    );
    assert_eq!(
        chunk.struct_vector(3).try_row_is_null(row).unwrap_err().to_string(),
        expected
    );
}

#[test]
fn family_constructors_enforce_their_logical_type() -> Result<()> {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    unsafe { chunk.assume_initialized() };

    let list_error = ListVector::from_vector(chunk.initialized_vector(0, 0)?).err().unwrap();
    assert_eq!(list_error.to_string(), "expected list or map vector, got Integer");

    let array_error = ArrayVector::from_vector(chunk.initialized_vector(0, 0)?).err().unwrap();
    assert_eq!(array_error.to_string(), "expected array vector, got Integer");

    let struct_error = StructVector::from_vector(chunk.initialized_vector(0, 0)?)
        .err()
        .unwrap();
    assert_eq!(struct_error.to_string(), "expected struct vector, got Integer");
    Ok(())
}

#[test]
fn struct_children_inherit_explicit_parent_capacity() {
    let int_type = LogicalTypeHandle::from(LogicalTypeId::Integer);
    let struct_type = LogicalTypeHandle::struct_type(&[
        ("items", LogicalTypeHandle::list(&int_type)),
        ("pair", LogicalTypeHandle::array(&int_type, 2)),
        (
            "nested",
            LogicalTypeHandle::struct_type(&[("value", LogicalTypeId::Integer.into())]),
        ),
    ]);
    let chunk = DataChunkHandle::new(&[struct_type]);
    let mut vector = chunk.struct_vector(0);
    let capacity = vector.vector.capacity();

    let list = vector.list_vector_child(0);
    assert_eq!(list.vector.capacity(), capacity);
    assert!(list.is_empty());
    drop(list);
    let array = vector.array_vector_child(1);
    assert_eq!(array.vector.capacity(), capacity);
    assert_eq!(array.get_array_size(), 2);
    drop(array);
    let nested = vector.struct_vector_child(2);
    assert_eq!(nested.vector.capacity(), capacity);
    assert_eq!(nested.num_children(), 1);
    assert_eq!(nested.child_name(0).to_str().unwrap(), "value");
}

#[test]
fn list_children_use_reserved_capacity_without_standard_floor() -> Result<()> {
    let child_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let list_type = LogicalTypeHandle::list(&child_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);

    assert_eq!(
        list.list_child().try_row_is_null(0).unwrap_err().to_string(),
        "row index 0 exceeds vector capacity 0"
    );

    let child_count = unsafe { crate::ffi::duckdb_vector_size() as usize } + 1;
    list.try_set_len(child_count)?;
    let mut child = list.list_child();
    child.set_entry(child_count - 1, 0, 0);
    assert!(!child.try_row_is_null((child_count - 1) as u64)?);
    Ok(())
}

#[test]
fn nested_array_capacity_is_checked_and_can_exceed_standard_vector_size() -> Result<()> {
    let item_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let list_type = LogicalTypeHandle::list(&item_type);
    let mut chunk = DataChunkHandle::new(&[list_type]);
    let child_count = unsafe { crate::ffi::duckdb_vector_size() as usize } + 1;
    let mut list = chunk.list_vector(0);
    list.try_reserve(child_count)?;

    let mut array = list.array_child();
    let data = (0..(child_count * 2) as i32).collect::<Vec<_>>();
    unsafe { array.set_child(&data) };
    drop(array);
    list.set_entry(0, 0, child_count);
    list.try_set_len(child_count)?;
    drop(list);
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };

    let list = ListVector::from_vector(chunk.initialized_vector(0, 1)?)?;
    let array = ArrayVector::from_vector(list.read_child()?)?;
    let child = array.read_child()?;
    let output = unsafe { child.as_slice::<i32>(data.len())? };
    assert_eq!(output, data);
    Ok(())
}

#[test]
fn array_and_struct_child_capacity_reject_overreach() {
    let array_chunk = DataChunkHandle::new(&[LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2)]);
    let mut array = array_chunk.array_vector(0);
    let alignment_error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        array.child(1);
    }))
    .unwrap_err();
    assert_eq!(
        panic_payload(alignment_error.as_ref()),
        "array child capacity must be a multiple of the fixed array size"
    );

    let array_capacity = (unsafe { crate::ffi::duckdb_vector_size() as usize } + 1) * 2;
    let array_error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        array.child(array_capacity);
    }))
    .unwrap_err();
    assert!(panic_payload(array_error.as_ref()).contains("exceeds backing capacity"));

    let struct_chunk = DataChunkHandle::new(&[LogicalTypeHandle::struct_type(&[(
        "value",
        LogicalTypeId::Integer.into(),
    )])]);
    let mut vector = struct_chunk.struct_vector(0);
    let struct_capacity = unsafe { crate::ffi::duckdb_vector_size() as usize } + 1;
    let struct_error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        vector.child(0, struct_capacity);
    }))
    .unwrap_err();
    assert!(panic_payload(struct_error.as_ref()).contains("exceeds parent capacity"));
}

#[test]
fn fresh_nested_array_capacity_error_explains_the_reservation_order() {
    let array_type = LogicalTypeHandle::array(&LogicalTypeId::Integer.into(), 2);
    let list_type = LogicalTypeHandle::list(&array_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);
    let mut array = list.array_child();

    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        array.child(2);
    }))
    .unwrap_err();

    assert_eq!(
        panic_payload(error.as_ref()),
        "array child capacity 2 exceeds backing capacity 0; reserve or commit the containing list child before writing nested values"
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn oversized_list_reservation_is_rejected_before_ffi() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);
    let oversized = MAX_VECTOR_SIZE as usize + 1;

    assert_eq!(
        list.try_set_len(oversized).unwrap_err().to_string(),
        format!(
            "cannot reserve {oversized} elements in DuckDB list vector: exceeds maximum vector size {MAX_VECTOR_SIZE}"
        )
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn oversized_list_physical_buffer_is_rejected_before_ffi() {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Bigint.into());
    let chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);
    let oversized = MAX_VECTOR_SIZE as usize / std::mem::size_of::<i64>() + 1;
    let resized_capacity = oversized.next_power_of_two();
    let target_bytes = resized_capacity as u128 * std::mem::size_of::<i64>() as u128;

    assert_eq!(
        list.try_reserve(oversized).unwrap_err().to_string(),
        format!(
            "cannot reserve {oversized} elements in DuckDB list vector: rounded capacity {resized_capacity} requires a {target_bytes}-byte physical buffer, exceeding maximum vector size {MAX_VECTOR_SIZE}"
        )
    );
}

#[test]
#[cfg(target_pointer_width = "64")]
fn oversized_array_multiplier_is_rejected_before_ffi() {
    let child_type = LogicalTypeHandle::array(&LogicalTypeId::UTinyint.into(), 32);
    let list_type = LogicalTypeHandle::list(&child_type);
    let chunk = DataChunkHandle::new(&[list_type]);
    let mut list = chunk.list_vector(0);
    let oversized = MAX_VECTOR_SIZE as usize / 32 + 1;
    let resized_capacity = oversized.next_power_of_two();
    let target_bytes = resized_capacity as u128 * 32;

    assert_eq!(
        list.try_reserve(oversized).unwrap_err().to_string(),
        format!(
            "cannot reserve {oversized} elements in DuckDB list vector: rounded capacity {resized_capacity} requires a {target_bytes}-byte physical buffer, exceeding maximum vector size {MAX_VECTOR_SIZE}"
        )
    );
}

#[test]
fn unsupported_resize_type_is_rejected() {
    let logical_type = LogicalTypeHandle::from(LogicalTypeId::Any);

    assert_eq!(
        max_resize_data_width(&logical_type).unwrap_err().to_string(),
        "cannot preflight DuckDB vector resize for logical type Any"
    );
}

#[test]
fn writable_payload_stays_unreadable_after_non_null_commit() -> Result<()> {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = DataChunkHandle::new(&[list_type]);
    let mut writable = chunk.writable_vector(0, 1)?;
    assert!(!writable.initialized_for_test());

    let mut list = writable.list_vector();
    list.try_set_len(1)?;
    list.set_entry(0, 0, 1);

    assert_eq!(
        list.try_get_entry(0).unwrap_err().to_string(),
        "DuckDB payload is still under construction; finish the writable view and call DataChunkHandle::assume_initialized before reading"
    );
    assert_eq!(
        list.read_child().err().unwrap().to_string(),
        "DuckDB payload is still under construction; finish the writable view and call DataChunkHandle::assume_initialized before reading"
    );
    Ok(())
}

#[test]
fn growing_initialized_list_child_revokes_read_access() -> Result<()> {
    let list_type = LogicalTypeHandle::list(&LogicalTypeId::Integer.into());
    let mut chunk = DataChunkHandle::new(&[list_type]);
    chunk.set_len(0);
    unsafe { chunk.assume_initialized() };

    let mut list = chunk.list_vector(0);
    assert_eq!(list.read_child()?.capacity(), 0);
    list.try_set_len(1)?;

    assert_eq!(
        list.read_child().err().unwrap().to_string(),
        "DuckDB payload is still under construction; finish the writable view and call DataChunkHandle::assume_initialized before reading"
    );
    Ok(())
}

#[test]
#[cfg(feature = "vtab")]
fn trusted_input_view_exposes_initialized_payload() -> Result<()> {
    let mut owner = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    {
        let mut vector = owner.flat_vector(0);
        unsafe { vector.copy(&[42_i32]) };
    }

    owner.set_len(1);
    unsafe { owner.assume_initialized() };
    let input = unsafe { DataChunkHandle::new_unowned_input(owner.get_ptr()) };
    let vector = input.flat_vector(0);
    assert_eq!(unsafe { vector.as_slice_with_len::<i32>(1) }, &[42]);
    Ok(())
}

#[test]
fn top_level_writable_capacity_is_checked() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    let capacity = unsafe { crate::ffi::duckdb_vector_size() as usize };
    let error = chunk.writable_vector(0, capacity + 1).err().unwrap();

    assert_eq!(
        error.to_string(),
        format!(
            "top-level vector capacity {} exceeds data chunk capacity {capacity}",
            capacity + 1
        )
    );
}

#[test]
fn top_level_initialized_capacity_is_checked() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    unsafe { chunk.assume_initialized() };
    let capacity = unsafe { crate::ffi::duckdb_vector_size() as usize };
    let error = chunk.initialized_vector(0, capacity + 1).err().unwrap();

    assert_eq!(
        error.to_string(),
        format!(
            "top-level vector capacity {} exceeds data chunk capacity {capacity}",
            capacity + 1
        )
    );
}

#[test]
fn union_vectors_are_not_adapted_as_structs() {
    let union_type = LogicalTypeHandle::union_type(&[
        ("number", LogicalTypeId::Integer.into()),
        ("text", LogicalTypeId::Varchar.into()),
    ]);
    let chunk = DataChunkHandle::new(&[union_type]);

    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        chunk.struct_vector(0);
    }))
    .unwrap_err();
    assert!(panic_payload(error.as_ref()).contains("expected struct vector, got Union"));
}
