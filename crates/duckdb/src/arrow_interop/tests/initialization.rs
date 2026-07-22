use super::super::{data_chunk_to_arrow, flat_vector_to_arrow_array};
use crate::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};

#[test]
fn flat_vector_arrow_read_is_bounded_by_committed_rows() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    chunk.flat_vector(0).set_null(0);
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };

    let vector = chunk.flat_vector(0);
    let error = flat_vector_to_arrow_array(&vector, 2).unwrap_err();
    assert_eq!(
        error.to_string(),
        "DuckDB vector length 2 exceeds initialized row count 1"
    );
}

#[test]
fn reserved_list_child_is_not_readable_past_its_committed_size() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeHandle::list(&LogicalTypeId::Integer.into())]);
    chunk.set_len(0);
    unsafe { chunk.assume_initialized() };

    let mut list = chunk.list_vector(0);
    let child = list.child(100);
    let error = flat_vector_to_arrow_array(&child, 100).unwrap_err();
    assert_eq!(
        error.to_string(),
        "DuckDB vector length 100 exceeds initialized row count 0"
    );
}

#[test]
fn growing_chunk_revokes_an_existing_flat_vector_read() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    chunk.flat_vector(0).set_null(0);
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };
    let vector = chunk.flat_vector(0);

    chunk.set_len(2);

    let error = flat_vector_to_arrow_array(&vector, 1).unwrap_err();
    assert_eq!(
        error.to_string(),
        "DuckDB payload is still under construction; finish writable views, then read through an initialized owner; chunk writers must call DataChunkHandle::assume_initialized"
    );
}

#[test]
fn chunk_arrow_read_joins_the_pointer_lease() {
    let mut chunk = DataChunkHandle::new(&[LogicalTypeId::Integer.into()]);
    chunk.flat_vector(0).set_null(0);
    chunk.set_len(1);
    unsafe { chunk.assume_initialized() };
    let _view = chunk.flat_vector(0);

    let error = data_chunk_to_arrow(&chunk).unwrap_err();
    assert_eq!(
        error.to_string(),
        "DuckDB vector already has a live view; drop it before requesting another"
    );
}
