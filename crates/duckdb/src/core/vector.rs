use std::{
    ffi::{c_char, CString},
    fmt::Debug,
    marker, slice,
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use derive_more::{Constructor, From};
use libduckdb_sys::{
    duckdb_array_vector_get_child, duckdb_union_type_member_name, duckdb_vector_assign_string_element_len, idx_t,
    DuckDbString,
};

use super::{column_info::ColumnInfo, DataChunkHandle, LogicalTypeHandle};
use crate::ffi::{
    duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_get_size, duckdb_list_vector_reserve,
    duckdb_list_vector_set_size, duckdb_struct_type_child_count, duckdb_struct_type_child_name,
    duckdb_struct_vector_get_child, duckdb_validity_set_row_invalid, duckdb_vector,
    duckdb_vector_assign_string_element, duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type,
    duckdb_vector_get_data, duckdb_vector_get_validity,
};

/// There is no way to get the length of the FlatVector directly,
/// As the C Api is not exposed making us do this ugly hack.
type GetVectorCapacity = dyn Fn() -> usize;

/// A handle to the vector created by data chunk
#[derive(Debug, Constructor, Clone)]
pub struct VectorHandle<T = ()> {
    ptr: duckdb_vector,
    _marker: marker::PhantomData<T>,
}

impl<T> VectorHandle<T> {
    pub unsafe fn new_unchecked(ptr: duckdb_vector) -> Self {
        Self {
            ptr,
            _marker: marker::PhantomData,
        }
    }

    pub const fn cast<Other>(&self) -> VectorHandle<Other> {
        VectorHandle {
            ptr: self.ptr,
            _marker: marker::PhantomData,
        }
    }

    pub const fn as_flat(&self) -> VectorHandle<FlatVector> {
        self.cast()
    }

    pub const fn as_list(&self) -> VectorHandle<ListVector> {
        self.cast()
    }

    pub const fn as_array(&self) -> VectorHandle<ArrayVector> {
        self.cast()
    }

    pub const fn as_struct(&self) -> VectorHandle<StructVector> {
        self.cast()
    }

    pub const fn as_union(&self) -> VectorHandle<UnionVector> {
        self.cast()
    }
}

impl VectorHandle<ListVector> {
    pub fn get_child(&self) -> VectorHandle {
        unsafe { VectorHandle::new_unchecked(duckdb_list_vector_get_child(self.ptr)) }
    }

    pub fn get_size(&self) -> usize {
        unsafe { duckdb_list_vector_get_size(self.ptr) as usize }
    }

    pub fn reserve_child_capacity(&mut self, capacity: usize) {
        unsafe {
            duckdb_list_vector_reserve(self.ptr, capacity as u64);
        }
    }

    /// Set the length of the list child vector.
    pub fn set_child_len(&mut self, new_len: usize) {
        unsafe {
            duckdb_list_vector_set_size(self.ptr, new_len as u64);
        }
    }
}

impl VectorHandle<ArrayVector> {
    pub fn get_child(&self) -> VectorHandle {
        unsafe { VectorHandle::new_unchecked(duckdb_array_vector_get_child(self.ptr)) }
    }
}

impl VectorHandle<StructVector> {
    pub fn get_child<'b>(&self, idx: usize) -> VectorHandle {
        unsafe { VectorHandle::new_unchecked(duckdb_struct_vector_get_child(self.ptr, idx as idx_t)) }
    }
}

impl VectorHandle<UnionVector> {
    pub fn get_child<'b>(&self, idx: usize) -> VectorHandle {
        unsafe { VectorHandle::new_unchecked(duckdb_struct_vector_get_child(self.ptr, idx as idx_t)) }
    }
}

/// A flat vector
#[derive(Constructor)]
pub struct FlatVector {
    handle: VectorHandle<Self>,
    /// get the size of elements for this vector.
    /// there is no good way to get the length just using the flat vector handle,
    /// hence we need to retrive it from higher above, either a list vector or directly a data chunk
    get_capacity: Arc<GetVectorCapacity>,
}

impl FlatVector {
    /// Returns the capacity of the vector
    pub fn capacity(&self) -> usize {
        (self.get_capacity)()
    }

    /// Returns an unsafe mutable pointer to the vector’s
    pub fn as_mut_ptr<T>(&self) -> *mut T {
        unsafe { duckdb_vector_get_data(self.handle.ptr.cast()).cast() }
    }

    /// Returns a slice of the vector
    pub fn as_slice<T>(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns a mutable slice of the vector
    pub fn as_mut_slice<T>(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.capacity()) }
    }

    /// Returns the logical type of the vector
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.handle.ptr)) }
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.handle.ptr);
            let idx = duckdb_vector_get_validity(self.handle.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    /// Copy data to the vector.
    pub fn copy<T: Copy>(&mut self, data: &[T]) {
        let capacity = self.capacity();
        assert!(
            data.len() <= capacity,
            "Provided data of length: {} exceeded capacity of {}",
            data.len(),
            capacity
        );
        self.as_mut_slice::<T>()[0..data.len()].copy_from_slice(data);
    }
}

/// A trait for inserting data into a vector.
pub trait Inserter<T> {
    /// Insert a value into the vector.
    fn insert(&self, index: usize, value: T);
}

impl Inserter<CString> for FlatVector {
    fn insert(&self, index: usize, value: CString) {
        unsafe {
            duckdb_vector_assign_string_element(self.handle.ptr, index as u64, value.as_ptr());
        }
    }
}

impl Inserter<&str> for FlatVector {
    fn insert(&self, index: usize, value: &str) {
        let bytes = value.as_bytes();
        unsafe {
            duckdb_vector_assign_string_element_len(
                self.handle.ptr,
                index as u64,
                bytes.as_ptr() as *const c_char,
                bytes.len() as u64,
            );
        }
    }
}

impl Inserter<&[u8]> for FlatVector {
    fn insert(&self, index: usize, value: &[u8]) {
        let value_size = value.len();
        unsafe {
            // This function also works for binary data. https://duckdb.org/docs/api/c/api#duckdb_vector_assign_string_element_len
            duckdb_vector_assign_string_element_len(
                self.handle.ptr,
                index as u64,
                value.as_ptr() as *const ::std::os::raw::c_char,
                value_size as u64,
            );
        }
    }
}

/// A list vector.
pub struct ListVector {
    /// ListVector does not own the vector pointer.
    pub entry_list: FlatVector,
    pub child: Box<Vector>,
}

impl ListVector {
    /// Returns true if the list vector is empty.
    pub fn is_empty(&self) -> bool {
        self.child_len() == 0
    }

    /// Set offset and length to the entry.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        self.entry_list.as_mut_slice::<duckdb_list_entry>()[idx] = duckdb_list_entry {
            offset: offset as u64,
            length: length as u64,
        }
    }

    /// Returns the number of entries in the list child vector.
    pub fn child_len(&self) -> usize {
        self.entry_list.handle.as_list().get_size()
    }

    /// Reserve the capacity for its child node.
    pub fn reserve_child_capacity(&mut self, capacity: usize) {
        self.entry_list.handle.as_list().reserve_child_capacity(capacity)
    }

    /// Set the length of the list child vector.
    pub fn set_child_len(&mut self, new_len: usize) {
        self.entry_list.handle.as_list().set_child_len(new_len)
    }
}

pub struct ArrayVector {
    pub child: Box<Vector>,
    array_size: usize, // number of columns
}

impl ArrayVector {
    pub fn array_size(&self) -> usize {
        self.array_size
    }
}

/// A struct vector.
pub struct StructVector {
    handle: VectorHandle<Self>,
    /// ListVector does not own the vector pointer.
    pub children: Vec<Vector>,
}

impl StructVector {
    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.handle.ptr)) }
    }

    /// Get the name of the child by idx.
    pub fn child_name(&self, idx: usize) -> DuckDbString {
        let logical_type = self.logical_type();
        unsafe { DuckDbString::from_ptr(duckdb_struct_type_child_name(logical_type.ptr, idx as u64)) }
    }

    /// Get the number of children.
    pub fn num_children(&self) -> usize {
        let logical_type = self.logical_type();
        unsafe { duckdb_struct_type_child_count(logical_type.ptr) as usize }
    }
}

/// A union vector
pub struct UnionVector {
    /// UnionVector does not own the vector pointer
    handle: VectorHandle<Self>,
    pub tags: FlatVector,
    pub members: Vec<Vector>,
}

impl UnionVector {
    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalTypeHandle {
        unsafe { LogicalTypeHandle::new(duckdb_vector_get_column_type(self.handle.ptr)) }
    }

    /// Retrieves the child type of the given union member at the specified index
    /// Get the name of the union member.
    pub fn member_name(&self, idx: usize) -> DuckDbString {
        let logical_type = self.logical_type();
        unsafe { DuckDbString::from_ptr(duckdb_union_type_member_name(logical_type.ptr, idx as u64)) }
    }
}

#[derive(From)]
pub enum Vector {
    Flat(FlatVector),
    List(ListVector),
    Array(ArrayVector),
    Struct(StructVector),
    Union(UnionVector),
}

impl Vector {
    pub fn try_as_flat_mut(&mut self) -> Result<&mut FlatVector, Box<dyn std::error::Error>> {
        match self {
            Vector::Flat(x) => Ok(x),
            _ => Err("Vector is not a FlatVector".into()),
        }
    }

    pub fn try_as_list_mut(&mut self) -> Result<&mut ListVector, Box<dyn std::error::Error>> {
        match self {
            Vector::List(x) => Ok(x),
            _ => Err("Vector is not a ListVector".into()),
        }
    }

    pub fn try_as_array_mut(&mut self) -> Result<&mut ArrayVector, Box<dyn std::error::Error>> {
        match self {
            Vector::Array(x) => Ok(x),
            _ => Err("Vector is not a ArrayVector".into()),
        }
    }

    pub fn try_as_struct_mut(&mut self) -> Result<&mut StructVector, Box<dyn std::error::Error>> {
        match self {
            Vector::Struct(x) => Ok(x),
            _ => Err("Vector is not a StructVector".into()),
        }
    }

    #[inline]
    pub fn try_as_union_mut(&mut self) -> Result<&mut UnionVector, Box<dyn std::error::Error>> {
        match self {
            Vector::Union(x) => Ok(x),
            _ => Err("Vector is not a UnionVector".into()),
        }
    }

    fn new(column_info: &ColumnInfo, base_handle: VectorHandle, get_capacity: Arc<GetVectorCapacity>) -> Self {
        match column_info {
            ColumnInfo::Flat => {
                let vector = FlatVector::new(base_handle.cast(), get_capacity);
                Vector::Flat(vector)
            }
            ColumnInfo::List(list) => {
                let list_handle = base_handle.clone().as_list();
                let child_handle = list_handle.get_child();
                let entry_list = FlatVector::new(base_handle.as_flat(), get_capacity);

                let size_fn = Arc::new(move || list_handle.get_size());

                let child = Box::new(Self::new(list.child.as_ref(), child_handle, size_fn));

                ListVector { entry_list, child }.into()
            }
            ColumnInfo::Array(info) => {
                let array_handle = base_handle.as_array();
                let child_handle = array_handle.get_child();
                let array_size = info.array_size;
                let get_array_size = Arc::new(move || array_size * get_capacity());
                let child = Box::new(Self::new(info.child.as_ref(), child_handle, get_array_size));

                let array_size = info.array_size;

                ArrayVector { child, array_size }.into()
            }
            ColumnInfo::Struct(info) => {
                let struct_handle = base_handle.as_struct();
                let children = info
                    .children
                    .iter()
                    .enumerate()
                    .map(|(idx, child)| {
                        let child_handle = struct_handle.get_child(idx);
                        Self::new(child, child_handle, get_capacity.clone())
                    })
                    .collect();

                StructVector {
                    handle: struct_handle,
                    children,
                }
                .into()
            }
            ColumnInfo::Union(info) => {
                let union_handle = base_handle.as_union();
                // first child = tag, idx is 0
                let tag_vector = union_handle.get_child(0).as_flat();
                let tags = FlatVector::new(tag_vector, get_capacity.clone());

                // members idx begin from 1
                let members = info
                    .members
                    .iter()
                    .enumerate()
                    .map(|(idx, child)| {
                        let child_handle = union_handle.get_child(idx + 1);
                        Self::new(child, child_handle.into(), get_capacity.clone())
                    })
                    .collect();

                UnionVector {
                    handle: union_handle,
                    tags,
                    members,
                }
                .into()
            }
        }
    }

    /// Create [Vector] lists from column infos and the data chunk.
    pub fn new_from_column_infos(columns: &[ColumnInfo], data_chunk_handle: Arc<DataChunkHandle>) -> Vec<Self> {
        let get_capacity = {
            let data_chunk_handle = data_chunk_handle.clone();
            Arc::new(move || data_chunk_handle.len().max(DataChunkHandle::max_capacity()))
        };
        (0..columns.len())
            .map(move |i| {
                let column_info = &columns[i];
                let base_handle = data_chunk_handle.get_vector(i);
                Self::new(column_info, base_handle, get_capacity.clone())
            })
            .collect()
    }

    /// Create [Vector] lists from Arrow schema.
    pub fn from_arrow_schema(schema: &SchemaRef, data_chunk_handle: Arc<DataChunkHandle>) -> Vec<Self> {
        let get_capacity = {
            let data_chunk_handle = data_chunk_handle.clone();
            Arc::new(move || data_chunk_handle.len().max(DataChunkHandle::max_capacity()))
        };

        schema
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let data_type = field.data_type();
                let column_info = ColumnInfo::new_from_data_type(data_type);
                let base_handle = data_chunk_handle.get_vector(i);
                Self::new(&column_info, base_handle, get_capacity.clone())
            })
            .collect()
    }
}
