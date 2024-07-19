use std::{any::Any, ffi::CString, slice};

use libduckdb_sys::{duckdb_array_type_array_size, duckdb_array_vector_get_child};

use super::LogicalType;
use crate::ffi::{
    duckdb_list_entry, duckdb_list_vector_get_child, duckdb_list_vector_get_size, duckdb_list_vector_reserve,
    duckdb_list_vector_set_size, duckdb_struct_type_child_count, duckdb_struct_type_child_name,
    duckdb_struct_vector_get_child, duckdb_validity_set_row_invalid, duckdb_vector,
    duckdb_vector_assign_string_element, duckdb_vector_assign_string_element_len,
    duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type, duckdb_vector_get_data,
    duckdb_vector_get_validity, duckdb_vector_size,
};

/// Vector trait.
pub trait Vector {
    /// Returns a reference to the underlying Any type that this trait object
    fn as_any(&self) -> &dyn Any;
    /// Returns a mutable reference to the underlying Any type that this trait object
    fn as_mut_any(&mut self) -> &mut dyn Any;
}

/// A flat vector
pub struct FlatVector {
    ptr: duckdb_vector,
    capacity: usize,
}

impl From<duckdb_vector> for FlatVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self {
            ptr,
            capacity: unsafe { duckdb_vector_size() as usize },
        }
    }
}

impl Vector for FlatVector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn Any {
        self
    }
}

impl FlatVector {
    fn with_capacity(ptr: duckdb_vector, capacity: usize) -> Self {
        Self { ptr, capacity }
    }

    /// Returns the capacity of the vector
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns an unsafe mutable pointer to the vector’s
    pub fn as_mut_ptr<T>(&self) -> *mut T {
        unsafe { duckdb_vector_get_data(self.ptr).cast() }
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
    pub fn logical_type(&self) -> LogicalType {
        LogicalType::from(unsafe { duckdb_vector_get_column_type(self.ptr) })
    }

    /// Set row as null
    pub fn set_null(&mut self, row: usize) {
        unsafe {
            duckdb_vector_ensure_validity_writable(self.ptr);
            let idx = duckdb_vector_get_validity(self.ptr);
            duckdb_validity_set_row_invalid(idx, row as u64);
        }
    }

    /// Copy data to the vector.
    pub fn copy<T: Copy>(&mut self, data: &[T]) {
        assert!(data.len() <= self.capacity());
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
            duckdb_vector_assign_string_element(self.ptr, index as u64, value.as_ptr());
        }
    }
}

impl Inserter<&str> for FlatVector {
    fn insert(&self, index: usize, value: &str) {
        let cstr = CString::new(value.as_bytes()).unwrap();
        unsafe {
            duckdb_vector_assign_string_element(self.ptr, index as u64, cstr.as_ptr());
        }
    }
}

impl Inserter<&[u8]> for FlatVector {
    fn insert(&self, index: usize, value: &[u8]) {
        let value_size = value.len();
        unsafe {
            // This function also works for binary data. https://duckdb.org/docs/api/c/api#duckdb_vector_assign_string_element_len
            duckdb_vector_assign_string_element_len(
                self.ptr,
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
    entries: FlatVector,
}

impl From<duckdb_vector> for ListVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self {
            entries: FlatVector::from(ptr),
        }
    }
}

impl ListVector {
    /// Returns the number of entries in the list vector.
    pub fn len(&self) -> usize {
        unsafe { duckdb_list_vector_get_size(self.entries.ptr) as usize }
    }

    /// Returns true if the list vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the child vector.
    // TODO: not ideal interface. Where should we keep capacity.
    pub fn child(&self, capacity: usize) -> FlatVector {
        self.reserve(capacity);
        FlatVector::with_capacity(unsafe { duckdb_list_vector_get_child(self.entries.ptr) }, capacity)
    }

    /// Set primitive data to the child node.
    pub fn set_child<T: Copy>(&self, data: &[T]) {
        self.child(data.len()).copy(data);
        self.set_len(data.len());
    }

    /// Set offset and length to the entry.
    pub fn set_entry(&mut self, idx: usize, offset: usize, length: usize) {
        self.entries.as_mut_slice::<duckdb_list_entry>()[idx].offset = offset as u64;
        self.entries.as_mut_slice::<duckdb_list_entry>()[idx].length = length as u64;
    }

    /// Reserve the capacity for its child node.
    fn reserve(&self, capacity: usize) {
        unsafe {
            duckdb_list_vector_reserve(self.entries.ptr, capacity as u64);
        }
    }

    /// Set the length of the list vector.
    pub fn set_len(&self, new_len: usize) {
        unsafe {
            duckdb_list_vector_set_size(self.entries.ptr, new_len as u64);
        }
    }
}

/// A array vector. (fixed-size list)
pub struct ArrayVector {
    /// ArrayVector does not own the vector pointer.
    ptr: duckdb_vector,
}

impl From<duckdb_vector> for ArrayVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self { ptr }
    }
}

impl ArrayVector {
    /// Get the logical type of this ArrayVector.
    pub fn logical_type(&self) -> LogicalType {
        LogicalType::from(unsafe { duckdb_vector_get_column_type(self.ptr) })
    }

    /// Returns the size of the array type.
    pub fn get_array_size(&self) -> u64 {
        let ty = self.logical_type();
        unsafe { duckdb_array_type_array_size(ty.ptr) as u64 }
    }

    /// Returns the child vector.
    /// capacity should be a multiple of the array size.
    // TODO: not ideal interface. Where should we keep count.
    pub fn child(&self, capacity: usize) -> FlatVector {
        FlatVector::with_capacity(unsafe { duckdb_array_vector_get_child(self.ptr) }, capacity)
    }

    /// Set primitive data to the child node.
    pub fn set_child<T: Copy>(&self, data: &[T]) {
        self.child(data.len()).copy(data);
    }
}

/// A struct vector.
pub struct StructVector {
    /// ListVector does not own the vector pointer.
    ptr: duckdb_vector,
}

impl From<duckdb_vector> for StructVector {
    fn from(ptr: duckdb_vector) -> Self {
        Self { ptr }
    }
}

impl StructVector {
    /// Returns the child by idx in the list vector.
    pub fn child(&self, idx: usize) -> FlatVector {
        FlatVector::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Take the child as [StructVector].
    pub fn struct_vector_child(&self, idx: usize) -> StructVector {
        Self::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Take the child as [ListVector].
    pub fn list_vector_child(&self, idx: usize) -> ListVector {
        ListVector::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Take the child as [ArrayVector].
    pub fn array_vector_child(&self, idx: usize) -> ArrayVector {
        ArrayVector::from(unsafe { duckdb_struct_vector_get_child(self.ptr, idx as u64) })
    }

    /// Get the logical type of this struct vector.
    pub fn logical_type(&self) -> LogicalType {
        LogicalType::from(unsafe { duckdb_vector_get_column_type(self.ptr) })
    }

    /// Get the name of the child by idx.
    pub fn child_name(&self, idx: usize) -> String {
        let logical_type = self.logical_type();
        unsafe {
            let child_name_ptr = duckdb_struct_type_child_name(logical_type.ptr, idx as u64);
            let c_str = CString::from_raw(child_name_ptr);
            let name = c_str.to_str().unwrap();
            // duckdb_free(child_name_ptr.cast());
            name.to_string()
        }
    }

    /// Get the number of children.
    pub fn num_children(&self) -> usize {
        let logical_type = self.logical_type();
        unsafe { duckdb_struct_type_child_count(logical_type.ptr) as usize }
    }
}
