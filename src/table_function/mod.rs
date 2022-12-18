use super::ffi;

use num_derive::FromPrimitive;

mod function;

pub use function::{BindInfo, FunctionInfo, InitInfo, TableFunction};

/// Asserts that the given expression returns DuckDBSuccess, else panics and prints the expression
#[macro_export]
macro_rules! check {
    ($x:expr) => {{
        if ($x != $ffi::duckdb_state_DuckDBSuccess) {
            Err(format!("failed call: {}", stringify!($x)))?;
        }
    }};
}

/// Returns a `*const c_char` pointer to the given string
#[macro_export]
macro_rules! as_string {
    ($x:expr) => {
        std::ffi::CString::new($x).expect("c string").as_ptr().cast::<c_char>()
    };
}
pub(crate) use as_string;

use ffi::duckdb_malloc;
use std::mem::size_of;

/// # Safety
/// This function is obviously unsafe
pub unsafe fn malloc_struct<T>() -> *mut T {
    duckdb_malloc(size_of::<T>()).cast::<T>()
}

#[derive(Debug, Eq, PartialEq, FromPrimitive)]
pub enum LogicalTypeId {
    Boolean = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN as isize,
    Tinyint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TINYINT as isize,
    Smallint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT as isize,
    Integer = ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTEGER as isize,
    Bigint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BIGINT as isize,
    Utinyint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT as isize,
    Usmallint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT as isize,
    Uinteger = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER as isize,
    Ubigint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT as isize,
    Float = ffi::DUCKDB_TYPE_DUCKDB_TYPE_FLOAT as isize,
    Double = ffi::DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE as isize,
    Timestamp = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP as isize,
    Date = ffi::DUCKDB_TYPE_DUCKDB_TYPE_DATE as isize,
    Time = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIME as isize,
    Interval = ffi::DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL as isize,
    Hugeint = ffi::DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT as isize,
    Varchar = ffi::DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR as isize,
    Blob = ffi::DUCKDB_TYPE_DUCKDB_TYPE_BLOB as isize,
    Decimal = ffi::DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL as isize,
    TimestampS = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S as isize,
    TimestampMs = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS as isize,
    TimestampNs = ffi::DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS as isize,
    Enum = ffi::DUCKDB_TYPE_DUCKDB_TYPE_ENUM as isize,
    List = ffi::DUCKDB_TYPE_DUCKDB_TYPE_LIST as isize,
    Struct = ffi::DUCKDB_TYPE_DUCKDB_TYPE_STRUCT as isize,
    Map = ffi::DUCKDB_TYPE_DUCKDB_TYPE_MAP as isize,
    Uuid = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UUID as isize,
    Union = ffi::DUCKDB_TYPE_DUCKDB_TYPE_UNION as isize,
    Json = ffi::DUCKDB_TYPE_DUCKDB_TYPE_JSON as isize,
}

use ffi::{
    duckdb_create_list_type, duckdb_create_logical_type, duckdb_create_map_type, duckdb_destroy_logical_type,
    duckdb_get_type_id, duckdb_logical_type, idx_t,
};
use num_traits::FromPrimitive;
use std::collections::HashMap;
use std::ffi::{c_char, CString};
use std::ops::Deref;

/// Represents a logical type in the database - the underlying physical type can differ depending on the implementation
#[derive(Debug)]
pub struct LogicalType {
    pub(crate) typ: duckdb_logical_type,
}

impl LogicalType {
    pub fn new(typ: LogicalTypeId) -> Self {
        unsafe {
            Self {
                typ: duckdb_create_logical_type(typ as u32),
            }
        }
    }

    /// Creates a map type from its key type and value type.
    ///
    /// # Arguments
    /// * `type`: The key type and value type of map type to create.
    /// * `returns`: The logical type.
    pub fn new_map_type(key: &LogicalType, value: &LogicalType) -> Self {
        unsafe {
            Self {
                typ: duckdb_create_map_type(key.typ, value.typ),
            }
        }
    }

    /// Creates a list type from its child type.
    ///
    /// # Arguments
    /// * `type`: The child type of list type to create.
    /// * `returns`: The logical type.
    pub fn new_list_type(child_type: &LogicalType) -> Self {
        unsafe {
            Self {
                typ: duckdb_create_list_type(child_type.typ),
            }
        }
    }
    /// Make `LogicalType` for `struct`
    ///
    /// # Argument
    /// `shape` should be the fields and types in the `struct`
    // pub fn new_struct_type(shape: HashMap<&str, LogicalType>) -> Self {
    //     Self::make_meta_type(shape, duckdb_create_struct_type)
    // }

    /// Make `LogicalType` for `union`
    ///
    /// # Argument
    /// `shape` should be the variants in the `union`
    // pub fn new_union_type(shape: HashMap<&str, LogicalType>) -> Self {
    //     Self::make_meta_type(shape, duckdb_create_union)
    // }

    fn make_meta_type(
        shape: HashMap<&str, LogicalType>,
        x: unsafe extern "C" fn(
            nmembers: idx_t,
            names: *mut *const c_char,
            types: *const duckdb_logical_type,
        ) -> duckdb_logical_type,
    ) -> LogicalType {
        let keys: Vec<CString> = shape.keys().map(|it| CString::new(it.deref()).unwrap()).collect();
        let values: Vec<duckdb_logical_type> = shape.values().map(|it| it.typ).collect();
        let name_ptrs = keys.iter().map(|it| it.as_ptr()).collect::<Vec<*const c_char>>();

        unsafe {
            Self {
                typ: x(
                    shape.len().try_into().unwrap(),
                    name_ptrs.as_slice().as_ptr().cast_mut(),
                    values.as_slice().as_ptr(),
                ),
            }
        }
    }

    /// Retrieves the type class of a `duckdb_logical_type`.
    ///
    /// # Arguments
    /// * `returns`: The type id
    pub fn type_id(&self) -> LogicalTypeId {
        let id = unsafe { duckdb_get_type_id(self.typ) };

        FromPrimitive::from_u32(id).unwrap()
    }
}

impl Clone for LogicalType {
    fn clone(&self) -> Self {
        let type_id = self.type_id();

        Self::new(type_id)
    }
}

impl From<duckdb_logical_type> for LogicalType {
    fn from(ptr: duckdb_logical_type) -> Self {
        Self { typ: ptr }
    }
}

impl Drop for LogicalType {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_logical_type(&mut self.typ);
        }
    }
}

use ffi::{duckdb_destroy_value, duckdb_get_varchar, duckdb_value};

/// The Value object holds a single arbitrary value of any type that can be
/// stored in the database.
#[derive(Debug)]
pub struct Value(pub(crate) duckdb_value);

impl Value {
    /// Obtains a string representation of the given value
    pub fn get_varchar(&self) -> CString {
        unsafe { CString::from_raw(duckdb_get_varchar(self.0)) }
    }
}

impl From<duckdb_value> for Value {
    fn from(ptr: duckdb_value) -> Self {
        Self(ptr)
    }
}

impl Drop for Value {
    fn drop(&mut self) {
        unsafe {
            duckdb_destroy_value(&mut self.0);
        }
    }
}
use ffi::{
    duckdb_validity_row_is_valid, duckdb_validity_set_row_invalid, duckdb_validity_set_row_valid,
    duckdb_validity_set_row_validity, duckdb_vector, duckdb_vector_assign_string_element,
    duckdb_vector_assign_string_element_len, duckdb_vector_ensure_validity_writable, duckdb_vector_get_column_type,
    duckdb_vector_get_data, duckdb_vector_get_validity, duckdb_vector_size,
};
use std::fmt::Debug;
use std::{marker::PhantomData, slice};

/// Vector of values of a specified PhysicalType.
pub struct Vector<T>(duckdb_vector, PhantomData<T>);

impl<T> From<duckdb_vector> for Vector<T> {
    fn from(ptr: duckdb_vector) -> Self {
        Self(ptr, PhantomData {})
    }
}

impl<T> Vector<T> {
    /// Retrieves the data pointer of the vector.
    ///
    /// The data pointer can be used to read or write values from the vector. How to read or write values depends on the type of the vector.
    pub fn get_data(&self) -> *mut T {
        unsafe { duckdb_vector_get_data(self.0).cast() }
    }

    /// Assigns a string element in the vector at the specified location.
    ///
    /// # Arguments
    ///  * `index` - The row position in the vector to assign the string to
    ///  * `str` - The string
    ///  * `str_len` - The length of the string (in bytes)
    ///
    /// # Safety
    pub unsafe fn assign_string_element_len(&self, index: idx_t, str_: *const c_char, str_len: idx_t) {
        duckdb_vector_assign_string_element_len(self.0, index, str_, str_len);
    }

    /// Assigns a string element in the vector at the specified location.
    ///
    /// # Arguments
    ///  * `index` - The row position in the vector to assign the string to
    ///  * `str` - The null-terminated string"]
    ///
    /// # Safety
    pub unsafe fn assign_string_element(&self, index: idx_t, str_: *const c_char) {
        duckdb_vector_assign_string_element(self.0, index, str_);
    }

    /// Retrieves the data pointer of the vector as a slice
    ///
    /// The data pointer can be used to read or write values from the vector. How to read or write values depends on the type of the vector.
    pub fn get_data_as_slice(&mut self) -> &mut [T] {
        let ptr = self.get_data();
        unsafe { slice::from_raw_parts_mut(ptr, duckdb_vector_size() as usize) }
    }

    /// Retrieves the column type of the specified vector.
    pub fn get_column_type(&self) -> LogicalType {
        unsafe { LogicalType::from(duckdb_vector_get_column_type(self.0)) }
    }
    /// Retrieves the validity mask pointer of the specified vector.
    ///
    /// If all values are valid, this function MIGHT return NULL!
    ///
    /// The validity mask is a bitset that signifies null-ness within the data chunk. It is a series of uint64_t values, where each uint64_t value contains validity for 64 tuples. The bit is set to 1 if the value is valid (i.e. not NULL) or 0 if the value is invalid (i.e. NULL).
    ///
    /// Validity of a specific value can be obtained like this:
    ///
    /// idx_t entry_idx = row_idx / 64; idx_t idx_in_entry = row_idx % 64; bool is_valid = validity_maskentry_idx & (1 << idx_in_entry);
    ///
    /// Alternatively, the (slower) row_is_valid function can be used.
    ///
    /// returns: The pointer to the validity mask, or NULL if no validity mask is present
    pub fn get_validity(&self) -> ValidityMask {
        unsafe { ValidityMask(duckdb_vector_get_validity(self.0), duckdb_vector_size()) }
    }
    /// Ensures the validity mask is writable by allocating it.
    ///
    /// After this function is called, get_validity will ALWAYS return non-NULL. This allows null values to be written to the vector, regardless of whether a validity mask was present before.
    pub fn ensure_validity_writable(&self) {
        unsafe { duckdb_vector_ensure_validity_writable(self.0) };
    }
}

pub struct ValidityMask(*mut u64, idx_t);

impl Debug for ValidityMask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let base = (0..self.1)
            .map(|row| if self.row_is_valid(row) { "." } else { "X" })
            .collect::<Vec<&str>>()
            .join("");

        f.debug_struct("ValidityMask").field("validity", &base).finish()
    }
}

impl ValidityMask {
    /// Returns whether or not a row is valid (i.e. not NULL) in the given validity mask.
    ///
    /// # Arguments
    ///  * `row`: The row index
    /// returns: true if the row is valid, false otherwise
    pub fn row_is_valid(&self, row: idx_t) -> bool {
        unsafe { duckdb_validity_row_is_valid(self.0, row) }
    }
    /// In a validity mask, sets a specific row to either valid or invalid.
    ///
    /// Note that ensure_validity_writable should be called before calling get_validity, to ensure that there is a validity mask to write to.
    ///
    /// # Arguments
    ///  * `row`: The row index
    ///  * `valid`: Whether or not to set the row to valid, or invalid
    pub fn set_row_validity(&self, row: idx_t, valid: bool) {
        unsafe { duckdb_validity_set_row_validity(self.0, row, valid) }
    }
    /// In a validity mask, sets a specific row to invalid.
    ///
    /// Equivalent to set_row_validity with valid set to false.
    ///
    /// # Arguments
    ///  * `row`: The row index
    pub fn set_row_invalid(&self, row: idx_t) {
        unsafe { duckdb_validity_set_row_invalid(self.0, row) }
    }
    /// In a validity mask, sets a specific row to valid.
    ///
    /// Equivalent to set_row_validity with valid set to true.
    ///
    /// # Arguments
    ///  * `row`: The row index
    pub fn set_row_valid(&self, row: idx_t) {
        unsafe { duckdb_validity_set_row_valid(self.0, row) }
    }
}

use ffi::{
    duckdb_create_data_chunk, duckdb_data_chunk, duckdb_data_chunk_get_column_count, duckdb_data_chunk_get_size,
    duckdb_data_chunk_get_vector, duckdb_data_chunk_reset, duckdb_data_chunk_set_size, duckdb_destroy_data_chunk,
};

/// A Data Chunk represents a set of vectors.
///
/// The data chunk class is the intermediate representation used by the
/// execution engine of DuckDB. It effectively represents a subset of a relation.
/// It holds a set of vectors that all have the same length.
///
/// DataChunk is initialized using the DataChunk::Initialize function by
/// providing it with a vector of TypeIds for the Vector members. By default,
/// this function will also allocate a chunk of memory in the DataChunk for the
/// vectors and all the vectors will be referencing vectors to the data owned by
/// the chunk. The reason for this behavior is that the underlying vectors can
/// become referencing vectors to other chunks as well (i.e. in the case an
/// operator does not alter the data, such as a Filter operator which only adds a
/// selection vector).
///
/// In addition to holding the data of the vectors, the DataChunk also owns the
/// selection vector that underlying vectors can point to.
#[derive(Debug)]
pub struct DataChunk {
    ptr: duckdb_data_chunk,
    owned: bool,
}

impl DataChunk {
    /// Creates an empty DataChunk with the specified set of types.
    ///
    /// # Arguments
    /// - `types`: An array of types of the data chunk.
    pub fn new(types: Vec<LogicalType>) -> Self {
        let types: Vec<duckdb_logical_type> = types.iter().map(|x| x.typ).collect();
        let mut types = types.into_boxed_slice();

        let ptr = unsafe { duckdb_create_data_chunk(types.as_mut_ptr(), types.len().try_into().unwrap()) };

        Self { ptr, owned: true }
    }

    /// Retrieves the vector at the specified column index in the data chunk.
    ///
    /// The pointer to the vector is valid for as long as the chunk is alive.
    /// It does NOT need to be destroyed.
    ///
    pub fn get_vector<T>(&self, column_index: idx_t) -> Vector<T> {
        Vector::from(unsafe { duckdb_data_chunk_get_vector(self.ptr, column_index) })
    }
    /// Sets the current number of tuples in a data chunk.
    pub fn set_size(&self, size: idx_t) {
        unsafe { duckdb_data_chunk_set_size(self.ptr, size) };
    }
    /// Resets a data chunk, clearing the validity masks and setting the cardinality of the data chunk to 0.
    pub fn reset(&self) {
        unsafe { duckdb_data_chunk_reset(self.ptr) }
    }
    /// Retrieves the number of columns in a data chunk.
    pub fn get_column_count(&self) -> idx_t {
        unsafe { duckdb_data_chunk_get_column_count(self.ptr) }
    }
    /// Retrieves the current number of tuples in a data chunk.
    pub fn get_size(&self) -> idx_t {
        unsafe { duckdb_data_chunk_get_size(self.ptr) }
    }
}

impl From<duckdb_data_chunk> for DataChunk {
    fn from(ptr: duckdb_data_chunk) -> Self {
        Self { ptr, owned: false }
    }
}

impl Drop for DataChunk {
    fn drop(&mut self) {
        if self.owned {
            unsafe { duckdb_destroy_data_chunk(&mut self.ptr) };
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_data_chunk_construction() {
        let dc = DataChunk::new(vec![LogicalType::new(LogicalTypeId::Integer)]);

        assert_eq!(dc.get_column_count(), 1);

        drop(dc);
    }

    #[test]
    fn test_vector() {
        let datachunk = DataChunk::new(vec![LogicalType::new(LogicalTypeId::Bigint)]);
        let mut vector = datachunk.get_vector::<u64>(0);
        let data = vector.get_data_as_slice();

        data[0] = 42;
    }

    #[test]
    fn test_logi() {
        let key = LogicalType::new(LogicalTypeId::Varchar);

        let value = LogicalType::new(LogicalTypeId::Utinyint);

        let map = LogicalType::new_map_type(&key, &value);

        assert_eq!(map.type_id(), LogicalTypeId::Map);

        // let union_ = LogicalType::new_union_type(HashMap::from([
        //     ("number", LogicalType::new(LogicalTypeId::Bigint)),
        //     ("string", LogicalType::new(LogicalTypeId::Varchar)),
        // ]));
        // assert_eq!(union_.type_id(), LogicalTypeId::Union);

        // let struct_ = LogicalType::new_struct_type(HashMap::from([
        //     ("number", LogicalType::new(LogicalTypeId::Bigint)),
        //     ("string", LogicalType::new(LogicalTypeId::Varchar)),
        // ]));
        // assert_eq!(struct_.type_id(), LogicalTypeId::Struct);
    }

    use crate::{Connection, Result};
    use ffi::duckdb_free;
    use ffi::{duckdb_bind_info, duckdb_data_chunk, duckdb_function_info, duckdb_init_info};
    use malloc_struct;
    use std::error::Error;
    use std::ffi::CString;

    #[repr(C)]
    struct TestInitInfo {
        done: bool,
    }

    unsafe extern "C" fn func(info: duckdb_function_info, output: duckdb_data_chunk) {
        let info = FunctionInfo::from(info);
        let output = DataChunk::from(output);

        let init_info = info.get_init_data::<TestInitInfo>();

        if (*init_info).done {
            output.set_size(0);
        } else {
            (*init_info).done = true;

            let vector = output.get_vector::<&str>(0);

            let string = CString::new("Hello world").expect("unable to build string");
            vector.assign_string_element(0, string.as_ptr());

            output.set_size(1);
        }
    }

    unsafe extern "C" fn init(info: duckdb_init_info) {
        let info = InitInfo::from(info);

        let data = malloc_struct::<TestInitInfo>();

        (*data).done = false;

        info.set_init_data(data.cast(), Some(duckdb_free))
    }

    unsafe extern "C" fn bind(info: duckdb_bind_info) {
        let info = BindInfo::from(info);

        info.add_result_column("column0", LogicalType::new(LogicalTypeId::Varchar));

        let param = info.get_parameter(0).get_varchar();

        assert_eq!("hello.json", param.to_str().unwrap());
    }

    #[test]
    fn test_table_function() -> Result<(), Box<dyn Error>> {
        let conn = Connection::open_in_memory()?;

        let table_function = TableFunction::default();
        table_function
            .add_parameter(&LogicalType::new(LogicalTypeId::Json))
            .set_name("read_json")
            .supports_pushdown(false)
            .set_function(Some(func))
            .set_init(Some(init))
            .set_bind(Some(bind));
        conn.register_table_function(table_function)?;

        let val = conn.query_row("select * from read_json('hello.json')", [], |row| {
            <(String,)>::try_from(row)
        })?;
        assert_eq!(val, ("Hello world".to_string(),));
        Ok(())
    }
}
