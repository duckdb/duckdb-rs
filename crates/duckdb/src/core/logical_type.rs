use std::{
    ffi::{c_char, CString},
    fmt::Debug,
};

use derive_more::{Constructor, From};
use strum::EnumTryAs;

use crate::ffi::*;

/// Logical Type Id
/// <https://duckdb.org/docs/api/c/types>
#[repr(u32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalTypeId {
    /// Boolean
    Boolean = DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN,
    /// Tinyint
    Tinyint = DUCKDB_TYPE_DUCKDB_TYPE_TINYINT,
    /// Smallint
    Smallint = DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT,
    /// Integer
    Integer = DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    /// Bigint
    Bigint = DUCKDB_TYPE_DUCKDB_TYPE_BIGINT,
    /// Unsigned Tinyint
    UTinyint = DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT,
    /// Unsigned Smallint
    USmallint = DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
    /// Unsigned Integer
    UInteger = DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER,
    /// Unsigned Bigint
    UBigint = DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    /// Float
    Float = DUCKDB_TYPE_DUCKDB_TYPE_FLOAT,
    /// Double
    Double = DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    /// Timestamp
    Timestamp = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP,
    /// Date
    Date = DUCKDB_TYPE_DUCKDB_TYPE_DATE,
    /// Time
    Time = DUCKDB_TYPE_DUCKDB_TYPE_TIME,
    /// Interval
    Interval = DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL,
    /// Hugeint
    Hugeint = DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT,
    /// Varchar
    Varchar = DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
    /// Blob
    Blob = DUCKDB_TYPE_DUCKDB_TYPE_BLOB,
    /// Decimal
    Decimal = DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL,
    /// Timestamp S
    TimestampS = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
    /// Timestamp MS
    TimestampMs = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
    /// Timestamp NS
    TimestampNs = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS,
    /// Enum
    Enum = DUCKDB_TYPE_DUCKDB_TYPE_ENUM,
    /// List
    List = DUCKDB_TYPE_DUCKDB_TYPE_LIST,
    /// Struct
    Struct = DUCKDB_TYPE_DUCKDB_TYPE_STRUCT,
    /// Map
    Map = DUCKDB_TYPE_DUCKDB_TYPE_MAP,
    /// Uuid
    Uuid = DUCKDB_TYPE_DUCKDB_TYPE_UUID,
    /// Union
    Union = DUCKDB_TYPE_DUCKDB_TYPE_UNION,
    /// Array
    Array = DUCKDB_TYPE_DUCKDB_TYPE_ARRAY,
    /// Timestamp TZ
    TimestampTZ = DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ,
}

impl From<u32> for LogicalTypeId {
    /// Convert from u32 to LogicalTypeId
    fn from(value: u32) -> Self {
        match value {
            DUCKDB_TYPE_DUCKDB_TYPE_BOOLEAN => Self::Boolean,
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => Self::Tinyint,
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => Self::Smallint,
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => Self::Integer,
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => Self::Bigint,
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => Self::UTinyint,
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => Self::USmallint,
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => Self::UInteger,
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => Self::UBigint,
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => Self::Float,
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => Self::Double,
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => Self::Varchar,
            DUCKDB_TYPE_DUCKDB_TYPE_BLOB => Self::Blob,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP => Self::Timestamp,
            DUCKDB_TYPE_DUCKDB_TYPE_DATE => Self::Date,
            DUCKDB_TYPE_DUCKDB_TYPE_TIME => Self::Time,
            DUCKDB_TYPE_DUCKDB_TYPE_INTERVAL => Self::Interval,
            DUCKDB_TYPE_DUCKDB_TYPE_HUGEINT => Self::Hugeint,
            DUCKDB_TYPE_DUCKDB_TYPE_DECIMAL => Self::Decimal,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => Self::TimestampS,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS => Self::TimestampMs,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS => Self::TimestampNs,
            DUCKDB_TYPE_DUCKDB_TYPE_ENUM => Self::Enum,
            DUCKDB_TYPE_DUCKDB_TYPE_LIST => Self::List,
            DUCKDB_TYPE_DUCKDB_TYPE_ARRAY => Self::Array,
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => Self::Struct,
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => Self::Map,
            DUCKDB_TYPE_DUCKDB_TYPE_UUID => Self::Uuid,
            DUCKDB_TYPE_DUCKDB_TYPE_UNION => Self::Union,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => Self::TimestampTZ,
            _ => unimplemented!("{value} not implemented"),
        }
    }
}

#[derive(Debug)]
pub struct LogicalTypeHandle {
    pub(crate) ptr: duckdb_logical_type,
}

// impl Debug for LogicalTypeHandle {
//   /// Debug implementation for LogicalType
//   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
//     let id = self.id();
//     match id {
//       LogicalTypeId::Struct => {
//         write!(f, "struct<")?;
//         for i in 0..self.num_children() {
//           if i > 0 {
//             write!(f, ", ")?;
//           }
//           write!(f, "{}: {:?}", self.child_name(i), self.struct_or_union_child(i))?;
//         }
//         write!(f, ">")
//       }
//       _ => write!(f, "{:?}", self.id()),
//     }
//   }
// }

impl Drop for LogicalTypeHandle {
    /// Drop implementation for LogicalType
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe {
                duckdb_destroy_logical_type(&mut self.ptr);
            }
        }

        self.ptr = std::ptr::null_mut();
    }
}

impl From<LogicalTypeId> for LogicalTypeHandle {
    /// Create a new [LogicalTypeHandle] from [LogicalTypeId]
    fn from(id: LogicalTypeId) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_logical_type(id as u32),
            }
        }
    }
}

// constructors
impl LogicalTypeHandle {
    /// Create a DuckDB logical type from C API
    pub(crate) unsafe fn new(ptr: duckdb_logical_type) -> Self {
        Self { ptr }
    }

    /// Creates a map type from its child type.
    pub fn map(key: &LogicalTypeHandle, value: &LogicalTypeHandle) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_map_type(key.ptr, value.ptr),
            }
        }
    }

    /// Creates a list type from its child type.
    pub fn list(child_type: &LogicalTypeHandle) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_list_type(child_type.ptr),
            }
        }
    }

    /// Creates a array type from its child type.
    pub fn array(child_type: &LogicalTypeHandle, size: i32) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_array_type(child_type.ptr, size as idx_t),
            }
        }
    }

    /// Creates a decimal type from its `width` and `scale`.
    pub fn decimal(width: u8, scale: u8) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_decimal_type(width, scale),
            }
        }
    }

    /// Make a `LogicalType` for `struct`
    pub fn struct_type(fields: &[(&str, LogicalTypeHandle)]) -> Self {
        let keys: Vec<CString> = fields.iter().map(|f| CString::new(f.0).unwrap()).collect();
        let values: Vec<duckdb_logical_type> = fields.iter().map(|it| it.1.ptr).collect();
        let name_ptrs = keys.iter().map(|it| it.as_ptr()).collect::<Vec<*const c_char>>();

        unsafe {
            Self {
                ptr: duckdb_create_struct_type(
                    values.as_slice().as_ptr().cast_mut(),
                    name_ptrs.as_slice().as_ptr().cast_mut(),
                    fields.len() as idx_t,
                ),
            }
        }
    }

    /// Make a `LogicalType` for `union`
    pub fn union_type(fields: &[(&str, LogicalTypeHandle)]) -> Self {
        let keys: Vec<CString> = fields.iter().map(|f| CString::new(f.0).unwrap()).collect();
        let values: Vec<duckdb_logical_type> = fields.iter().map(|it| it.1.ptr).collect();
        let name_ptrs = keys.iter().map(|it| it.as_ptr()).collect::<Vec<*const c_char>>();

        unsafe {
            Self {
                ptr: duckdb_create_union_type(
                    values.as_slice().as_ptr().cast_mut(),
                    name_ptrs.as_slice().as_ptr().cast_mut(),
                    fields.len() as idx_t,
                ),
            }
        }
    }

    /// Logical type ID
    pub fn id(&self) -> LogicalTypeId {
        let duckdb_type_id = unsafe { duckdb_get_type_id(self.ptr) };
        duckdb_type_id.into()
    }
}

#[derive(From, Constructor, Debug)]
pub struct DecimalLogicalType(LogicalTypeHandle);

impl DecimalLogicalType {
    /// Retrieves the decimal width
    /// Returns 0 if the LogicalType is not a decimal
    pub fn decimal_width(&self) -> u8 {
        unsafe { duckdb_decimal_width(self.0.ptr) }
    }

    /// Retrieves the decimal scale
    /// Returns 0 if the LogicalType is not a decimal
    pub fn decimal_scale(&self) -> u8 {
        unsafe { duckdb_decimal_scale(self.0.ptr) }
    }
}

#[derive(From, Constructor, Debug)]
pub struct ListLogicalType(LogicalTypeHandle);

impl ListLogicalType {
    pub fn child_type(&self) -> LogicalType {
        let handle = unsafe { LogicalTypeHandle::new(duckdb_list_type_child_type(self.0.ptr)) };

        LogicalType::from(handle)
    }
}

#[derive(From, Constructor, Debug)]
pub struct UnionLogicalType(LogicalTypeHandle);

impl UnionLogicalType {
    pub fn member_count(&self) -> usize {
        unsafe { duckdb_union_type_member_count(self.0.ptr) as usize }
    }

    /// Logical type child by idx
    pub fn member_type(&self, idx: usize) -> LogicalType {
        let handle = unsafe { LogicalTypeHandle::new(duckdb_union_type_member_type(self.0.ptr, idx as u64)) };
        LogicalType::from(handle)
    }

    /// Logical type child name by idx
    pub fn member_name(&self, idx: usize) -> String {
        unsafe {
            let child_name_ptr = duckdb_union_type_member_name(self.0.ptr, idx as u64);
            let c_str = CString::from_raw(child_name_ptr);
            let name = c_str.to_str().unwrap();
            name.to_string()
        }
    }
}

#[derive(From, Constructor, Debug)]
pub struct StructLogicalType(LogicalTypeHandle);

impl StructLogicalType {
    /// Logical type child by idx
    pub fn child_type(&self, idx: usize) -> LogicalType {
        let handle = unsafe { LogicalTypeHandle::new(duckdb_struct_type_child_type(self.0.ptr, idx as u64)) };
        LogicalType::from(handle)
    }

    pub fn child_count(&self) -> usize {
        unsafe { duckdb_struct_type_child_count(self.0.ptr) as usize }
    }

    /// Logical type child name by idx
    ///
    /// Panics if the logical type is not a struct or union
    pub fn child_name(&self, idx: usize) -> String {
        unsafe {
            let child_name_ptr = duckdb_struct_type_child_name(self.0.ptr, idx as u64);
            let c_str = CString::from_raw(child_name_ptr);
            let name = c_str.to_str().unwrap();
            name.to_string()
        }
    }
}

#[derive(From, Constructor, Debug)]
pub struct MapLogicalType(LogicalTypeHandle);

impl MapLogicalType {
    pub fn key_type(&self) -> LogicalType {
        let handle = unsafe { LogicalTypeHandle::new(duckdb_map_type_key_type(self.0.ptr)) };
        handle.into()
    }

    pub fn value_type(&self) -> LogicalType {
        let handle = unsafe { LogicalTypeHandle::new(duckdb_map_type_value_type(self.0.ptr)) };
        handle.into()
    }
}

#[derive(From, Constructor, Debug)]
pub struct ArrayLogicalType(LogicalTypeHandle);

impl ArrayLogicalType {
    pub fn size(&self) -> usize {
        unsafe { duckdb_array_type_array_size(self.0.ptr) as usize }
    }

    pub fn child_type(&self) -> LogicalType {
        let handle = unsafe { LogicalTypeHandle::new(duckdb_array_type_child_type(self.0.ptr)) };
        LogicalType::from(handle)
    }
}

/// DuckDB Logical Type.
/// Modelled from https://duckdb.org/docs/sql/data_types/overview
#[derive(EnumTryAs, Debug)]
pub enum LogicalType {
    // TODO: Possibly break this down further
    General {
        id: LogicalTypeId,
        handle: LogicalTypeHandle,
    },
    List(ListLogicalType),
    Decimal(DecimalLogicalType),
    Union(UnionLogicalType),
    Struct(StructLogicalType),
    Array(ArrayLogicalType),
    Map(MapLogicalType),
}

impl From<LogicalTypeHandle> for LogicalType {
    fn from(handle: LogicalTypeHandle) -> Self {
        let id = handle.id();
        match id {
            LogicalTypeId::Boolean
            | LogicalTypeId::Tinyint
            | LogicalTypeId::Smallint
            | LogicalTypeId::Integer
            | LogicalTypeId::Bigint
            | LogicalTypeId::UTinyint
            | LogicalTypeId::USmallint
            | LogicalTypeId::UInteger
            | LogicalTypeId::UBigint
            | LogicalTypeId::Float
            | LogicalTypeId::Double
            | LogicalTypeId::Timestamp
            | LogicalTypeId::TimestampTZ
            | LogicalTypeId::Date
            | LogicalTypeId::Time
            | LogicalTypeId::Interval
            | LogicalTypeId::Hugeint
            | LogicalTypeId::Varchar
            | LogicalTypeId::Blob
            | LogicalTypeId::TimestampS
            | LogicalTypeId::TimestampMs
            | LogicalTypeId::TimestampNs
            | LogicalTypeId::Uuid
            | LogicalTypeId::Enum => LogicalType::General { id, handle },
            LogicalTypeId::Decimal => Self::Decimal(handle.into()),
            LogicalTypeId::List => Self::List(handle.into()),
            LogicalTypeId::Struct => Self::Struct(handle.into()),
            LogicalTypeId::Map => Self::Map(handle.into()),
            LogicalTypeId::Union => Self::Union(handle.into()),
            LogicalTypeId::Array => Self::Array(handle.into()),
        }
    }
}

impl LogicalType {
    /// Creates a map type from its child type.
    pub fn new_map(key: &LogicalTypeHandle, value: &LogicalTypeHandle) -> Self {
        Self::Map(LogicalTypeHandle::map(key, value).into())
    }

    /// Creates a list type from its child type.
    pub fn new_list(child_type: &LogicalTypeHandle) -> Self {
        Self::List(LogicalTypeHandle::list(child_type).into())
    }

    /// Creates a array type from its child type.
    pub fn new_array(child_type: &LogicalTypeHandle, size: i32) -> Self {
        Self::Array(LogicalTypeHandle::array(child_type, size).into())
    }

    /// Creates a decimal type from its `width` and `scale`.
    pub fn new_decimal(width: u8, scale: u8) -> Self {
        Self::Decimal(LogicalTypeHandle::decimal(width, scale).into())
    }

    /// Make a `LogicalType` for `struct`
    pub fn new_struct_type(fields: &[(&str, LogicalTypeHandle)]) -> Self {
        Self::Struct(LogicalTypeHandle::struct_type(fields).into())
    }

    /// Make a `LogicalType` for `union`
    pub fn new_union_type(fields: &[(&str, LogicalTypeHandle)]) -> Self {
        Self::Union(LogicalTypeHandle::union_type(fields).into())
    }

    pub fn logical_id(&self) -> LogicalTypeId {
        match self {
            LogicalType::General { id, .. } => *id,
            LogicalType::Decimal(_) => LogicalTypeId::Decimal,
            LogicalType::List(_) => LogicalTypeId::List,
            LogicalType::Struct(_) => LogicalTypeId::Struct,
            LogicalType::Map(_) => LogicalTypeId::Map,
            LogicalType::Union(_) => LogicalTypeId::Union,
            LogicalType::Array(_) => LogicalTypeId::Array,
        }
    }

    /// This method returns the number of children in the struct or union.
    /// This is unsafe because dropping the LogicalTypeHandle will invalidate the pointer.
    pub unsafe fn ptr(&self) -> duckdb_logical_type {
        match self {
            LogicalType::General { handle, .. } => handle.ptr,
            LogicalType::Decimal(ty) => ty.0.ptr,
            LogicalType::List(ty) => ty.0.ptr,
            LogicalType::Struct(ty) => ty.0.ptr,
            LogicalType::Map(ty) => ty.0.ptr,
            LogicalType::Union(ty) => ty.0.ptr,
            LogicalType::Array(ty) => ty.0.ptr,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{LogicalType, LogicalTypeHandle, LogicalTypeId};

    #[test]
    fn test_struct() {
        let fields = &[("hello", LogicalTypeHandle::from(LogicalTypeId::Boolean))];
        let typ = LogicalType::new_struct_type(fields).try_as_struct().unwrap();

        assert_eq!(typ.child_count(), 1);
        assert_eq!(typ.child_name(0), "hello");
        assert_eq!(typ.child_type(0).logical_id(), LogicalTypeId::Boolean);
    }

    #[test]
    fn test_decimal() {
        let typ = LogicalType::new_decimal(10, 2).try_as_decimal().unwrap();

        assert_eq!(typ.decimal_width(), 10);
        assert_eq!(typ.decimal_scale(), 2);
    }

    #[test]
    fn test_union_type() {
        let fields = &[
            ("hello", LogicalTypeHandle::from(LogicalTypeId::Boolean)),
            ("world", LogicalTypeHandle::from(LogicalTypeId::Integer)),
        ];
        let typ = LogicalType::new_union_type(fields).try_as_union().unwrap();

        assert_eq!(typ.member_count(), 2);

        assert_eq!(typ.member_name(0), "hello");
        assert_eq!(typ.member_type(0).logical_id(), LogicalTypeId::Boolean);

        assert_eq!(typ.member_name(1), "world");
        assert_eq!(typ.member_type(1).logical_id(), LogicalTypeId::Integer);
    }

    #[test]
    fn test_map_type() {
        let key = LogicalTypeHandle::from(LogicalTypeId::Varchar);
        let value = LogicalTypeHandle::from(LogicalTypeId::UTinyint);
        let map = LogicalTypeHandle::map(&key, &value);

        assert_eq!(map.id(), LogicalTypeId::Map);
        let typ = LogicalType::new_map(&key, &value).try_as_map().unwrap();
        assert_eq!(typ.key_type().logical_id(), LogicalTypeId::Varchar);
        assert_eq!(typ.value_type().logical_id(), LogicalTypeId::UTinyint);
    }
}
