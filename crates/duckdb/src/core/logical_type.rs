use std::{
    ffi::{c_char, CString},
    fmt::Debug,
};

use crate::ffi::*;

/// Logical Type Id
/// <https://duckdb.org/docs/api/c/types>
#[repr(u32)]
#[derive(Debug, PartialEq, Eq)]
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
            DUCKDB_TYPE_DUCKDB_TYPE_STRUCT => Self::Struct,
            DUCKDB_TYPE_DUCKDB_TYPE_MAP => Self::Map,
            DUCKDB_TYPE_DUCKDB_TYPE_UUID => Self::Uuid,
            DUCKDB_TYPE_DUCKDB_TYPE_UNION => Self::Union,
            DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_TZ => Self::TimestampTZ,
            _ => panic!(),
        }
    }
}

/// DuckDB Logical Type.
/// <https://duckdb.org/docs/sql/data_types/overview>
pub struct LogicalTypeHandle {
    pub(crate) ptr: duckdb_logical_type,
}

impl Debug for LogicalTypeHandle {
    /// Debug implementation for LogicalType
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let id = self.id();
        match id {
            LogicalTypeId::Struct => {
                write!(f, "struct<")?;
                for i in 0..self.num_children() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}: {:?}", self.child_name(i), self.child(i))?;
                }
                write!(f, ">")
            }
            _ => write!(f, "{:?}", self.id()),
        }
    }
}

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

    /// Creates an array type from its child type.
    pub fn array(child_type: &LogicalTypeHandle, array_size: u64) -> Self {
        unsafe {
            Self {
                ptr: duckdb_create_array_type(child_type.ptr, array_size),
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

    /// Retrieves the decimal width
    /// Returns 0 if the LogicalType is not a decimal
    pub fn decimal_width(&self) -> u8 {
        unsafe { duckdb_decimal_width(self.ptr) }
    }

    /// Retrieves the decimal scale
    /// Returns 0 if the LogicalType is not a decimal
    pub fn decimal_scale(&self) -> u8 {
        unsafe { duckdb_decimal_scale(self.ptr) }
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

    /// Logical type children num
    pub fn num_children(&self) -> usize {
        match self.id() {
            LogicalTypeId::Struct => unsafe { duckdb_struct_type_child_count(self.ptr) as usize },
            LogicalTypeId::Union => unsafe { duckdb_union_type_member_count(self.ptr) as usize },
            LogicalTypeId::List => 1,
            _ => 0,
        }
    }

    /// Logical type child name by idx
    ///
    /// Panics if the logical type is not a struct or union
    pub fn child_name(&self, idx: usize) -> String {
        unsafe {
            let child_name_ptr = match self.id() {
                LogicalTypeId::Struct => duckdb_struct_type_child_name(self.ptr, idx as u64),
                LogicalTypeId::Union => duckdb_union_type_member_name(self.ptr, idx as u64),
                _ => panic!("not a struct or union"),
            };
            let c_str = CString::from_raw(child_name_ptr);
            let name = c_str.to_str().unwrap();
            name.to_string()
        }
    }

    /// Logical type child by idx
    pub fn child(&self, idx: usize) -> Self {
        let c_logical_type = unsafe {
            match self.id() {
                LogicalTypeId::Struct => duckdb_struct_type_child_type(self.ptr, idx as u64),
                LogicalTypeId::Union => duckdb_union_type_member_type(self.ptr, idx as u64),
                _ => panic!("not a struct or union"),
            }
        };
        unsafe { Self::new(c_logical_type) }
    }
}

#[cfg(test)]
mod test {
    use crate::core::{LogicalTypeHandle, LogicalTypeId};

    #[test]
    fn test_struct() {
        let fields = &[("hello", LogicalTypeHandle::from(crate::core::LogicalTypeId::Boolean))];
        let typ = LogicalTypeHandle::struct_type(fields);

        assert_eq!(typ.num_children(), 1);
        assert_eq!(typ.child_name(0), "hello");
        assert_eq!(typ.child(0).id(), crate::core::LogicalTypeId::Boolean);
    }

    #[test]
    fn test_decimal() {
        let typ = LogicalTypeHandle::decimal(10, 2);

        assert_eq!(typ.id(), crate::core::LogicalTypeId::Decimal);
        assert_eq!(typ.decimal_width(), 10);
        assert_eq!(typ.decimal_scale(), 2);
    }

    #[test]
    fn test_decimal_methods() {
        let typ = LogicalTypeHandle::from(crate::core::LogicalTypeId::Varchar);

        assert_eq!(typ.decimal_width(), 0);
        assert_eq!(typ.decimal_scale(), 0);
    }

    #[test]
    fn test_union_type() {
        let fields = &[
            ("hello", LogicalTypeHandle::from(LogicalTypeId::Boolean)),
            ("world", LogicalTypeHandle::from(LogicalTypeId::Integer)),
        ];
        let typ = LogicalTypeHandle::union_type(fields);

        assert_eq!(typ.num_children(), 2);

        assert_eq!(typ.child_name(0), "hello");
        assert_eq!(typ.child(0).id(), LogicalTypeId::Boolean);

        assert_eq!(typ.child_name(1), "world");
        assert_eq!(typ.child(1).id(), LogicalTypeId::Integer);
    }
}
