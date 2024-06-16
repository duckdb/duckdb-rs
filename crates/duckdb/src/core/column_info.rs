use arrow::datatypes::DataType;
use derive_more::Unwrap;

use super::LogicalType;

#[derive(Debug)]
pub struct ListColumnInfo {
    pub child: Box<ColumnInfo>,
}

// note: Also works for fixed size length
#[derive(Debug)]
pub struct ArrayColumnInfo {
    pub array_size: usize,
    pub child: Box<ColumnInfo>,
}

#[derive(Debug)]
pub struct StructColumnInfo {
    pub children: Vec<ColumnInfo>,
}

#[derive(Debug)]
pub struct UnionColumnInfo {
    pub members: Vec<ColumnInfo>,
}

/// Represents the duckdb schema for error handling and caching purposes.
#[derive(Unwrap, Debug)]
pub enum ColumnInfo {
    Flat,
    List(ListColumnInfo),
    Array(ArrayColumnInfo),
    Struct(StructColumnInfo),
    Union(UnionColumnInfo),
}

impl ColumnInfo {
    pub fn new(logical_type: &LogicalType) -> Self {
        match logical_type {
            LogicalType::General { .. } | LogicalType::Decimal(_) => ColumnInfo::Flat,
            LogicalType::List(ty) => {
                let child_type = ty.child_type();
                let child_info = Self::new(&child_type);

                ColumnInfo::List(ListColumnInfo {
                    child: Box::new(child_info),
                })
            }
            LogicalType::Struct(ty) => {
                let child_count = ty.child_count();

                let child_column_infos = (0..child_count)
                    .map(|i| {
                        let child_type = ty.child_type(i);
                        Self::new(&child_type)
                    })
                    .collect::<Vec<_>>();

                ColumnInfo::Struct(StructColumnInfo {
                    children: child_column_infos,
                })
            }
            LogicalType::Map(_) => unimplemented!(),
            LogicalType::Union(ty) => {
                let child_count = ty.member_count();

                let member_column_infos = (0..child_count)
                    .map(|i| {
                        let child_type = ty.member_type(i);
                        Self::new(&child_type)
                    })
                    .collect::<Vec<_>>();

                ColumnInfo::Union(UnionColumnInfo {
                    members: member_column_infos,
                })
            }
            LogicalType::Array(ty) => {
                let array_size = ty.size();
                let child_type = ty.child_type();
                let child_info = Self::new(&child_type);

                ColumnInfo::Array(ArrayColumnInfo {
                    array_size,
                    child: Box::new(child_info),
                })
            }
        }
    }

    pub fn new_from_data_type(data_type: &DataType) -> Self {
        match data_type {
            dt if dt.is_primitive() => ColumnInfo::Flat,
            DataType::Binary | DataType::Utf8 | DataType::LargeUtf8 => ColumnInfo::Flat,
            DataType::List(ty) => {
                let child_info = Self::new_from_data_type(ty.data_type());
                ColumnInfo::List(ListColumnInfo {
                    child: Box::new(child_info),
                })
            }
            DataType::FixedSizeList(field, size) => {
                let child_info = Self::new_from_data_type(field.data_type());
                ColumnInfo::Array(ArrayColumnInfo {
                    array_size: *size as usize,
                    child: Box::new(child_info),
                })
            }
            DataType::Struct(fields) => {
                let children = fields
                    .iter()
                    .map(|field| {
                        let child_info = Self::new_from_data_type(field.data_type());
                        child_info
                    })
                    .collect::<Vec<_>>();
                ColumnInfo::Struct(StructColumnInfo { children })
            }
            DataType::Union(fields, _) => {
                let members = fields
                    .iter()
                    .map(|(_, field)| {
                        let child_info = Self::new_from_data_type(field.data_type());
                        child_info
                    })
                    .collect::<Vec<_>>();
                ColumnInfo::Union(UnionColumnInfo { members })
            }
            DataType::Map(_, _) => todo!(),
            dt => unimplemented!("data type: {} is not implemented yet.", dt),
        }
    }
}
