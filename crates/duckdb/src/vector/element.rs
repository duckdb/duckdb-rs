use std::fmt::Debug;
use std::marker::PhantomData;

use super::*;
use crate::bytes::DuckDBBytes;
use crate::logical_type::LogicalTypeID;
use crate::{
    logical_type::LogicalType,
    types::{
        Array, BigNum, BigNumValue, BitValue, BlobValue, DateValue, Decimal, DecimalValue, InternalDecimalType,
        IntervalValue, List, Map, Struct, TString, TimeNsValue, TimeTzValue, TimeValue, TimestampMsValue,
        TimestampNsValue, TimestampSecValue, TimestampTzNsValue, TimestampTzValue, TimestampValue, Union, UuidValueRaw,
        Variant,
    },
};

/// Describes validation and decoding for a DuckDB logical type.
pub trait VectorElement: Sized {
    /// The DuckDB logical type represented by this Rust type.
    const TYPE_ID: LogicalTypeID;

    /// The borrowed value returned for one vector row.
    type Ref<'a>
    where
        Self: 'a;

    /// Validate nested children before values are read.
    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        let _ = children;
        Ok(other.type_id() == Self::TYPE_ID)
    }

    /// Borrow a value at its physical and logical indexes.
    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a;
}

/// Adds a supported output representation to a readable vector element.
pub trait WritableVectorElement: VectorElement {
    /// The value accepted when writing one row.
    type Write<'a>
    where
        Self: 'a;

    /// Write one value into a writable vector.
    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()>;
}

macro_rules! DeclareVectorElement {
    ($type:tt , $id:expr) => {
        impl VectorElement for $type {
            const TYPE_ID: ffi::DUCKDB_V2_LOGICAL_TYPE_ID = $id;

            type Ref<'a> = &'a $type;

            fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
            where
                Self: Sized + 'a,
            {
                let data_ptr = vector.view.unwrap().data as *const $type;
                unsafe { &*data_ptr.add(physical) }
            }
        }
    };
}

DeclareVectorElement!(bool, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BOOLEAN);
DeclareVectorElement!(u8, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UTINYINT);
DeclareVectorElement!(i8, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_TINYINT);
DeclareVectorElement!(i16, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_SMALLINT);
DeclareVectorElement!(i32, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_INTEGER);
DeclareVectorElement!(i64, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT);
DeclareVectorElement!(u16, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_USMALLINT);
DeclareVectorElement!(u32, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UINTEGER);
DeclareVectorElement!(u64, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT);
DeclareVectorElement!(f32, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_FLOAT);
DeclareVectorElement!(f64, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE);
DeclareVectorElement!(i128, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_HUGEINT);
DeclareVectorElement!(u128, LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UHUGEINT);

macro_rules! declare_storage_vector_element {
    ($type:ty, $type_id:ident) => {
        impl VectorElement for $type {
            const TYPE_ID: LogicalTypeID = LogicalTypeID::$type_id;

            type Ref<'a> = &'a Self;

            fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
            where
                Self: 'a,
            {
                let data_ptr = vector.view.unwrap().data as *const Self;
                unsafe { &*data_ptr.add(physical) }
            }
        }
    };
}

declare_storage_vector_element!(DateValue, DUCKDB_V2_LOGICAL_TYPE_ID_DATE);
declare_storage_vector_element!(TimeValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIME);
declare_storage_vector_element!(TimeNsValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIME_NS);
declare_storage_vector_element!(TimeTzValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIME_TZ);
declare_storage_vector_element!(TimestampValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP);
declare_storage_vector_element!(TimestampSecValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_SEC);
declare_storage_vector_element!(TimestampMsValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_MS);
declare_storage_vector_element!(TimestampNsValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_NS);
declare_storage_vector_element!(TimestampTzValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ);
declare_storage_vector_element!(TimestampTzNsValue, DUCKDB_V2_LOGICAL_TYPE_ID_TIMESTAMP_TZ_NS);
declare_storage_vector_element!(IntervalValue, DUCKDB_V2_LOGICAL_TYPE_ID_INTERVAL);
declare_storage_vector_element!(UuidValueRaw, DUCKDB_V2_LOGICAL_TYPE_ID_UUID);

impl<T> VectorElement for BlobValue<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BLOB;

    type Ref<'a>
        = &'a [u8]
    where
        T: 'a;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        T: 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const DuckDBBytes;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

impl<T> VectorElement for BitValue<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIT;

    type Ref<'a>
        = &'a [u8]
    where
        T: 'a;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        T: 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const DuckDBBytes;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

impl<T: InternalDecimalType, const WIDTH: u8, const SCALE: u8> VectorElement for DecimalValue<T, WIDTH, SCALE> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

    type Ref<'a>
        = &'a Self
    where
        T: 'a;

    fn validate(other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let expected_size = match WIDTH {
            1..=4 => std::mem::size_of::<i16>(),
            5..=9 => std::mem::size_of::<i32>(),
            10..=18 => std::mem::size_of::<i64>(),
            19..=38 => std::mem::size_of::<i128>(),
            _ => return Ok(false),
        };
        if std::mem::size_of::<T>() != expected_size {
            return Ok(false);
        }

        let params = other.get_params()?;
        if params.len() != 2 {
            return Ok(false);
        }
        Ok(params[0].1.get::<u8>()? == WIDTH && params[1].1.get::<u8>()? == SCALE)
    }

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        T: 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const Self;
        unsafe { &*data_ptr.add(physical) }
    }
}

impl VectorElement for BigNumValue {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;

    type Ref<'a> = &'a BigNum;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const BigNum;
        unsafe { &*data_ptr.add(physical) }
    }
}

impl VectorElement for TString {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;

    type Ref<'a> = &'a DuckDBBytes;

    fn validate(_other: &LogicalType, _children: &[Vector<'_, Unknown>]) -> Result<bool> {
        Ok(true)
    }

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const DuckDBBytes;
        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

impl VectorElement for Variant {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARIANT;

    type Ref<'a> = Value;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        vector.get_value_slow(physical).unwrap()
    }
}

impl VectorElement for BigNum {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_BIGNUM;

    type Ref<'a> = &'a BigNum;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const BigNum;
        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

impl VectorElement for String {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_VARCHAR;

    type Ref<'a> = &'a str;

    // TODO return slice.
    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const ffi::duckdb_v2_bytes;

        let string_view = unsafe { &*data_ptr.add(physical) };

        string_view.into()
    }
}

impl<T: InternalDecimalType> VectorElement for Decimal<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_DECIMAL;

    type Ref<'a>
        = &'a T
    where
        Self: 'a;

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const T;

        (unsafe { &*data_ptr.add(physical) }) as _
    }
}

impl<T: WritableVectorElement> WritableVectorElement for List<T> {
    type Write<'a>
        = Vec<Option<T::Write<'a>>>
    where
        T: 'a;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(values) = value else {
            return vector.write_raw::<List<T>>(index, None);
        };

        let offset = vector.child_write_offset;
        let len = values.len();
        let mut child = std::mem::take(&mut vector.children)
            .into_iter()
            .next()
            .expect("validated list child")
            .cast_unchecked::<T>();
        let result = (|| {
            child.set_size(offset + len)?;
            for (child_index, value) in values.into_iter().enumerate() {
                child.write(offset + child_index, value)?;
            }
            Ok(())
        })();
        vector.children = vec![child.into_unknown()];
        result?;
        vector.child_write_offset += len;
        vector.write_raw::<List<T>>(
            index,
            Some(List {
                offset: offset as u64,
                length: len as u64,
                _marker: PhantomData,
            }),
        )
    }
}

impl<L: VectorElement> VectorElement for List<L> {
    type Ref<'a>
        = ListRef<'a, L>
    where
        L: 'a;

    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_LIST;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        if children.len() != 1 {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: "List vector must have exactly one child".to_string(),
            });
        }

        let child = children.first().unwrap();
        child.validate_as::<L>()
    }

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const List<L>;
        let list = unsafe { &*data_ptr.add(physical) };
        ListRef {
            list,
            child: &vector.children[0],
        }
    }
}

/// A borrowed list row backed by a range in its child vector.
pub struct ListRef<'a, T> {
    list: &'a List<T>,
    child: &'a Vector<'a, Unknown>,
}

impl<'a, T: VectorElement> ListRef<'a, T> {
    /// Iterate over the list's values.
    pub fn iter(&self) -> ListIterator<'a, T> {
        ListIterator {
            child: self.child,
            offset: self.list.offset as usize,
            length: self.list.length as usize,
            index: 0,
            _type: PhantomData,
        }
    }

    /// Return the number of values in the list.
    pub fn len(&self) -> usize {
        self.list.length as usize
    }

    /// Return whether the list contains no values.
    pub fn is_empty(&self) -> bool {
        self.list.length == 0
    }

    /// Return a unified view of the list's child vector.
    pub fn view(&self) -> ffi::duckdb_v2_vector_view {
        self.child.view.expect("list child must be readable")
    }
}

/// Traverses the child-vector range belonging to a [`ListRef`].
pub struct ListIterator<'a, T> {
    child: &'a Vector<'a, Unknown>,
    offset: usize,
    length: usize,
    index: usize,
    _type: PhantomData<T>,
}

impl<'a, T: VectorElement + 'a> Iterator for ListIterator<'a, T> {
    type Item = Option<T::Ref<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let logical = self.offset + self.index;
        self.index += 1;

        Some(self.child.get_as_unchecked::<T>(logical))
    }
}

impl<T: VectorElement> VectorElement for Array<T> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_ARRAY;

    type Ref<'a>
        = ArrayRef<'a, T>
    where
        T: 'a;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let child = children.first().ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: "Array vector is missing its child vector".to_string(),
        })?;
        child.validate_as::<T>()
    }

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let slice_size = vector.children[0].len() / vector.len();

        ArrayRef {
            offset: physical * slice_size,
            size: slice_size,
            _length: vector.children[0].len(),
            child: &vector.children[0],
            _marker: PhantomData,
        }
    }
}

/// A borrowed array row backed by a contiguous range in its child vector.
pub struct ArrayRef<'a, T> {
    offset: usize,
    size: usize,
    _length: usize,
    child: &'a Vector<'a, Unknown>,
    _marker: PhantomData<T>,
}

impl<'a, T: VectorElement> ArrayRef<'a, T> {
    /// Iterate over the array's values.
    pub fn iter(&self) -> ArrayIterator<'a, T> {
        ArrayIterator {
            child: self.child,
            offset: self.offset,
            length: self.size,
            index: 0,
            _type: PhantomData,
        }
    }

    /// Return the number of values in the array.
    pub fn size(&self) -> usize {
        self.size
    }
}

/// Traverses the child-vector range belonging to an [`ArrayRef`].
pub struct ArrayIterator<'a, T> {
    child: &'a Vector<'a, Unknown>,
    offset: usize,
    length: usize,
    index: usize,
    _type: PhantomData<T>,
}

impl<'a, T: VectorElement + 'a> Iterator for ArrayIterator<'a, T> {
    type Item = Option<T::Ref<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.length {
            return None;
        }

        let logical = self.offset + self.index;
        self.index += 1;

        Some(self.child.get_as_unchecked::<T>(logical))
    }
}

impl VectorElement for Struct {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_STRUCT;
    type Ref<'a> = StructRow<'a>;

    fn get<'a, U>(vector: &'a Vector<'_, U>, _physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        StructRow {
            children: &vector.children,
            logical_type: vector.logical_type(),
            logical,
        }
    }
}

trait StructFieldWrite {
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()>;
}

struct TypedStructField<'a, T: WritableVectorElement + 'a> {
    value: Option<T::Write<'a>>,
}

impl<'a, T> StructFieldWrite for TypedStructField<'a, T>
where
    T: WritableVectorElement + 'a,
    T::Write<'a>: 'a,
{
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()> {
        child.write_as::<T>(index, self.value)
    }
}

/// Heterogeneous field values written into one struct row.
#[derive(Default)]
pub struct StructWrite<'a> {
    fields: Vec<Box<dyn StructFieldWrite + 'a>>,
}

impl<'a> StructWrite<'a> {
    /// Append a typed field value in schema order.
    pub fn field<T>(mut self, value: Option<T::Write<'a>>) -> Self
    where
        T: WritableVectorElement + 'a,
        T::Write<'a>: 'a,
    {
        self.fields.push(Box::new(TypedStructField::<T> { value }));
        self
    }
}

impl WritableVectorElement for Struct {
    type Write<'a> = StructWrite<'a>;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            return vector.set_row_validity(index, false);
        };
        if value.fields.len() != vector.children.len() {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Struct row has {} fields, expected {}",
                    value.fields.len(),
                    vector.children.len()
                ),
            });
        }

        vector.set_row_validity(index, true)?;
        for (field, child) in value.fields.into_iter().zip(&mut vector.children) {
            if child.len() != vector.len {
                child.set_size(vector.len)?;
            }
            field.write(child, index)?;
        }
        Ok(())
    }
}

/// A borrowed struct row that resolves named fields to child vectors.
pub struct StructRow<'a> {
    children: &'a [Vector<'a, Unknown>],
    logical_type: &'a LogicalType,
    logical: usize,
}

impl<'a> StructRow<'a> {
    // TODO: fn get_index(idx: usize)

    /// Return a struct field by name after validating its logical type.
    pub fn get<T: VectorElement>(&self, name: &str) -> Result<Option<T::Ref<'a>>> {
        let fields = self.logical_type.get_params()?;
        let index = fields
            .iter()
            .position(|(field_name, _)| field_name == name)
            .ok_or_else(|| Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!("Field '{}' not found in struct", name),
            })?;
        let child = self.children.get(index).ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("Struct field '{}' is missing its child vector", name),
        })?;

        if child.logical_type().type_id() != T::TYPE_ID {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "Field '{}' has type {:?}, expected {:?}",
                    name,
                    child.logical_type().type_id(),
                    T::TYPE_ID
                ),
            });
        }

        child.get_as_checked::<T>(self.logical)
    }
}

/// Values written into one map row.
pub struct MapWrite<'a, K: WritableVectorElement + 'a, V: WritableVectorElement + 'a> {
    /// Key-value entries for the row.
    pub entries: Vec<(K::Write<'a>, V::Write<'a>)>,
}

impl<K: VectorElement, V: VectorElement> VectorElement for Map<K, V> {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_MAP;

    type Ref<'a>
        = MapRow<'a, K, V>
    where
        K: 'a,
        V: 'a;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        if children.len() != 2 {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: "Map vector must have exactly two children".to_string(),
            });
        }

        children[0].validate_as::<K>()?;
        children[1].validate_as::<V>()
    }

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.view.unwrap().data as *const ffi::duckdb_v2_list_entry;
        let list = unsafe { &*data_ptr.add(physical) };

        MapRow::<K, V> {
            children: &vector.children,
            offset: list.offset as usize,
            length: list.length as usize,
            _marker: PhantomData,
        }
    }
}

impl<K: WritableVectorElement, V: WritableVectorElement> WritableVectorElement for Map<K, V> {
    type Write<'a>
        = MapWrite<'a, K, V>
    where
        K: 'a,
        V: 'a;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            return vector.write_raw::<ffi::duckdb_v2_list_entry>(index, None);
        };

        let offset = vector.child_write_offset;
        let len = value.entries.len();
        let mut children = std::mem::take(&mut vector.children).into_iter();
        let mut keys = children.next().expect("validated map key child").cast_unchecked::<K>();
        let mut values = children
            .next()
            .expect("validated map value child")
            .cast_unchecked::<V>();

        let result = (|| {
            keys.set_size(offset + len)?;
            values.set_size(offset + len)?;
            for (child_index, (key, value)) in value.entries.into_iter().enumerate() {
                keys.write(offset + child_index, Some(key))?;
                values.write(offset + child_index, Some(value))?;
            }
            Ok(())
        })();

        vector.children = vec![keys.into_unknown(), values.into_unknown()];
        result?;
        vector.child_write_offset += len;
        vector.write_raw(
            index,
            Some(ffi::duckdb_v2_list_entry {
                offset: offset as u64,
                length: len as u64,
            }),
        )
    }
}

/// A borrowed map row backed by matching ranges in key and value vectors.
pub struct MapRow<'a, K, V> {
    pub(crate) children: &'a [Vector<'a, Unknown>],
    pub(crate) offset: usize,
    pub(crate) length: usize,
    pub(crate) _marker: PhantomData<(K, V)>,
}

impl<'a, K: Debug + VectorElement, V: VectorElement + Debug> MapRow<'a, K, V>
where
    for<'b> K::Ref<'b>: PartialEq<&'b K>,
    for<'b> <V as VectorElement>::Ref<'b>: Debug,
{
    /// Return the value associated with `key`, if it exists.
    pub fn get(&self, key: &K) -> Result<Option<V::Ref<'a>>> {
        let mut index = None;

        for logical in self.offset..self.offset + self.length {
            if self.children[0]
                .get_as_unchecked::<K>(logical)
                .is_some_and(|value| value == key)
            {
                index = Some(logical);
                break;
            }
        }

        if index.is_none() {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!("Key '{:?}' not found in map", key),
            });
        }

        Ok(self.children[1].get_as_unchecked::<V>(index.unwrap()))
    }

    /// Return the map's keys.
    pub fn keys(&self) -> Result<Vec<K::Ref<'a>>> {
        let mut keys = Vec::new();

        self.children[0].validate_as::<K>()?;

        for logical in self.offset..self.offset + self.length {
            if let Some(key) = self.children[0].get_as_unchecked::<K>(logical) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    /// Return the map's values.
    pub fn values(&self) -> Result<Vec<V::Ref<'a>>> {
        let mut values: Vec<_> = Vec::new();

        self.children[1].validate_as::<V>()?;

        for logical in self.offset..self.offset + self.length {
            if let Some(value) = self.children[1].get_as_unchecked::<V>(logical) {
                values.push(value);
            }
        }

        Ok(values)
    }
}

impl VectorElement for Union {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNION;
    type Ref<'a> = UnionRow<'a>;

    fn validate(other: &LogicalType, children: &[Vector<'_, Unknown>]) -> Result<bool> {
        if other.type_id() != Self::TYPE_ID {
            return Ok(false);
        }

        let tag = children.first().ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: "Union vector is missing its tag child".to_string(),
        })?;
        tag.validate_as::<u8>()
    }

    fn get<'a, U>(vector: &'a Vector<'_, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        UnionRow {
            children: &vector.children,
            logical: physical,
        }
    }
}

/// The element type has not been checked against the vector's logical type yet.
#[derive(Debug, Clone)]
pub struct Unknown;

impl VectorElement for Unknown {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::DUCKDB_V2_LOGICAL_TYPE_ID_UNKNOWN;

    type Ref<'a> = ();

    fn get<'a, U>(_vector: &'a Vector<'_, U>, _physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        panic!("Unknown type: cannot index into data of unknown type");
    }
}

/// A borrowed union row with access to its tag and member child vectors.
pub struct UnionRow<'a> {
    children: &'a [Vector<'a, Unknown>],
    logical: usize,
}

impl<'a> UnionRow<'a> {
    /// Return the active union member index.
    pub fn member(&self) -> u8 {
        *self.children[0].get_as_unchecked::<u8>(self.logical).unwrap()
    }

    /// Return a union member by index after validating its logical type.
    pub fn get<T: VectorElement>(&self, index: usize) -> Result<Option<T::Ref<'a>>> {
        let index = index + 1;

        if index >= self.children.len() {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!(
                    "Union member index {} is out of bounds ({} members)",
                    index - 1,
                    self.children.len() - 1
                ),
            });
        }

        let child = self.children.get(index).ok_or_else(|| Error {
            code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
            message: format!("Union member {} is missing its child vector", index - 1),
        })?;

        child.get_as_checked::<T>(self.logical)
    }
}

trait UnionFieldWrite {
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()>;
}

struct TypedUnionField<'a, T: WritableVectorElement + 'a> {
    member: Option<T::Write<'a>>,
}

impl<'a, T> UnionFieldWrite for TypedUnionField<'a, T>
where
    T: WritableVectorElement + 'a,
    T::Write<'a>: 'a,
{
    fn write(self: Box<Self>, child: &mut Vector<'_, Unknown>, index: usize) -> Result<()> {
        child.write_as::<T>(index, self.member)
    }
}

/// A typed value for one writable `UNION` row.
pub struct UnionWriter<'a> {
    tag: u8,
    value: Box<dyn UnionFieldWrite + 'a>,
}

impl<'a> UnionWriter<'a> {
    /// Select a zero-based union member and provide its typed value.
    pub fn set_value<T>(index: u8, value: Option<T::Write<'a>>) -> Self
    where
        T: WritableVectorElement + 'a,
        T::Write<'a>: 'a,
    {
        Self {
            tag: index,
            value: Box::new(TypedUnionField::<T> { member: value }),
        }
    }
}

impl WritableVectorElement for Union {
    type Write<'a> = UnionWriter<'a>;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        let Some(value) = value else {
            vector.set_row_validity(index, false)?;
            for child in &mut vector.children {
                child.set_row_validity(index, false)?;
            }
            return Ok(());
        };

        let member_count = vector.children.len().saturating_sub(1);
        if value.tag as usize >= member_count {
            return Err(Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_PARAMETER_INVALID,
                message: format!(
                    "Union member index {} is out of bounds ({} members)",
                    value.tag, member_count
                ),
            });
        }

        vector.set_row_validity(index, true)?;
        vector.children[0].write_as::<u8>(index, Some(value.tag))?;
        for child in &mut vector.children[1..] {
            child.set_row_validity(index, false)?;
        }
        value.value.write(&mut vector.children[1 + value.tag as usize], index)
    }
}

macro_rules! DeclareWritableVectorElement {
    ($type:ty) => {
        impl WritableVectorElement for $type {
            type Write<'a> = $type;

            fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
                vector.write_raw(index, value)
            }
        }
    };
}

DeclareWritableVectorElement!(bool);
DeclareWritableVectorElement!(i8);
DeclareWritableVectorElement!(i16);
DeclareWritableVectorElement!(i32);
DeclareWritableVectorElement!(i64);
DeclareWritableVectorElement!(i128);
DeclareWritableVectorElement!(u8);
DeclareWritableVectorElement!(u16);
DeclareWritableVectorElement!(u32);
DeclareWritableVectorElement!(u64);
DeclareWritableVectorElement!(u128);
DeclareWritableVectorElement!(f32);
DeclareWritableVectorElement!(f64);
DeclareWritableVectorElement!(String);

impl WritableVectorElement for Variant {
    type Write<'a> = Value;

    fn write(vector: &mut Vector<'_, Self>, index: usize, value: Option<Self::Write<'_>>) -> Result<()> {
        match value {
            Some(value) => vector.write_value_slow(index, value),
            None => vector.write_raw::<Unknown>(index, None),
        }
    }
}
