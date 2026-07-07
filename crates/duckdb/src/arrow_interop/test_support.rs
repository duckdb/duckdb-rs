use arrow::{
    array::FixedSizeBinaryArray,
    buffer::{Buffer, NullBuffer},
    datatypes::{DataType, Field},
};
use std::collections::HashMap;
use uuid::Uuid;

use super::{UUID_BYTE_WIDTH, UUID_EXTENSION_NAME};

pub(crate) const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";

pub(crate) fn uuid_metadata() -> HashMap<String, String> {
    HashMap::from([(ARROW_EXTENSION_NAME_KEY.to_string(), UUID_EXTENSION_NAME.to_string())])
}

pub(crate) fn uuid_field(name: &str) -> Field {
    Field::new(name, DataType::FixedSizeBinary(UUID_BYTE_WIDTH), true).with_metadata(uuid_metadata())
}

pub(crate) fn uuid_array(values: &[Uuid], nulls: Option<Vec<bool>>) -> FixedSizeBinaryArray {
    let buffer = Buffer::from_vec(
        values
            .iter()
            .flat_map(|uuid| uuid.as_bytes().iter().copied())
            .collect::<Vec<_>>(),
    );
    FixedSizeBinaryArray::new(UUID_BYTE_WIDTH, buffer, nulls.map(NullBuffer::from))
}
