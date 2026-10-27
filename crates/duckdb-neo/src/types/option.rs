use crate::{
    Result,
    data_chunk::VectorCollection,
    logical_type::{LogicalType, LogicalTypeID},
    vector::{OwnedVectorElement, VectorElement, VectorReader, VectorWriter},
};

// SAFETY: Option changes nullability, not the underlying physical layout or child schema.
unsafe impl<T: VectorElement> VectorElement for Option<T> {
    const TYPE_ID: LogicalTypeID = T::TYPE_ID;
    type Internal<'a> = T::Internal<'a>;
    type Ref<'a>
        = T::Ref<'a>
    where
        T: 'a;

    fn validate(ty: &LogicalType, children: Option<&VectorCollection>) -> Result<bool> {
        T::validate(ty, children)
    }

    unsafe fn get<'a, U: VectorElement>(reader: &VectorReader<'a, U>, physical: usize, logical: usize) -> Self::Ref<'a>
    where
        Self: 'a,
    {
        unsafe { T::get(reader, physical, logical) }
    }
}

// SAFETY: flattening optional inputs preserves SQL NULLs; T maintains the underlying validity.
unsafe impl<T: OwnedVectorElement> OwnedVectorElement for Option<T> {
    #[inline]
    unsafe fn write_owned(writer: &mut VectorWriter<'_, Self>, index: usize, value: Option<Self>) -> Result<()> {
        unsafe { T::write_owned(writer.cast_unchecked::<T>(), index, value.flatten()) }
    }
}
