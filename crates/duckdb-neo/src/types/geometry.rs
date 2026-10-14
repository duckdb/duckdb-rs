use crate::{
    DuckDBType, FromValue, Parameters, Result, ToValue,
    bytes::DuckDBBytes,
    connection::FFILink,
    error::check_api_call,
    ffi,
    ffi_str::DuckDBStr,
    logical_type::{LogicalType, LogicalTypeID},
    raw::RawExt,
    value::{Value, ValueInput},
    vector::{HasDuckDBBytes, VectorElement, VectorReader, VectorWriter, WritableVectorElement},
};

pub struct GeometryValue(pub Vec<u8>);

impl DuckDBType for GeometryValue {
    fn logical_type<C: FFILink + ?Sized>(link: &C) -> Result<LogicalType> {
        link.logical_type_create("GEOMETRY", Parameters::None)
    }
}

impl ToValue for GeometryValue {
    fn value<C: FFILink + ?Sized>(&self, link: &C) -> Result<Value> {
        link.create_value(ValueInput::Geometry(self.0.as_slice()))
    }
}

impl FromValue for GeometryValue {
    fn _get_inner(value: &Value) -> Result<Self> {
        let blob = check_api_call!(ffi::duckdb_v2_value_get_blob, value.raw(), RET)?;

        let bytes = DuckDBStr::from_raw(blob).as_bytes().unwrap().to_vec();
        Ok(GeometryValue(bytes))
    }
}

impl HasDuckDBBytes for GeometryValue {}

// SAFETY: `DuckDBBytes` matches DuckDB's geometry storage.
unsafe impl VectorElement for GeometryValue {
    const TYPE_ID: LogicalTypeID = LogicalTypeID::Geometry;

    type Internal<'a> = DuckDBBytes<'a>;

    type Ref<'a> = &'a [u8];

    unsafe fn get<'a, U: VectorElement>(vector: &VectorReader<'a, U>, physical: usize, _logical: usize) -> Self::Ref<'a>
    where
        Self: Sized + 'a,
    {
        let data_ptr = vector.as_ptr() as *const DuckDBBytes<'a>;
        unsafe { &*data_ptr.add(physical) }.get_data()
    }
}

// SAFETY: `write_bytes` stores the bytes in the vector's arena.
unsafe impl WritableVectorElement for GeometryValue {
    type Write<'a> = &'a [u8];

    unsafe fn write(
        vector: &mut VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()> {
        // SAFETY: The caller guarantees a writable, flat, in-bounds row.
        unsafe { vector.write_bytes(index, value) }
    }
}

#[cfg(test)]
mod tests {
    use geo_traits::{GeometryTrait, GeometryType, to_geo::ToGeoPoint};

    use crate::{
        Parameters,
        environment::{Environment, StorageLocation},
        types::geometry::GeometryValue,
    };

    #[test]
    pub fn test_geometry_basic_usage() -> crate::Result<()> {
        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        conn.execute(
            "CREATE TABLE geometries (
                id INTEGER,
                geom GEOMETRY
            ); ",
            Parameters::None,
        )?;

        assert_eq!(
            conn.execute(
                "INSERT INTO geometries VALUES
  (1, 'POINT (30 10)'),
  (2, 'LINESTRING (30 10, 10 30, 40 40)'),
  (3, 'POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))'),
  (4, 'MULTIPOINT ((10 40), (40 30), (20 20), (30 10))'),
  (5, 'MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20))'),
  (6, 'MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10,15 5)))'),
  (7, 'GEOMETRYCOLLECTION (POINT(40 10), LINESTRING(10 10,20 20,10 40), POLYGON((40 40,20 45,45 30,40 40)))');",
                Parameters::None,
            )?,
            7
        );

        let mut query = conn.query("SELECT geom FROM geometries;", Parameters::None)?;

        while let Some(chunk) = query.next_chunk()? {
            let col = chunk.get_vector_at::<GeometryValue>(0)?;

            assert!(col.len() == 7);

            for row in col.reader()?.iter() {
                let item = wkb::reader::read_wkb(row.unwrap()).unwrap();

                if let GeometryType::Point(p) = item.as_type() {
                    dbg!("POINT", p, p.to_point());
                    assert_eq!(p.to_point().x_y(), (30.0, 10.0))
                } else {
                    panic!("")
                }
            }
        }

        Ok(())
    }
}
