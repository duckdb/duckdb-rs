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

#[derive(Debug)]
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
    type Write<'a> = &'a GeometryValue;

    unsafe fn write(
        vector: &mut VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self::Write<'_>>,
    ) -> Result<()> {
        // SAFETY: The caller guarantees a writable, flat, in-bounds row.
        unsafe { vector.write_bytes(index, value.map(|x| x.0.as_slice())) }
    }
}

#[cfg(feature = "geozero")]
impl geozero::GeozeroGeometry for GeometryValue {
    fn process_geom<P: geozero::GeomProcessor>(&self, processor: &mut P) -> geozero::error::Result<()> {
        geozero::wkb::process_wkb_geom(&mut self.0.as_slice(), processor)
    }

    // ISO WKB encodes Z/M in the thousands digit of the geometry type code.
    fn dims(&self) -> geozero::CoordDimensions {
        use geozero::CoordDimensions;

        let Some(header) = self.0.first_chunk::<5>() else {
            return CoordDimensions::xy();
        };
        let code = [header[1], header[2], header[3], header[4]];
        let code = if header[0] == 0 {
            u32::from_be_bytes(code)
        } else {
            u32::from_le_bytes(code)
        };
        match code / 1000 {
            1 => CoordDimensions::xyz(),
            2 => CoordDimensions::xym(),
            3 => CoordDimensions::xyzm(),
            _ => CoordDimensions::xy(),
        }
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

    /// DuckDB WKT and the coordinate dimensions it should decode to.
    #[cfg(feature = "geozero")]
    const CASES: [(&str, geozero::CoordDimensions); 17] = {
        use geozero::CoordDimensions as D;
        [
            ("POINT (30 10)", D::xy()),
            ("LINESTRING (30 10, 10 30, 40 40)", D::xy()),
            ("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))", D::xy()),
            ("MULTIPOINT ((10 40), (40 30), (20 20), (30 10))", D::xy()),
            (
                "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20))",
                D::xy(),
            ),
            (
                "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))",
                D::xy(),
            ),
            (
                "GEOMETRYCOLLECTION (POINT (40 10), LINESTRING (10 10, 20 20, 10 40))",
                D::xy(),
            ),
            ("POINT Z (30 10 5)", D::xyz()),
            ("POINT M (30 10 7)", D::xym()),
            ("POINT ZM (30 10 5 7)", D::xyzm()),
            ("LINESTRING Z (30 10 1, 10 30 2, 40 40 3)", D::xyz()),
            ("POLYGON M ((30 10 1, 40 40 2, 20 40 3, 10 20 4, 30 10 1))", D::xym()),
            ("MULTIPOINT ZM ((10 40 1 2), (40 30 3 4))", D::xyzm()),
            ("MULTILINESTRING Z ((10 10 1, 20 20 2), (40 40 3, 30 30 4))", D::xyz()),
            (
                "MULTIPOLYGON ZM (((30 20 1 2, 45 40 3 4, 10 40 5 6, 30 20 1 2)))",
                D::xyzm(),
            ),
            (
                "GEOMETRYCOLLECTION M (POINT M (40 10 1), LINESTRING M (10 10 2, 20 20 3))",
                D::xym(),
            ),
            (
                "GEOMETRYCOLLECTION ZM (POINT ZM (40 10 1 2), POLYGON ZM ((0 0 1 2, 1 0 3 4, 1 1 5 6, 0 0 1 2)))",
                D::xyzm(),
            ),
        ]
    };

    #[cfg(feature = "geozero")]
    #[test]
    pub fn test_geometry_geozero_read() -> crate::Result<()> {
        use geozero::{GeozeroGeometry, ToWkb, ToWkt};

        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        for (wkt, dims) in CASES {
            let geometry = GeometryValue(query_geometry(&conn, wkt)?);

            let actual = geometry.dims();
            assert_eq!((actual.z, actual.m), (dims.z, dims.m), "{wkt}");
            // Re-encoding must reproduce DuckDB's bytes, so every Z/M ordinate was read.
            assert_eq!(geometry.to_wkb(dims).unwrap(), geometry.0, "{wkt}");
        }

        let geometry = GeometryValue(query_geometry(&conn, "POINT ZM (30 10 5 7)")?);
        assert_eq!(geometry.to_wkt_ndim(geometry.dims()).unwrap(), "POINT(30 10 5 7)");

        Ok(())
    }

    #[cfg(feature = "geozero")]
    fn query_geometry(conn: &crate::connection::Connection, wkt: &str) -> crate::Result<Vec<u8>> {
        let mut query = conn.query("SELECT $1::GEOMETRY", Parameters::Positional(&[&wkt]))?;
        let chunk = query.next_chunk()?.unwrap();
        Ok(chunk
            .get_vector_at::<GeometryValue>(0)?
            .reader()?
            .get(0)?
            .unwrap()
            .to_vec())
    }

    #[cfg(feature = "geozero")]
    #[test]
    pub fn test_geometry_geozero_write() -> crate::Result<()> {
        use geozero::{ToWkb, wkt::Wkt};

        use crate::{DuckDBType, data_chunk::DataChunk};

        let env = Environment::new()?;
        let db = env.open(StorageLocation::InMemory)?;
        let conn = db.connect()?;

        let geometries = CASES
            .iter()
            .map(|(wkt, dims)| GeometryValue(Wkt(wkt).to_wkb(*dims).unwrap()))
            .collect::<Vec<_>>();

        for ((wkt, _), geometry) in CASES.iter().zip(&geometries) {
            // geozero must produce DuckDB's canonical WKB.
            assert_eq!(geometry.0, query_geometry(&conn, wkt)?, "{wkt}");

            // Bound values arrive as BLOB: the v2 C API has no GEOMETRY value constructor.
            let mut query = conn.query(
                "SELECT ST_GeomFromWKB($1)::VARCHAR = $2::GEOMETRY::VARCHAR",
                Parameters::Positional(&[geometry, wkt]),
            )?;
            let chunk = query.next_chunk()?.unwrap();
            assert_eq!(chunk.get_vector_at::<bool>(0)?.reader()?.get(0)?, Some(&true), "{wkt}");
        }

        let mut chunk = DataChunk::create(&conn, [GeometryValue::logical_type(&conn)?], true)?;
        let mut vector = chunk.get_vector_at_mut::<GeometryValue>(0)?;
        vector.set_size(geometries.len() + 1)?;
        {
            let mut writer = vector.writer()?;
            for (i, geometry) in geometries.iter().enumerate() {
                writer.set(i, Some(geometry))?;
            }
            writer.set(geometries.len(), None)?;
        }

        let actual = vector
            .reader()?
            .iter()
            .map(|row| row.map(<[u8]>::to_vec))
            .collect::<Vec<_>>();
        let expected = geometries
            .iter()
            .map(|geometry| Some(geometry.0.clone()))
            .chain([None])
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }
}
