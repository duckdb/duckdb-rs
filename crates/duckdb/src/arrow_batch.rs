use super::{
    Statement,
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
};

/// A handle for the resulting RecordBatch of a query.
///
/// The iterator panics if DuckDB fails to fetch a result chunk or convert it
/// to Arrow.
#[must_use = "Arrow is lazy and will do nothing unless consumed"]
pub struct Arrow<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
}

#[allow(clippy::needless_lifetimes)]
impl<'stmt> Arrow<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Self {
        Arrow { stmt: Some(stmt) }
    }

    /// return arrow schema
    #[inline]
    pub fn get_schema(&self) -> SchemaRef {
        self.stmt.unwrap().stmt.schema()
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'stmt> Iterator for Arrow<'stmt> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stmt?.step() {
            Ok(Some(array)) => Some(RecordBatch::from(&array)),
            Ok(None) => None,
            Err(err) => panic!("Failed to fetch Arrow record batch: {err}"),
        }
    }
}

/// A handle for the resulting RecordBatch of a query in streaming.
///
/// The iterator panics if DuckDB fails to fetch a result chunk or convert it
/// to Arrow.
#[must_use = "Arrow stream is lazy and will not fetch data unless consumed"]
#[allow(clippy::needless_lifetimes)]
pub struct ArrowStream<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
}

#[allow(clippy::needless_lifetimes)]
impl<'stmt> ArrowStream<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Self {
        ArrowStream { stmt: Some(stmt) }
    }

    /// Return the Arrow schema reported by DuckDB after execution.
    #[inline]
    pub fn get_schema(&self) -> SchemaRef {
        self.stmt.unwrap().stmt.schema()
    }
}

#[allow(clippy::needless_lifetimes)]
impl<'stmt> Iterator for ArrowStream<'stmt> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stmt?.step() {
            Ok(Some(array)) => Some(RecordBatch::from(&array)),
            Ok(None) => None,
            Err(err) => panic!("Failed to fetch streaming Arrow record batch: {err}"),
        }
    }
}
