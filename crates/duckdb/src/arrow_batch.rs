use super::{
    Statement,
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
};

/// A handle for iterating the [`RecordBatch`]es of a query result.
///
/// The iterator is lazy: each batch is fetched only as the iterator is
/// advanced. Whether the full result is materialized up front or fetched
/// lazily in chunks depends on how the statement was executed — see
/// [`query_arrow`](Statement::query_arrow) and
/// [`stream_arrow`](Statement::stream_arrow).
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

    /// Return the Arrow schema reported by DuckDB after execution.
    #[inline]
    pub fn get_schema(&self) -> SchemaRef {
        self.stmt
            .expect("Arrow iterator always holds a statement")
            .stmt
            .schema()
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

/// A handle for the resulting RecordBatch of a streaming query.
///
/// Identical to [`Arrow`]: the streaming behavior comes from how the
/// statement was executed, not from the iterator type.
pub type ArrowStream<'stmt> = Arrow<'stmt>;
