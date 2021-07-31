use super::Statement;
use arrow::record_batch::RecordBatch;

/// An handle for the resulting RecordBatch of a query.
#[must_use = "Arrow is lazy and will do nothing unless consumed"]
pub struct Arrow<'stmt> {
    pub(crate) stmt: Option<&'stmt Statement<'stmt>>,
}

impl<'stmt> Arrow<'stmt> {
    #[inline]
    pub(crate) fn new(stmt: &'stmt Statement<'stmt>) -> Arrow<'stmt> {
        Arrow { stmt: Some(stmt) }
    }
}

impl<'stmt> Iterator for Arrow<'stmt> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stmt.is_none() {
            return None;
        }
        let arr = self.stmt.unwrap().step();
        if arr.is_none() {
            return None;
        }
        Some(RecordBatch::from(&arr.unwrap()))
    }
}
