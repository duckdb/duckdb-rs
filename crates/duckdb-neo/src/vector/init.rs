//! Initialization of rows a resize exposes, so safe reads never see uninitialized storage.

use super::{NativeElement, Vector, out_of_bounds};
use crate::{
    Result, check_api_call, ffi,
    logical_type::{LogicalType, LogicalTypeID},
    types::{DecimalSignature, array::array_size},
};

/// How rows of one vector are initialized after it grows.
enum Fill {
    /// Zero `size` bytes per row: a valid `0`, `''`, `[]` or first enum member.
    Zero(usize),
    /// Fill each child over `size` rows per row: 1 for struct fields, the length for array elements.
    Children(usize),
    /// Make each row the first member: a zero tag and first member, and NULL other members.
    Union,
    /// Mark rows NULL, for types whose zeroed value is not a valid DuckDB value.
    Null,
}

fn fill(logical_type: &LogicalType) -> Result<Fill> {
    use LogicalTypeID::*;
    Ok(match logical_type.id()? {
        Boolean | TinyInt | UTinyInt => Fill::Zero(1),
        SmallInt | USmallInt => Fill::Zero(2),
        Integer | UInteger | Float | Date | SqlNull => Fill::Zero(4),
        BigInt | UBigInt | Double | Time | TimeNs | TimeTz | Timestamp | TimestampSec | TimestampMs | TimestampNs
        | TimestampTz | TimestampTzNs => Fill::Zero(8),
        HugeInt | UHugeInt | Uuid | Interval | Varchar | Blob | List | Map => Fill::Zero(16),
        Decimal => Fill::Zero(match DecimalSignature::from_logical_type(logical_type)?.width {
            ..=4 => 2,
            5..=9 => 4,
            10..=18 => 8,
            _ => 16,
        }),
        Enum => match logical_type.param_count()? {
            0 => Fill::Null,
            1..=0xFF => Fill::Zero(1),
            0x100..=0xFFFF => Fill::Zero(2),
            _ => Fill::Zero(4),
        },
        Struct | Tuple => Fill::Children(1),
        Union => Fill::Union,
        Array => Fill::Children(array_size(logical_type)?),
        _ => Fill::Null,
    })
}

impl<T: NativeElement> Vector<'_, T> {
    /// Initialize rows `start..end` and their descendants after they were reserved.
    ///
    /// Rows keep their validity, so a valid row reads as a zeroed value. `LIST` and `MAP` rows
    /// become empty and reference no child rows.
    pub(super) fn init_rows(&mut self, start: usize, end: usize) -> Result<()> {
        if start >= end {
            return Ok(());
        }
        self.check_rows(end)?;
        match fill(&self.logical_type)? {
            Fill::Zero(size) => {
                let data = check_api_call!(ffi::duckdb_v2_vector_get_data_mutable, self.handle, RET)?.cast::<u8>();
                // SAFETY: the rows were reserved, and zero bytes are a valid value of this physical type.
                unsafe { data.add(start * size).write_bytes(0, (end - start) * size) };
            }
            Fill::Children(size) => {
                if let Some(children) = &mut self.children {
                    for child in children.cached_iter_mut()? {
                        child.init_rows(start * size, end * size)?;
                    }
                }
            }
            Fill::Union => {
                if let Some(children) = &mut self.children {
                    for (index, child) in children.cached_iter_mut()?.enumerate() {
                        match index {
                            0 | 1 => child.init_rows(start, end)?,
                            _ => child.null_rows(start, end)?,
                        }
                    }
                }
            }
            Fill::Null => self.null_rows(start, end)?,
        }
        Ok(())
    }

    /// Mark rows NULL, including the descendants a NULL `STRUCT` or `ARRAY` row requires.
    pub(super) fn null_rows(&mut self, start: usize, end: usize) -> Result<()> {
        if start >= end {
            return Ok(());
        }
        self.check_rows(end)?;
        let validity = check_api_call!(ffi::duckdb_v2_vector_flat_get_validity_mutable, self.handle, RET)?;
        // SAFETY: the mask covers every reserved row.
        unsafe { clear_bits(validity, start, end) };
        // Allocating the mask can replace the cached view's validity pointer.
        self.view.take();
        let size = match self.logical_type.id()? {
            LogicalTypeID::Array => array_size(&self.logical_type)?,
            LogicalTypeID::List | LogicalTypeID::Map => return Ok(()),
            _ => 1,
        };
        if let Some(children) = &mut self.children {
            for child in children.cached_iter_mut()? {
                child.null_rows(start * size, end * size)?;
            }
        }
        Ok(())
    }
}

impl<T: NativeElement> Vector<'_, T> {
    fn check_rows(&self, end: usize) -> Result<()> {
        if end > self.len {
            return Err(out_of_bounds(end - 1, self.len));
        }
        Ok(())
    }
}

/// Clear bits `start..end`, a whole word at a time where possible.
///
/// # Safety
/// The mask must cover bit `end - 1`, and `start < end`.
unsafe fn clear_bits(validity: *mut u64, start: usize, end: usize) {
    let (first, last) = (start / 64, (end - 1) / 64);
    let low = u64::MAX << (start % 64);
    let high = u64::MAX >> (63 - (end - 1) % 64);
    // SAFETY: the caller guarantees the bits are within the mask.
    unsafe {
        if first == last {
            *validity.add(first) &= !(low & high);
            return;
        }
        *validity.add(first) &= !low;
        std::slice::from_raw_parts_mut(validity.add(first + 1), last - first - 1).fill(0);
        *validity.add(last) &= !high;
    }
}
