//! Generate series table function.
//!
//! Port of C [generate series "function"](http://www.sqlite.org/cgi/src/finfo?name=ext/misc/series.c):
//! `https://www.sqlite.org/series.html`
use super::ffi::duckdb_free;
use super::malloc_struct;
use super::{BindInfo, DataChunk, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab};
use crate::vtab::vector::Inserter;
use calamine::{open_workbook_auto, DataType, Range, Reader};
use std::ffi::c_void;

#[repr(C)]
struct ExcelBindData {
    range: *mut Range<DataType>,
    start: usize,
    width: usize,
    height: usize,
}

/// Drop the ExcelBindData from C.
///
/// # Safety
unsafe extern "C" fn drop_excel_bind_data_c(v: *mut c_void) {
    let actual = v.cast::<ExcelBindData>();
    drop(Box::from_raw((*actual).range));
    duckdb_free(v);
}

#[repr(C)]
struct ExcelInitData {
    start: usize,
}

/// Drop the ExcelBindData from C.
///
/// # Safety
unsafe extern "C" fn drop_excel_init_data_c(v: *mut c_void) {
    duckdb_free(v);
}

struct ExcelVTab;

impl VTab for ExcelVTab {
    fn bind(bind: &BindInfo) -> Result<(), Box<dyn std::error::Error>> {
        let param_count = bind.get_parameter_count();
        assert!(param_count == 2);
        let path = bind.get_parameter(0).to_string();
        let sheet = bind.get_parameter(1).to_string();
        // let sheet = if param_count > 1 {
        //     bind.get_parameter(1).to_string()
        // } else {
        //     workbook.sheet_names()[0].to_owned()
        // };
        let mut workbook = open_workbook_auto(path)?;
        let range = workbook
            .worksheet_range(&sheet)
            .expect(&format!("Can't find sheet: {} ?", sheet))?;
        let column_count = range.get_size().1;
        let mut rows = range.rows();
        let mut start = 0;
        while let Some(header) = rows.next() {
            start += 1;
            while let Some(data) = rows.next() {
                start += 1;
                let mut idx = 0;
                let mut should_break = true;
                for i in 0..column_count {
                    let cell = &data[i];
                    match cell {
                        DataType::String(_) => {
                            bind.add_result_column(
                                header[idx].get_string().expect(&format!("idx {} header empty?", idx)),
                                LogicalType::new(LogicalTypeId::Varchar),
                            );
                        }
                        DataType::Float(_) => {
                            bind.add_result_column(
                                header[idx].get_string().expect(&format!("idx {} header empty?", idx)),
                                LogicalType::new(LogicalTypeId::Double),
                            );
                        }
                        DataType::Int(_) => {
                            bind.add_result_column(
                                header[idx].get_string().expect(&format!("idx {} header empty?", idx)),
                                LogicalType::new(LogicalTypeId::Bigint),
                            );
                        }
                        DataType::Bool(_) => {
                            bind.add_result_column(
                                header[idx].get_string().expect(&format!("idx {} header empty?", idx)),
                                LogicalType::new(LogicalTypeId::Boolean),
                            );
                        }
                        DataType::DateTime(_) => {
                            bind.add_result_column(
                                header[idx].get_string().expect(&format!("idx {} header empty?", idx)),
                                LogicalType::new(LogicalTypeId::Date),
                            );
                        }
                        _ => {
                            should_break = false;
                            break;
                        }
                    }
                    idx += 1;
                }
                if should_break {
                    break;
                }
            }
            break;
        }

        unsafe {
            let data = malloc_struct::<ExcelBindData>();
            (*data).start = start - 1;
            (*data).width = range.get_size().1;
            (*data).height = range.get_size().0;
            (*data).range = Box::into_raw(Box::new(range));
            bind.set_bind_data(data.cast(), Some(drop_excel_bind_data_c));
        }
        Ok(())
    }

    fn init(init: &InitInfo) -> Result<(), Box<dyn std::error::Error>> {
        let bind_info = init.get_bind_data::<ExcelBindData>();
        unsafe {
            let data = malloc_struct::<ExcelInitData>();
            (*data).start = (*bind_info).start;
            init.set_init_data(data.cast(), Some(drop_excel_init_data_c));
        }
        Ok(())
    }

    fn func(func: &FunctionInfo, output: &DataChunk) -> Result<(), Box<dyn std::error::Error>> {
        let init_info = func.get_init_data::<ExcelInitData>();
        let bind_info = func.get_bind_data::<ExcelBindData>();
        unsafe {
            if (*init_info).start >= (*bind_info).height {
                output.set_len(0);
            } else {
                let range = (*bind_info).range.as_ref().expect("range is null");
                let height = std::cmp::min(
                    output.flat_vector(0).capacity(),
                    (*bind_info).height - (*init_info).start,
                );
                for i in 0..(*bind_info).width {
                    let mut vector = output.flat_vector(i);
                    for j in 0..height {
                        let cell = range.get_value((((*init_info).start + j) as u32, i as u32));
                        if cell.is_none() {
                            continue;
                        }
                        match cell.unwrap() {
                            DataType::String(s) => {
                                vector.insert(j, s.as_str());
                            }
                            DataType::Float(f) => {
                                vector.as_mut_slice::<f64>()[j] = *f;
                            }
                            DataType::Int(ii) => {
                                vector.as_mut_slice::<i64>()[j] = *ii;
                            }
                            DataType::Bool(b) => {
                                vector.as_mut_slice::<bool>()[j] = *b;
                            }
                            DataType::DateTime(d) => {
                                vector.as_mut_slice::<i32>()[j] = d.round() as i32 - 25569;
                            }
                            _ => {
                                //
                            }
                        }
                    }
                }
                (*init_info).start += height;
                output.set_len(height);
            }
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalType>> {
        Some(vec![
            LogicalType::new(LogicalTypeId::Varchar), // file path
            LogicalType::new(LogicalTypeId::Varchar), // sheet name
        ])
    }
}

#[cfg(test)]
mod test {
    use crate::{vtab::excel::ExcelVTab, Connection, Result};
    use arrow::array::{Array, Date32Array, Float64Array, StringArray};
    use std::error::Error;

    #[test]
    fn test_excel() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ExcelVTab>("excel")?;

        let val = db
            .prepare("select count(*) from excel('./examples/Movies_Social_metadata.xlsx', 'Data')")?
            .query_row::<i64, _, _>([], |row| row.get(0))?;
        assert_eq!(3039, val);
        let mut stmt = db.prepare("select genres, sum(movie_facebook_likes) from excel('./examples/Movies_Social_metadata.xlsx', 'Data') group by genres order by genres limit 4")?;
        let mut arr = stmt.query_arrow([])?;
        // +-------------+---------------------------+
        // | genres      | sum(movie_facebook_likes) |
        // +-------------+---------------------------+
        // | Action      | 9773520.0                 |
        // | Adventure   | 4355937.0                 |
        // | Animation   | 202219.0                  |
        // | Biography   | 1724632.0                 |
        // +-------------+---------------------------+
        let rb = arr.next().expect("no record batch");
        let column = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(column.len(), 4);
        assert_eq!(column.value(0), "Action");
        assert_eq!(column.value(1), "Adventure");
        assert_eq!(column.value(2), "Animation");
        assert_eq!(column.value(3), "Biography");
        let column = rb.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(column.len(), 4);
        assert_eq!(column.value(0), 9773520.0);
        assert_eq!(column.value(1), 4355937.0);
        assert_eq!(column.value(2), 202219.0);
        assert_eq!(column.value(3), 1724632.0);
        assert!(arr.next().is_none());

        Ok(())
    }

    #[test]
    fn test_excel_date() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ExcelVTab>("excel")?;
        let mut stmt = db.prepare("select * from excel('./examples/date.xlsx', 'Sheet1')")?;
        let mut arr = stmt.query_arrow([])?;
        let rb = arr.next().expect("no record batch");
        let column = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(column.len(), 2);
        assert_eq!(column.value_as_date(0).unwrap().to_string(), "2021-01-01");
        assert_eq!(column.value_as_date(1).unwrap().to_string(), "2021-01-02");
        let column = rb.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(column.len(), 2);
        assert_eq!(column.value(0), 15.0);
        assert_eq!(column.value(1), 16.0);
        assert!(arr.next().is_none());

        Ok(())
    }
}
