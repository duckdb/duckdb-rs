use super::{BindInfo, DataChunk, Free, FunctionInfo, InitInfo, LogicalType, LogicalTypeId, VTab};
use crate::core::Inserter;
use calamine::{open_workbook_auto, DataType, Range, Reader};

#[repr(C)]
struct ExcelBindData {
    range: *mut Range<DataType>,
    width: usize,
    height: usize,
}

impl Free for ExcelBindData {
    fn free(&mut self) {
        unsafe {
            if self.range.is_null() {
                return;
            }
            drop(Box::from_raw(self.range));
        }
    }
}

#[repr(C)]
struct ExcelInitData {
    start: usize,
}

impl Free for ExcelInitData {}

struct ExcelVTab;

impl VTab for ExcelVTab {
    type BindData = ExcelBindData;
    type InitData = ExcelInitData;

    unsafe fn bind(bind: &BindInfo, data: *mut ExcelBindData) -> Result<(), Box<dyn std::error::Error>> {
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
            .unwrap_or_else(|| panic!("Can't find sheet: {} ?", sheet))?;
        let _column_count = range.get_size().1;
        let mut rows = range.rows();
        let header = rows.next().unwrap();
        for data in rows.by_ref() {
            // find the first row with no empty cell
            let mut found = true;
            for cell in data.iter() {
                match cell {
                    DataType::Error(_) | DataType::Empty => {
                        found = false;
                        break;
                    }
                    _ => {}
                }
            }
            if !found {
                continue;
            }

            // use the first row as data type
            for (idx, cell) in data.iter().enumerate() {
                match cell {
                    DataType::String(_) => {
                        bind.add_result_column(
                            header[idx]
                                .get_string()
                                .unwrap_or_else(|| panic!("idx {} header empty?", idx)),
                            LogicalType::new(LogicalTypeId::Varchar),
                        );
                    }
                    DataType::Float(_) => {
                        bind.add_result_column(
                            header[idx]
                                .get_string()
                                .unwrap_or_else(|| panic!("idx {} header empty?", idx)),
                            LogicalType::new(LogicalTypeId::Double),
                        );
                    }
                    DataType::Int(_) => {
                        bind.add_result_column(
                            header[idx]
                                .get_string()
                                .unwrap_or_else(|| panic!("idx {} header empty?", idx)),
                            LogicalType::new(LogicalTypeId::Bigint),
                        );
                    }
                    DataType::Bool(_) => {
                        bind.add_result_column(
                            header[idx]
                                .get_string()
                                .unwrap_or_else(|| panic!("idx {} header empty?", idx)),
                            LogicalType::new(LogicalTypeId::Boolean),
                        );
                    }
                    DataType::DateTime(_) => {
                        bind.add_result_column(
                            header[idx]
                                .get_string()
                                .unwrap_or_else(|| panic!("idx {} header empty?", idx)),
                            LogicalType::new(LogicalTypeId::Date),
                        );
                    }
                    _ => {
                        panic!("Shouldn't happen");
                    }
                }
            }
            break;
        }

        unsafe {
            (*data).width = range.get_size().1;
            (*data).height = range.get_size().0;
            (*data).range = Box::into_raw(Box::new(range));
        }
        Ok(())
    }

    unsafe fn init(_: &InitInfo, data: *mut ExcelInitData) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            (*data).start = 1;
        }
        Ok(())
    }

    unsafe fn func(func: &FunctionInfo, output: &mut DataChunk) -> Result<(), Box<dyn std::error::Error>> {
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
                        let cell = range.get(((*init_info).start + j, i));
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
                                vector.set_null(j);
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
        // +-------------+---------------------------+
        // | genres      | sum(movie_facebook_likes) |
        // +-------------+---------------------------+
        // | Action      | 9773520.0                 |
        // | Adventure   | 4355937.0                 |
        // | Animation   | 202219.0                  |
        // | Biography   | 1724632.0                 |
        // +-------------+---------------------------+
        let mut arr = stmt.query_arrow([])?;
        let rb = arr.next().expect("no record batch");
        assert_eq!(rb.num_rows(), 4);
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

    #[test]
    fn test_excel_with_empty_row() -> Result<(), Box<dyn Error>> {
        let db = Connection::open_in_memory()?;
        db.register_table_function::<ExcelVTab>("excel")?;

        // use arrow::record_batch::RecordBatch;
        // use arrow::util::pretty::print_batches;
        // let val: Vec<RecordBatch> = db.prepare("select * from excel('./examples/date.xlsx', 'Sheet2')")?.query_arrow([])?.collect();
        // print_batches(&val)?;

        let mut stmt = db.prepare("select * from excel('./examples/date.xlsx', 'Sheet2')")?;
        let mut arr = stmt.query_arrow([])?;
        let rb = arr.next().expect("no record batch");
        let column = rb.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        assert_eq!(column.len(), 3);
        assert!(column.is_null(0));
        assert_eq!(column.value_as_date(1).unwrap().to_string(), "2021-01-01");
        assert_eq!(column.value_as_date(2).unwrap().to_string(), "2021-01-02");
        let column = rb.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(column.len(), 3);
        assert!(column.is_null(0));
        assert_eq!(column.value(1), 15.0);
        assert_eq!(column.value(2), 16.0);
        assert!(arr.next().is_none());
        Ok(())
    }
}
