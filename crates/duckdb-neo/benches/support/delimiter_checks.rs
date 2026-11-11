#[test]
fn separators_match_reference() {
    use super::*;
    fn reference(data: &[u8], rows: Range<usize>, columns: usize) -> Result<Vec<Range<usize>>> {
        let mut fields = Vec::new();
        let mut line_start = rows.start;
        for line in data[rows].split_inclusive(|&byte| byte == ROW_SEPARATOR) {
            let content = line.strip_suffix(&[ROW_SEPARATOR]).unwrap_or(line);
            let mut field_start = line_start;
            let mut count = 0;
            for field in content.split(|&byte| byte == FIELD_SEPARATOR) {
                fields.push(field_start..field_start + field.len());
                field_start += field.len() + 1;
                count += 1;
            }
            if count != columns {
                return Err(Error::invalid_input("wrong field count"));
            }
            line_start += line.len();
        }
        Ok(fields)
    }

    for len in 0..=7 {
        for mut code in 0..3usize.pow(len) {
            let mut input = Vec::new();
            for _ in 0..len {
                input.push(b"a,\n"[code % 3]);
                code /= 3;
            }
            for prefix in [0, 3] {
                let mut data = vec![b'x'; prefix];
                data.extend_from_slice(&input);
                let range = prefix..data.len();
                data.extend_from_slice(b"\nignored,tail");
                for columns in 1..=4 {
                    let expected = reference(&data, range.clone(), columns);
                    let mut fields = Vec::new();
                    let actual = split_fields(&data, range.clone(), columns, &mut fields);
                    assert_eq!(actual.is_ok(), expected.is_ok(), "{input:?}, {columns}");
                    if let Ok(expected) = expected {
                        assert_eq!(fields, expected, "{input:?}, {columns}");
                    }
                }
            }
        }
    }
}
