use duckdb_neo::{
    Parameters, Result,
    environment::{Environment, StorageLocation},
    types::DateValue,
};

struct Customer {
    id: i64,
    name: String,
    email: String,
    birthday: DateValue,
}

// Absolute paths so the example runs from any directory.
const CSV_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/001-json-to-duckdb-db/data/001.csv"
);

fn main() -> Result<()> {
    let db_path = std::env::temp_dir().join("duckdb-neo-001-example.db");
    std::fs::remove_file(&db_path).ok();

    let env = Environment::new()?;
    let instance = env.open(StorageLocation::OnDisk(db_path.to_string_lossy().into_owned()));
    let conn = instance?.connect()?;

    conn.execute(
        "CREATE TABLE customers (id INTEGER, first_name VARCHAR, email VARCHAR, birthday DATE);",
        Parameters::None,
    )?;

    let csv_query = format!(
        "SELECT id, first_name, email, make_date(birth_year, birth_month, birth_day) FROM '{CSV_PATH}' WHERE country != $1"
    );
    let mut csv_data = conn.query(csv_query.as_str(), Parameters::positional(&[&"CHILE"]))?;

    let mut customers = vec![];

    while let Some(chunk) = csv_data.next_chunk()? {
        let id = chunk.get_vector_at::<i64>(0)?;
        let name = chunk.get_vector_at::<String>(1)?;
        let email = chunk.get_vector_at::<String>(2)?;
        // `make_date` handles leap years and month lengths.
        let birthday = chunk.get_vector_at::<DateValue>(3)?;

        for i in 0..chunk.row_count()? {
            let customer = Customer {
                id: *id.get(i)?.unwrap(),
                name: name.get(i)?.unwrap().to_string(),
                email: email.get(i)?.unwrap().to_string(),
                birthday: *birthday.get(i)?.unwrap(),
            };

            customers.push(customer);
        }
    }

    // TODO: This will be replaced with a ColumnData insert.
    for customer in customers {
        conn.execute(
            "INSERT INTO customers VALUES ($1, $2, $3, $4);",
            Parameters::positional(&[&customer.id, &customer.name, &customer.email, &customer.birthday]),
        )?;
    }

    let mut db_customers = conn.query(
        "SELECT id, first_name, email, birthday::VARCHAR FROM customers",
        Parameters::None,
    )?;

    while let Some(chunk) = db_customers.next_chunk()? {
        let id = chunk.get_vector_at::<i32>(0)?;
        let name = chunk.get_vector_at::<String>(1)?;
        let email = chunk.get_vector_at::<String>(2)?;
        let birthday = chunk.get_vector_at::<String>(3)?;

        for i in 0..chunk.row_count()? {
            println!(
                "Customer: id={}, name={}, email={}, birthday={}",
                id.get(i)?.unwrap(),
                name.get(i)?.unwrap(),
                email.get(i)?.unwrap(),
                birthday.get(i)?.unwrap()
            );
        }
    }

    Ok(())
}
