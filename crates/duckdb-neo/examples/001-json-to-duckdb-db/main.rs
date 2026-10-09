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

fn main() -> Result<()> {
    std::fs::remove_file("001-example.db").ok();

    let env = Environment::new()?;
    let db = env.open(StorageLocation::OnDisk("001-example.db".to_string()));
    let conn = db?.connect()?;

    conn.execute(
        "CREATE TABLE customers (id INTEGER, first_name VARCHAR, email VARCHAR, birthday DATE);",
        Parameters::None,
    )?;

    let mut csv_data = conn.query(
        "SELECT id, first_name, email, make_date(birth_year, birth_month, birth_day) FROM 'crates/duckdb-neo/examples/001-json-to-duckdb-db/data/001.csv' WHERE country != $1",
        Parameters::positional(&[&"CHILE"]),
    )?;

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
