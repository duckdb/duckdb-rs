use duckdb_rs::{
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

    let csv_data = conn.query(
        "SELECT id, first_name, email, birth_day, birth_month, birth_year, FROM 'duckdb-rs/examples/001-json-to-duckdb-db/data/001.csv' WHERE country != $1",
        Parameters::positional(&[&"CHILE"]),
    )?;

    let mut customers = vec![];

    for chunk in csv_data {
        let chunk = chunk?;

        let id = chunk.get_vector_at::<i64>(0)?;
        let name = chunk.get_vector_at::<String>(1)?;
        let email = chunk.get_vector_at::<String>(2)?;
        let birth_day = chunk.get_vector_at::<i64>(3)?;
        let birth_month = chunk.get_vector_at::<i64>(4)?;
        let birth_year = chunk.get_vector_at::<i64>(5)?;

        for i in 0..chunk.row_count()? {
            let birthday = DateValue(
                ((*birth_year.get(i)?.unwrap() - 1970) * 365i64
                    + *birth_month.get(i)?.unwrap() * 30
                    + *birth_day.get(i)?.unwrap()) as i32,
            );

            let customer = Customer {
                id: *id.get(i)?.unwrap(),
                name: name.get(i)?.unwrap().to_string(),
                email: email.get(i)?.unwrap().to_string(),
                birthday,
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

    let db_customers = conn.query("SELECT * FROM customers", Parameters::None)?;

    for chunk in db_customers {
        let chunk = chunk?;

        let id = chunk.get_vector_at::<i32>(0)?;
        let name = chunk.get_vector_at::<String>(1)?;
        let email = chunk.get_vector_at::<String>(2)?;
        let birthday = chunk.get_vector_at::<DateValue>(3)?;

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
