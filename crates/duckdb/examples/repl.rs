// Usage:
// cargo run --example repl
// cargo run --example repl path/to/database.db

use duckdb::{arrow::record_batch::RecordBatch, Connection, Result as DuckResult};
use rustyline::{error::ReadlineError, history::DefaultHistory, Config, Editor};

const HISTORY_FILE: &str = ".duckdb_rs_history";

struct SqlRepl {
    conn: Connection,
    editor: Editor<(), DefaultHistory>,
}

impl SqlRepl {
    fn new() -> DuckResult<Self> {
        Self::new_with_file(":memory:")
    }

    fn new_with_file(path: &str) -> DuckResult<Self> {
        let conn = Connection::open(path)?;
        let editor = {
            let config = Config::builder().auto_add_history(true).build();
            let mut editor = Editor::with_config(config).expect("Failed to create editor");
            let _ = editor.load_history(HISTORY_FILE); // Load history, might not exist yet
            editor
        };
        Ok(SqlRepl { conn, editor })
    }

    fn run(&mut self) -> DuckResult<()> {
        println!("duckdb-rs v{} 🦀", env!("CARGO_PKG_VERSION"));
        println!("Type '.help' for help.");

        loop {
            match self.editor.readline("> ") {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    match line {
                        ".quit" => break,
                        ".help" => self.show_help(),
                        ".schema" => {
                            if let Err(e) = self.show_schema() {
                                eprintln!("Error showing schema: {e}");
                            }
                        }
                        ".tables" => {
                            if let Err(e) = self.show_tables() {
                                eprintln!("Error showing tables: {e}");
                            }
                        }
                        _ => {
                            if let Err(e) = self.execute_sql(line) {
                                eprintln!("{e}");
                            }
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    break;
                }
                Err(err) => {
                    eprintln!("Error: {err}");
                    break;
                }
            }
        }

        if let Err(e) = self.editor.save_history(HISTORY_FILE) {
            eprintln!("Warning: Failed to save history: {e}");
        }

        Ok(())
    }

    fn show_help(&self) {
        println!("Available commands:");
        println!("  .help      - Show this help message");
        println!("  .quit      - Exit the REPL");
        println!("  .schema    - Show database schema");
        println!("  .tables    - Show all tables");
        println!();
        println!("Keyboard shortcuts:");
        println!("  Up/Down    - Navigate command history");
        println!("  Ctrl+R     - Search command history");
        println!("  Ctrl+C     - Cancel current input");
        println!("  Ctrl+D     - Exit REPL");
        println!();
        println!("Enter any SQL statement to execute it.");
        println!();
        println!("Examples:");
        println!("  SELECT 1 + 1;");
        println!("  CREATE TABLE test (id INTEGER, name TEXT);");
        println!("  INSERT INTO test VALUES (1, 'hello');");
        println!("  SELECT * FROM test;");
    }

    fn show_schema(&self) -> DuckResult<()> {
        let mut stmt = self.conn.prepare("SELECT sql FROM sqlite_master WHERE type='table'")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

        if rbs.is_empty() || rbs[0].num_rows() == 0 {
            println!("No tables found in database.");
        } else {
            print_records(&rbs);
        }

        Ok(())
    }

    fn show_tables(&self) -> DuckResult<()> {
        let mut stmt = self.conn.prepare("SHOW TABLES")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

        if rbs.is_empty() || rbs[0].num_rows() == 0 {
            println!("No tables found in database.");
        } else {
            print_records(&rbs);
        }

        Ok(())
    }

    fn execute_sql(&self, sql: &str) -> DuckResult<()> {
        // Check if it's a statement that returns results
        let sql_upper = sql.trim().to_uppercase();
        let is_query = sql_upper.starts_with("SELECT")
            || sql_upper.starts_with("FROM")
            || sql_upper.starts_with("SHOW")
            || sql_upper.starts_with("DESCRIBE")
            || sql_upper.starts_with("EXPLAIN")
            || sql_upper.starts_with("PRAGMA")
            || sql_upper.starts_with("WITH");

        if is_query {
            let mut stmt = self.conn.prepare(sql)?;
            let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

            if rbs.is_empty() || rbs[0].num_rows() == 0 {
                println!("No results returned.");
            } else {
                print_records(&rbs);
            }
        } else {
            // Execute non-query statements
            self.conn.execute_batch(sql)?;
        }

        Ok(())
    }
}

fn print_records(rbs: &[RecordBatch]) {
    let options = arrow::util::display::FormatOptions::default()
        .with_display_error(true)
        .with_types_info(true);
    let str = arrow::util::pretty::pretty_format_batches_with_options(rbs, &options).unwrap();
    println!("{str}");
}

fn main() -> DuckResult<()> {
    let args: Vec<String> = std::env::args().collect();

    let mut repl = if args.len() > 1 {
        let db_path = &args[1];
        SqlRepl::new_with_file(db_path)?
    } else {
        SqlRepl::new()?
    };

    repl.run()
}
