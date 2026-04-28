// Usage:
// cargo run --example repl
// cargo run --example repl path/to/database.db
// cargo run --example repl -- -c "SELECT 1"
// cargo run --example repl path/to/database.db -c "SELECT 1" -c "SELECT 2"
// cat example.sql | cargo run --example repl

use duckdb::{Connection, Result as DuckResult, arrow::record_batch::RecordBatch};
use rustyline::{Config, Editor, error::ReadlineError, history::DefaultHistory};

const HISTORY_FILE: &str = ".duckdb_rs_history";

struct SqlRepl {
    conn: Connection,
    editor: Editor<(), DefaultHistory>,
    debug: bool,
}

impl SqlRepl {
    fn new(path: Option<&str>) -> DuckResult<Self> {
        let conn = Connection::open(path.unwrap_or(":memory:"))?;
        let editor = {
            let config = Config::builder().auto_add_history(true).build();
            let mut editor = Editor::with_config(config).expect("Failed to create editor");
            let _ = editor.load_history(HISTORY_FILE); // History might not exist yet
            editor
        };
        Ok(SqlRepl {
            conn,
            editor,
            debug: false,
        })
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
                        ".debug" => self.debug = !self.debug,
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
        println!("  .debug     - Toggle debug output");
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
            self.print_records(&rbs);
        }

        Ok(())
    }

    fn show_tables(&self) -> DuckResult<()> {
        let mut stmt = self.conn.prepare("SHOW TABLES")?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

        if rbs.is_empty() || rbs[0].num_rows() == 0 {
            println!("No tables found in database.");
        } else {
            self.print_records(&rbs);
        }

        Ok(())
    }

    fn execute_sql(&self, sql: &str) -> DuckResult<()> {
        let mut stmt = self.conn.prepare(sql)?;
        let rbs: Vec<RecordBatch> = stmt.query_arrow([])?.collect();

        // NOTE: When executing multi-statement queries (e.g., "SELECT 1; SELECT 2;"),
        // only the result from the final statement will be displayed. This differs from
        // the DuckDB CLI which shows results from all statements.
        if !rbs.is_empty() && rbs[0].num_rows() > 0 {
            self.print_records(&rbs);
        }

        Ok(())
    }

    fn print_records(&self, rbs: &[RecordBatch]) {
        let options = arrow::util::display::FormatOptions::default()
            .with_display_error(true)
            .with_types_info(true);
        let table = arrow::util::pretty::pretty_format_batches_with_options(rbs, &options).unwrap();
        println!("{table}");

        if self.debug {
            dbg!(rbs);
        }
    }
}

struct Args {
    db_path: Option<String>,
    commands: Vec<String>,
}

fn parse_args() -> Args {
    let mut args = std::env::args().skip(1);
    let mut db_path = None;
    let mut commands = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-c" => {
                if let Some(sql) = args.next() {
                    commands.push(sql);
                } else {
                    eprintln!("Error: -c requires a SQL argument");
                    std::process::exit(1);
                }
            }
            _ => {
                if db_path.is_none() && !arg.starts_with('-') {
                    db_path = Some(arg);
                } else {
                    eprintln!("Error: unexpected argument '{arg}'");
                    std::process::exit(1);
                }
            }
        }
    }

    Args { db_path, commands }
}

fn main() -> DuckResult<()> {
    let args = parse_args();
    let mut repl = SqlRepl::new(args.db_path.as_deref())?;

    if args.commands.is_empty() {
        repl.run()
    } else {
        for sql in &args.commands {
            repl.execute_sql(sql)?;
        }
        Ok(())
    }
}
