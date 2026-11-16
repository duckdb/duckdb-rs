// Usage:
// cargo run -p duckdb-neo --example repl
// cargo run -p duckdb-neo --example repl path/to/database.db
// cargo run -p duckdb-neo --example repl -- -c "SELECT 1"
// cargo run -p duckdb-neo --example repl path/to/database.db -c "SELECT 1" -c "SELECT 2"
// cat example.sql | cargo run -p duckdb-neo --example repl

use duckdb_neo::{
    Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
    query_result::ResultType,
};
use rustyline::{Config, Editor, error::ReadlineError, history::DefaultHistory};

const HISTORY_FILE: &str = ".duckdb_neo_history";

struct SqlRepl {
    conn: Connection,
    editor: Editor<(), DefaultHistory>,
}

impl SqlRepl {
    fn new(path: Option<String>) -> Result<Self> {
        let location = path.map_or(StorageLocation::InMemory, StorageLocation::OnDisk);
        let conn = Environment::new()?.open(location)?.connect()?;
        let config = Config::builder().auto_add_history(true).build();
        let mut editor = Editor::with_config(config).expect("Failed to create editor");
        let _ = editor.load_history(HISTORY_FILE); // History might not exist yet
        Ok(SqlRepl { conn, editor })
    }

    fn run(&mut self) {
        println!("duckdb-neo v{} 🦀", env!("CARGO_PKG_VERSION"));
        println!("Type '.help' for help.");

        loop {
            match self.editor.readline("> ") {
                Ok(line) => match line.trim() {
                    "" => {}
                    ".quit" => break,
                    ".help" => show_help(),
                    ".schema" => self.execute_and_report("SELECT sql FROM sqlite_master WHERE type='table'"),
                    ".tables" => self.execute_and_report("SHOW TABLES"),
                    sql => self.execute_and_report(sql),
                },
                Err(ReadlineError::Interrupted) => {}
                Err(ReadlineError::Eof) => break,
                Err(err) => {
                    eprintln!("Error: {err}");
                    break;
                }
            }
        }

        if let Err(e) = self.editor.save_history(HISTORY_FILE) {
            eprintln!("Warning: Failed to save history: {e}");
        }
    }

    fn execute_and_report(&self, sql: &str) {
        if let Err(e) = self.execute_sql(sql) {
            eprintln!("{e}");
        }
    }

    fn execute_sql(&self, sql: &str) -> Result<()> {
        for statement in self.conn.parse(sql)? {
            let mut result = self.conn.query(statement?, Parameters::None)?;
            if result.result_type()? == ResultType::Nothing {
                result.drain()?;
            } else {
                print!("{}", result.to_text(0, 0, 0, 0, 0)?);
            }
        }
        Ok(())
    }
}

fn show_help() {
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
            "-c" => match args.next() {
                Some(sql) => commands.push(sql),
                None => {
                    eprintln!("Error: -c requires a SQL argument");
                    std::process::exit(1);
                }
            },
            _ if db_path.is_none() && !arg.starts_with('-') => db_path = Some(arg),
            _ => {
                eprintln!("Error: unexpected argument '{arg}'");
                std::process::exit(1);
            }
        }
    }

    Args { db_path, commands }
}

fn main() -> Result<()> {
    let args = parse_args();
    let mut repl = SqlRepl::new(args.db_path)?;

    if args.commands.is_empty() {
        repl.run();
    } else {
        for sql in &args.commands {
            repl.execute_sql(sql)?;
        }
    }
    Ok(())
}
