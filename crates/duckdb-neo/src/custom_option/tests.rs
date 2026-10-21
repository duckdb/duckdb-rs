use std::sync::{Arc, Mutex};

use crate::{
    DuckDBType, FromValue, Parameters, ToValue,
    connection::Connection,
    environment::{Environment, StorageLocation},
    error::Error,
};

use super::*;

/// One call to [`Clamp::update`].
type Update = (Option<i64>, SettingScope, bool);

/// Caps the option at 100 and refuses negative values, recording every update.
struct Clamp {
    updates: Arc<Mutex<Vec<Update>>>,
}

impl CustomOptionCallbacks for Clamp {
    fn update(&self, context: &Context, value: Value, scope: SettingScope, is_reset: bool) -> Result<Value> {
        let number = i64::from_value(&value)?;
        self.updates.lock().unwrap().push((number, scope, is_reset));
        match number {
            Some(n) if n < 0 => Err(Error::invalid_input("rs_limit must not be negative")),
            Some(n) if n > 100 => 100_i64.value(context),
            _ => Ok(value),
        }
    }
}

fn setting(conn: &Connection, name: &str) -> Result<Option<i64>> {
    let mut result = conn.query(
        format!("SELECT current_setting('{name}')::BIGINT").as_str(),
        Parameters::None,
    )?;
    let chunk = result.next_chunk()?.expect("expected a result chunk");
    Ok(chunk.get_vector_at::<i64>(0)?.reader()?.get(0)?.copied())
}

#[test]
fn test_custom_option() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;

    let updates = Arc::new(Mutex::new(Vec::new()));
    CustomOptionBuilder::new(
        "rs_limit",
        "A limit between 0 and 100",
        i64::logical_type(&conn)?,
        Clamp {
            updates: updates.clone(),
        },
    )
    .default_value(10_i64.value(&conn)?)
    .register(&conn)?;

    // The default value does not go through the update callback.
    assert_eq!(setting(&conn, "rs_limit")?, Some(10));
    assert!(updates.lock().unwrap().is_empty());

    conn.execute("SET rs_limit = 42", Parameters::None)?;
    assert_eq!(setting(&conn, "rs_limit")?, Some(42));

    // The callback replaces the value.
    conn.execute("SET rs_limit = 500", Parameters::None)?;
    assert_eq!(setting(&conn, "rs_limit")?, Some(100));

    // The callback refuses the value, which leaves the option unchanged.
    let error = conn.execute("SET rs_limit = -1", Parameters::None).unwrap_err();
    assert!(error.message.contains("must not be negative"), "{error}");
    assert_eq!(setting(&conn, "rs_limit")?, Some(100));

    conn.execute("RESET rs_limit", Parameters::None)?;
    assert_eq!(setting(&conn, "rs_limit")?, Some(10));

    // SET without a scope writes the session.
    assert_eq!(
        *updates.lock().unwrap(),
        [
            (Some(42), SettingScope::Session, false),
            (Some(500), SettingScope::Session, false),
            (Some(-1), SettingScope::Session, false),
            (Some(10), SettingScope::Session, true),
        ]
    );

    let option = conn.config()?.get_by_name("rs_limit")?;
    assert_eq!(option.description()?, "A limit between 0 and 100");
    Ok(())
}

#[test]
fn test_custom_option_scopes() -> Result<()> {
    let env = Environment::new()?;
    let conn = env.open(StorageLocation::InMemory)?.connect()?;

    let updates = Arc::new(Mutex::new(Vec::new()));
    CustomOptionBuilder::new(
        "rs_global_only",
        "Only settable globally",
        i64::logical_type(&conn)?,
        Clamp {
            updates: updates.clone(),
        },
    )
    .no_session()
    .register(&conn)?;

    let option = conn.config()?.get_by_name("rs_global_only")?;
    assert!(option.supports_scope(SettingScope::Global)?);
    assert!(!option.supports_scope(SettingScope::Session)?);

    // Without a default value the option reads as NULL. SQL `current_setting` reports it as unrecognized instead.
    let (value, _scope) = conn.config()?.get_value("rs_global_only")?;
    assert_eq!(i64::from_value(&value)?, None);

    assert!(
        conn.execute("SET SESSION rs_global_only = 1", Parameters::None)
            .is_err()
    );
    conn.execute("SET GLOBAL rs_global_only = 2", Parameters::None)?;
    // Without SESSION support, SET writes the global.
    conn.execute("SET rs_global_only = 3", Parameters::None)?;
    assert_eq!(setting(&conn, "rs_global_only")?, Some(3));

    assert_eq!(
        *updates.lock().unwrap(),
        [
            (Some(2), SettingScope::Global, false),
            (Some(3), SettingScope::Global, false),
        ]
    );

    // Registering a name twice fails.
    let duplicate = CustomOptionBuilder::new(
        "rs_global_only",
        "",
        i64::logical_type(&conn)?,
        Clamp {
            updates: updates.clone(),
        },
    );
    assert!(duplicate.register(&conn).is_err());
    Ok(())
}
