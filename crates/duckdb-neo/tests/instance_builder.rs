use duckdb_neo::{
    FromValue, Parameters, Result,
    connection::Connection,
    environment::{Environment, StorageLocation},
    instance::{AttachBuilder, Instance, InstanceBuilder},
};

fn current_database(connection: &Connection) -> Result<Option<String>> {
    let chunk = connection
        .query("SELECT current_database()", Parameters::None)?
        .next_chunk()?
        .unwrap();
    Ok(chunk.get_vector_at::<String>(0)?.get(0)?.map(str::to_owned))
}

#[test]
fn new_creates_empty_instance() -> Result<()> {
    let env = Environment::new()?;
    let instance = Instance::new(&env)?;
    assert_eq!(env.instance_count()?, 1);
    let connection = instance.connect()?;
    let chunk = connection
        .query(
            "SELECT count(*) FROM duckdb_databases() WHERE NOT internal",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<i64>(0)?.get(0)?, Some(&0));
    assert!(
        connection
            .execute("CREATE TABLE example (id INTEGER)", Parameters::None)
            .is_err()
    );
    drop(chunk);
    drop(connection);
    drop(instance);
    assert_eq!(env.instance_count()?, 0);
    Ok(())
}

#[test]
fn environment_open_attaches_default() -> Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let connection = instance.connect()?;
    assert_eq!(current_database(&connection)?.as_deref(), Some("memory"));
    connection.execute("CREATE TABLE example (id INTEGER)", Parameters::None)?;
    Ok(())
}

#[test]
fn instance_builder_applies_startup_options() -> Result<()> {
    let env = Environment::new()?;
    let builder = InstanceBuilder::new()
        .option("threads", "1")
        .options([("THREADS".to_owned(), "2".to_owned())])
        .option("allow_unsigned_extensions", "true");
    assert_eq!(env.instance_count()?, 0);
    let first = builder.clone().build(&env)?;
    let instance = builder.build(&env)?;
    assert_eq!(env.instance_count()?, 2);
    for instance in [&first, &instance] {
        let (threads, _) = instance.config()?.get_value("threads")?;
        assert_eq!(i64::from_value(&threads)?, Some(2));
        let (unsigned, _) = instance.config()?.get_value("allow_unsigned_extensions")?;
        assert_eq!(bool::from_value(&unsigned)?, Some(true));
    }
    instance.attach(
        AttachBuilder::new(StorageLocation::InMemory)
            .alias("main2")
            .make_default(),
    )?;
    assert_eq!(current_database(&instance.connect()?)?.as_deref(), Some("main2"));
    Ok(())
}

#[test]
fn instance_keeps_owned_and_borrowed_environments_alive() -> Result<()> {
    let env = Environment::new()?;
    let instance = Instance::new(&env)?;
    drop(env);
    instance.attach(AttachBuilder::new(StorageLocation::InMemory).make_default())?;
    let connection = instance.connect()?;
    drop(instance);
    connection.execute("CREATE TABLE example (id INTEGER)", Parameters::None)?;

    let instance = Instance::new(Environment::new()?)?;
    instance.attach(StorageLocation::InMemory)?;
    instance.connect()?.execute("SELECT 42", Parameters::None)?;

    let instance = InstanceBuilder::default().build(Environment::new()?)?;
    instance.attach(StorageLocation::InMemory)?;
    instance.connect()?.execute("SELECT 42", Parameters::None)?;
    Ok(())
}

#[test]
fn bare_attachment_does_not_select_default() -> Result<()> {
    let env = Environment::new()?;
    let instance = Instance::new(&env)?;
    instance.attach(StorageLocation::InMemory)?;
    let connection = instance.connect()?;
    assert!(
        connection
            .execute("CREATE TABLE example (id INTEGER)", Parameters::None)
            .is_err()
    );
    connection.execute("CREATE TABLE memory.example (id INTEGER)", Parameters::None)?;
    instance.set_default("memory")?;
    // Existing connections retain the default they started with.
    assert!(
        connection
            .execute("CREATE TABLE second (id INTEGER)", Parameters::None)
            .is_err()
    );
    let connection = instance.connect()?;
    assert_eq!(current_database(&connection)?.as_deref(), Some("memory"));
    connection.execute("CREATE TABLE second (id INTEGER)", Parameters::None)?;
    Ok(())
}

#[test]
fn attachment_builder_is_independent_of_instance() -> Result<()> {
    let builder = AttachBuilder::new(StorageLocation::InMemory)
        .option("read_only", "true")
        .options(vec![("READ_ONLY".to_owned(), "false".to_owned())])
        .alias("unused")
        .alias("main\"2'; --".to_owned())
        .make_default();
    let env = Environment::new()?;
    assert_eq!(env.instance_count()?, 0);
    let first = Instance::new(&env)?;
    let second = Instance::new(&env)?;
    assert!(std::ptr::eq(first.attach(builder.clone())?, &first));
    second.attach(builder)?;
    for instance in [&first, &second] {
        let connection = instance.connect()?;
        assert_eq!(current_database(&connection)?.as_deref(), Some("main\"2'; --"));
        connection.execute("CREATE TABLE example (id INTEGER)", Parameters::None)?;
        drop(connection);
        instance.detach("main\"2'; --")?;
        assert!(instance.detach("main\"2'; --").is_err());
    }
    Ok(())
}

#[test]
fn multiple_attachments_share_instance() -> Result<()> {
    let env = Environment::new()?;
    let instance = Instance::new(&env)?;
    instance.attach(
        AttachBuilder::new(StorageLocation::InMemory)
            .alias("main2")
            .make_default(),
    )?;
    instance.attach(StorageLocation::InMemory)?;
    let extra: AttachBuilder = StorageLocation::InMemory.into();
    assert!(instance.attach(extra).is_err());
    instance.attach(AttachBuilder::new(StorageLocation::InMemory).alias("extra"))?;
    assert_eq!(env.instance_count()?, 1);
    let connection = instance.connect()?;
    assert_eq!(current_database(&connection)?.as_deref(), Some("main2"));
    connection.execute("CREATE TABLE example AS SELECT 42 AS id", Parameters::None)?;
    connection.execute("CREATE TABLE memory.example AS SELECT 84 AS id", Parameters::None)?;
    connection.execute("CREATE TABLE extra.example AS SELECT 126 AS id", Parameters::None)?;
    let chunk = connection
        .query(
            "SELECT a.id + b.id + c.id AS total FROM example a, memory.example b, extra.example c",
            Parameters::None,
        )?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&252));
    drop(chunk);
    instance.set_default("extra")?;
    assert_eq!(current_database(&connection)?.as_deref(), Some("main2"));
    assert_eq!(current_database(&instance.connect()?)?.as_deref(), Some("extra"));
    drop(connection);
    instance.detach("main2")?;
    instance.detach("memory")?;
    instance.connect()?.execute("SELECT * FROM example", Parameters::None)?;
    Ok(())
}

#[test]
fn default_only_affects_new_connections() -> Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let connection = instance.connect()?;
    let name = "extra\"'; --";
    instance.attach(AttachBuilder::new(StorageLocation::InMemory).alias(name).make_default())?;
    assert_eq!(env.instance_count()?, 1);
    assert_eq!(current_database(&connection)?.as_deref(), Some("memory"));
    assert_eq!(current_database(&instance.connect()?)?.as_deref(), Some(name));
    instance.set_default("memory")?;
    instance.detach(name)?;
    Ok(())
}

#[test]
fn attachment_supports_read_only_file() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let location = StorageLocation::OnDisk(directory.path().join("data.duckdb").to_str().unwrap().to_owned());
    let env = Environment::new()?;
    {
        let instance = env.open(location.clone())?;
        instance
            .connect()?
            .execute("CREATE TABLE example AS SELECT 42 AS id", Parameters::None)?;
    }
    let instance = env.open(StorageLocation::InMemory)?;
    instance.attach(
        AttachBuilder::new(location)
            .options([("read_only", "false"), ("READ_ONLY", "true")])
            .alias("analytics"),
    )?;
    assert_eq!(env.instance_count()?, 1);
    let connection = instance.connect()?;
    assert_eq!(current_database(&connection)?.as_deref(), Some("memory"));
    let chunk = connection
        .query("SELECT id FROM analytics.example", Parameters::None)?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));
    let error = connection
        .execute("INSERT INTO analytics.example VALUES (43)", Parameters::None)
        .unwrap_err();
    assert!(error.to_string().contains("read-only"));
    drop(chunk);
    drop(connection);
    instance.detach("analytics")?;
    Ok(())
}

#[test]
fn invalid_startup_options_release_instance() -> Result<()> {
    let env = Environment::new()?;
    for (key, value) in [("not_a_duckdb_option", "true"), ("threads", "not_an_integer")] {
        let builder = InstanceBuilder::new().option(key, value);
        assert_eq!(env.instance_count()?, 0);
        assert!(builder.build(&env).is_err());
        assert_eq!(env.instance_count()?, 0);
    }
    Instance::new(&env)?.connect()?.execute("SELECT 42", Parameters::None)?;
    Ok(())
}

#[test]
fn failed_attachment_preserves_instance_and_default() -> Result<()> {
    let env = Environment::new()?;
    let instance = env.open(StorageLocation::InMemory)?;
    let connection = instance.connect()?;
    connection.execute("CREATE TABLE example AS SELECT 42 AS id", Parameters::None)?;
    for (key, value) in [
        ("not_an_attach_option", "true"),
        ("read_only", "not_a_boolean"),
        ("threads", "2"),
        ("allow_unsigned_extensions", "true"),
    ] {
        let attachment = AttachBuilder::new(StorageLocation::InMemory)
            .alias("extra")
            .make_default()
            .option(key, value);
        assert!(instance.attach(attachment).is_err());
        assert_eq!(env.instance_count()?, 1);
        assert_eq!(current_database(&instance.connect()?)?.as_deref(), Some("memory"));
    }
    let directory = tempfile::tempdir().unwrap();
    let location = StorageLocation::OnDisk(directory.path().to_str().unwrap().to_owned());
    assert!(instance.attach(location).is_err());
    assert!(instance.attach(StorageLocation::InMemory).is_err());
    instance.attach(AttachBuilder::new(StorageLocation::InMemory).alias("extra"))?;
    let chunk = connection
        .query("SELECT id FROM example", Parameters::None)?
        .next_chunk()?
        .unwrap();
    assert_eq!(chunk.get_vector_at::<i32>(0)?.get(0)?, Some(&42));
    Ok(())
}

#[test]
fn attachment_file_conflicts_within_environment() -> Result<()> {
    let directory = tempfile::tempdir().unwrap();
    let location = StorageLocation::OnDisk(directory.path().join("data.duckdb").to_str().unwrap().to_owned());
    let env = Environment::new()?;
    let first = Instance::new(&env)?;
    let second = Instance::new(&env)?;
    first.attach(location.clone())?;
    assert!(second.attach(location.clone()).is_err());
    assert_eq!(env.instance_count()?, 2);
    first.detach("data")?;
    second.attach(location)?;
    second.detach("data")?;
    Ok(())
}
