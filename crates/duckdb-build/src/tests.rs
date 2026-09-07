use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = env::temp_dir().join(format!(
            "duckdb-build-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path.canonicalize().unwrap())
    }

    fn directory(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn settings(&self) -> Directories {
        Directories {
            home: Some(self.0.clone()),
            lib: None,
            include: None,
            invocation_dir: self.0.clone(),
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn defaults_to_the_pinned_version_for_both_directories() {
    let fixture = Fixture::new();
    let expected = fixture.directory(&format!(".duckdb/lib/{ARTIFACT_VERSION}"));
    let settings = fixture.settings();
    assert_eq!(settings.library().unwrap(), expected);
    assert_eq!(settings.include().unwrap(), expected);
}

#[test]
fn overrides_are_independent_and_relative_to_invocation() {
    let fixture = Fixture::new();
    let default = fixture.directory(&format!(".duckdb/lib/{ARTIFACT_VERSION}"));
    let custom = fixture.directory("custom artifacts");
    let mut settings = fixture.settings();

    settings.lib = Some(PathBuf::from("custom artifacts"));
    assert_eq!(settings.library().unwrap(), custom);
    assert_eq!(settings.include().unwrap(), default);

    settings.lib = None;
    settings.include = Some(custom.clone());
    assert_eq!(settings.library().unwrap(), default);
    assert_eq!(settings.include().unwrap(), custom);

    settings.home = None;
    settings.lib = Some(custom.clone());
    assert_eq!(settings.library().unwrap(), custom);
    assert_eq!(settings.include().unwrap(), custom);
}

#[test]
fn tilde_overrides_expand_against_home_before_canonicalization() {
    let fixture = Fixture::new();
    let custom = fixture.directory("duckdb libraries");
    let mut settings = fixture.settings();
    settings.invocation_dir = fixture.directory("working-directory");

    for (raw, expected) in [
        ("~", &fixture.0),
        ("~/", &fixture.0),
        ("~/duckdb libraries", &custom),
        ("~/duckdb libraries/../duckdb libraries", &custom),
    ] {
        settings.lib = Some(PathBuf::from(raw));
        settings.include = Some(PathBuf::from(raw));
        assert_eq!(&settings.library().unwrap(), expected);
        assert_eq!(&settings.include().unwrap(), expected);
    }
}

#[test]
fn tilde_expansion_requires_a_home_directory() {
    let fixture = Fixture::new();
    let mut settings = fixture.settings();
    settings.home = None;
    settings.lib = Some(PathBuf::from("~/duckdb"));
    settings.include = Some(PathBuf::from("~"));

    for (result, variable) in [
        (settings.library(), "DUCKDB_LIB_DIR"),
        (settings.include(), "DUCKDB_INCLUDE_DIR"),
    ] {
        let error = result.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(error.to_string().contains(variable));
        assert!(error.to_string().contains("absolute path"));
    }
}

#[test]
fn tildes_in_ordinary_directory_names_are_not_expanded() {
    let fixture = Fixture::new();
    let mut settings = fixture.settings();
    settings.home = None;
    for raw in ["~duckdb", "native/~/duckdb"] {
        let expected = fixture.directory(raw);
        settings.lib = Some(PathBuf::from(raw));
        assert_eq!(settings.library().unwrap(), expected);
    }
}

#[test]
fn missing_installation_explains_how_to_install() {
    let fixture = Fixture::new();
    let error = fixture.settings().library().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NotFound);
    let message = error.to_string();
    for expected in [INSTALL_COMMAND, ARTIFACT_VERSION, ARTIFACT_SHA, "DUCKDB_LIB_DIR"] {
        assert!(message.contains(expected), "{message}");
    }
}

#[test]
fn invalid_overrides_do_not_fall_back_to_the_default() {
    let fixture = Fixture::new();
    fixture.directory(&format!(".duckdb/lib/{ARTIFACT_VERSION}"));
    let mut settings = fixture.settings();
    settings.lib = Some(fixture.0.join("missing"));
    assert_eq!(settings.library().unwrap_err().kind(), io::ErrorKind::NotFound);
    settings.lib = Some(PathBuf::new());
    assert_eq!(settings.library().unwrap_err().kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn absent_home_is_only_an_error_when_a_default_is_needed() {
    let fixture = Fixture::new();
    let mut settings = fixture.settings();
    settings.home = None;
    assert!(settings.library().unwrap_err().to_string().contains(INSTALL_COMMAND));
}

#[test]
fn libraries_are_validated_for_the_target_not_the_host() {
    let cases = [
        ("linux", "gnu", LinkMode::Dynamic, "libduckdb.so"),
        ("android", "", LinkMode::Dynamic, "libduckdb.so"),
        ("macos", "", LinkMode::Dynamic, "libduckdb.dylib"),
        ("ios", "", LinkMode::Dynamic, "libduckdb.dylib"),
        ("windows", "msvc", LinkMode::Dynamic, "duckdb.lib"),
        ("windows", "msvc", LinkMode::Dynamic, "libduckdb.lib"),
        ("windows", "gnu", LinkMode::Dynamic, "libduckdb.dll.a"),
        ("windows", "gnullvm", LinkMode::Dynamic, "libduckdb.dll.a"),
        ("windows", "msvc", LinkMode::Static, "duckdb_static.lib"),
        ("windows", "msvc", LinkMode::Static, "libduckdb_static.lib"),
        ("linux", "gnu", LinkMode::Static, "libduckdb_static.a"),
        ("macos", "", LinkMode::Static, "libduckdb_static.a"),
    ];
    for (os, environment, mode, name) in cases {
        let fixture = Fixture::new();
        let target = Target {
            os: os.into(),
            environment: environment.into(),
        };
        assert!(validate_library(&fixture.0, &target, mode).is_err());
        fs::write(fixture.0.join(name), []).unwrap();
        if os == "windows" && mode == LinkMode::Dynamic {
            let error = validate_library(&fixture.0, &target, mode).unwrap_err();
            assert!(error.to_string().contains("duckdb.dll"));
            fs::write(fixture.0.join("duckdb.dll"), []).unwrap();
        }
        validate_library(&fixture.0, &target, mode).unwrap();
    }
}

#[test]
fn missing_headers_and_directories_named_like_headers_are_rejected() {
    let fixture = Fixture::new();
    assert!(
        require_file(&fixture.0, &["duckdb_v2.h"])
            .unwrap_err()
            .to_string()
            .contains(INSTALL_COMMAND)
    );
    fixture.directory("duckdb_v2.h");
    assert!(require_file(&fixture.0, &["duckdb_v2.h"]).is_err());
}

#[test]
fn unsupported_library_targets_are_rejected_in_both_modes() {
    let fixture = Fixture::new();
    let target = Target {
        os: "unknown".into(),
        environment: String::new(),
    };
    for mode in [LinkMode::Dynamic, LinkMode::Static] {
        assert_eq!(
            validate_library(&fixture.0, &target, mode).unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }
}

#[test]
fn flat_and_split_distributions_use_target_relative_paths() {
    for (os, origin) in [
        ("linux", "$ORIGIN"),
        ("android", "$ORIGIN"),
        ("macos", "@loader_path"),
        ("ios", "@loader_path"),
    ] {
        assert_eq!(runtime_origin(os).unwrap(), Some(origin));
        assert_eq!(relative_rpath(origin, ".").unwrap(), format!("{origin}/."));
        assert_eq!(relative_rpath(origin, "../lib").unwrap(), format!("{origin}/../lib"));
        assert_eq!(
            relative_rpath(origin, "native libs").unwrap(),
            format!("{origin}/native libs")
        );
    }
    assert_eq!(runtime_origin("windows").unwrap(), None);
    assert_eq!(
        runtime_origin("unknown").unwrap_err().kind(),
        io::ErrorKind::Unsupported
    );
}

#[test]
fn absolute_and_malformed_distribution_paths_are_rejected() {
    for path in [
        "",
        "/usr/lib",
        "C:/lib",
        r"C:\lib",
        r"\\server\lib",
        "lib\ncargo:warning=bad",
    ] {
        assert_eq!(
            relative_rpath("$ORIGIN", path).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn cargo_paths_allow_spaces_and_commas_but_not_newlines() {
    assert_eq!(
        cargo_path(Path::new("some path,with commas")).unwrap(),
        "some path,with commas"
    );
    assert!(cargo_path(Path::new("bad\npath")).is_err());
    assert!(cargo_path(Path::new("bad\rpath")).is_err());
}
