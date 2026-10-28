#!/usr/bin/env python3

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

WORKSPACE_DIR = Path(__file__).resolve().parent
RELEASE_PIN_FILE = Path("crates/libduckdb-sys/.duckdb-release")
CRATES = ("duckdb", "libduckdb-sys", "duckdb-loadable-macros")
VERSION_PATTERN = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"


def crate_version(value):
    if not re.fullmatch(VERSION_PATTERN, value):
        raise argparse.ArgumentTypeError("expected MAJOR.MINOR.PATCH (e.g. 1.20000.0)")
    return value


def core_version(value):
    identifier = r"(?:0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    prerelease = rf"(?:-{identifier}(?:\.{identifier})*)?"
    metadata = r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    if not re.fullmatch(rf"v?{VERSION_PATTERN}{prerelease}{metadata}", value):
        raise argparse.ArgumentTypeError(
            "expected [v]MAJOR.MINOR.PATCH with an optional prerelease/build suffix"
        )
    return f"v{value.removeprefix('v')}"


def core_commit(value):
    if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
        raise argparse.ArgumentTypeError(
            "expected a full 40-character hexadecimal commit"
        )
    return value.lower()


def core_url(value):
    error = argparse.ArgumentTypeError(
        "expected an HTTP(S) directory URL without credentials, query, or fragment"
    )
    if any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in value):
        raise error
    if any(char in value for char in "\\?#"):
        raise error
    try:
        url = urlsplit(value)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or url.port == 0
        ):
            raise error
    except ValueError as cause:
        raise error from cause
    return value.rstrip("/")


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Bump the duckdb crates and regenerate bundled DuckDB sources and bindings.",
        epilog="Supply all three core options together to update the pin, sources, and bindings; omit them to update only crate versions.",
        allow_abbrev=False,
    )
    parser.add_argument(
        "duckdb_rs_version",
        metavar="duckdb-rs-version",
        type=crate_version,
        help="version for duckdb, libduckdb-sys, and duckdb-loadable-macros",
    )
    parser.add_argument(
        "neo_version",
        metavar="neo-version",
        type=crate_version,
        help="version for duckdb-neo",
    )
    parser.add_argument(
        "--core-commit",
        metavar="COMMIT",
        type=core_commit,
        help="full DuckDB commit used to build the libraries",
    )
    parser.add_argument(
        "--core-url",
        metavar="URL",
        type=core_url,
        help="HTTP(S) directory containing duckdb-shared-libs-<platform>.tar.gz",
    )
    parser.add_argument(
        "--core-version",
        metavar="VERSION",
        type=core_version,
        help="DuckDB build version (e.g. v2.0.0 or v2.0.0-alpha39998)",
    )
    args = parser.parse_args(argv)
    core = (args.core_commit, args.core_url, args.core_version)
    if any(value is not None for value in core) and not all(
        value is not None for value in core
    ):
        parser.error(
            "--core-commit, --core-url, and --core-version must be supplied together"
        )
    return args


def current_crate_version(path):
    manifest = tomllib.loads(path.read_text(encoding="utf-8"))
    package = (
        manifest["workspace"]["package"]
        if "workspace" in manifest
        else manifest["package"]
    )
    return crate_version(package["version"])


def crate_version_to_core_version(version):
    encoded = int(version.split(".")[1])
    return f"v{encoded // 10000}.{encoded // 100 % 100}.{encoded % 100}"


def read_release_pin(root):
    path = root / RELEASE_PIN_FILE
    if not path.exists():
        return {}
    return dict(
        line.split("=", 1)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith("DUCKDB_RELEASE_") and "=" in line
    )


def update_release_pin(root, args):
    path = root / RELEASE_PIN_FILE
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    values = {
        "DUCKDB_RELEASE_COMMIT": args.core_commit,
        "DUCKDB_RELEASE_URL": args.core_url,
        "DUCKDB_RELEASE_VERSION": args.core_version,
    }
    for key, value in values.items():
        line = f"{key}={value}"
        text, count = re.subn(
            rf"(?m)^{key}=[^\n]*$", lambda match, line=line: line, text
        )
        if not count:
            if text and not text.endswith("\n"):
                text += "\n"
            text += f"{line}\n"
    path.write_text(text, encoding="utf-8")


def replace_version(text, name, version, count=0):
    pattern = rf'(?m)^([ \t]*{re.escape(name)}[ \t]*=[ \t]*(?:\{{[^\n]*?\bversion[ \t]*=[ \t]*)?"[=~^]?)[^"\n]+(")'
    return re.sub(
        pattern, lambda match: f"{match[1]}{version}{match[2]}", text, count=count
    )


def update_crate_versions(root, version, neo_version):
    subprocess.run(
        ["cargo", "metadata", "--format-version", "1"],
        cwd=root,
        check=True,
        stdout=subprocess.DEVNULL,
    )

    workspace = root / "Cargo.toml"
    text = replace_version(
        workspace.read_text(encoding="utf-8"), "version", version, count=1
    )
    for crate in CRATES:
        text = replace_version(text, crate, version)
    text = replace_version(text, "duckdb-neo", neo_version)
    workspace.write_text(text, encoding="utf-8")

    neo = root / "crates/duckdb-neo/Cargo.toml"
    neo.write_text(
        replace_version(
            neo.read_text(encoding="utf-8"), "version", neo_version, count=1
        ),
        encoding="utf-8",
    )

    readme = root / "README.md"
    text = readme.read_text(encoding="utf-8")
    for crate in CRATES:
        text = replace_version(text, crate, version)
    text = replace_version(text, "duckdb-neo", neo_version)
    readme.write_text(text, encoding="utf-8")

    for path, expected in ((workspace, version), (neo, neo_version)):
        actual = current_crate_version(path)
        if actual != expected:
            raise ValueError(f"Expected {path} version {expected}, found {actual}")

    # Let Cargo update the lockfile from the manifests.
    subprocess.run(
        ["cargo", "metadata", "--format-version", "1"],
        cwd=root,
        check=True,
        stdout=subprocess.DEVNULL,
    )


def upgrade(args, root):
    current = current_crate_version(root / "Cargo.toml")
    current_neo = current_crate_version(root / "crates/duckdb-neo/Cargo.toml")
    if args.core_commit:
        current_core = read_release_pin(root).get(
            "DUCKDB_RELEASE_VERSION", crate_version_to_core_version(current)
        )
        print(
            f"Upgrade DuckDB from {current_core} to {args.core_version} ({args.core_commit})"
        )
    print(f"Update crate version from {current} to {args.duckdb_rs_version}")
    print(f"Update duckdb-neo version from {current_neo} to {args.neo_version}")
    update_crate_versions(root, args.duckdb_rs_version, args.neo_version)
    if not args.core_commit:
        return
    update_release_pin(root, args)

    workflow = root / ".github/workflows/rust.yaml"
    text = workflow.read_text(encoding="utf-8")
    text = re.sub(
        rf"{re.escape(current_core)}(?![0-9A-Za-z.+-])", args.core_version, text
    )
    workflow.write_text(text, encoding="utf-8")
    subprocess.run(
        [
            "bash",
            "crates/libduckdb-sys/upgrade.sh",
            args.core_version,
            "--sha",
            args.core_commit,
        ],
        cwd=root,
        check=True,
    )


def main(argv=None):
    args = parse_args(argv)
    try:
        upgrade(args, WORKSPACE_DIR)
    except (
        OSError,
        ValueError,
        argparse.ArgumentTypeError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
