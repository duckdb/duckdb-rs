#!/usr/bin/env python3

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import urlsplit

WORKSPACE_DIR = Path(__file__).resolve().parent
RELEASE_PIN_FILE = Path("crates/libduckdb-sys/.duckdb-release")
CRATES = ("duckdb", "libduckdb-sys", "duckdb-loadable-macros")
VERSION_PATTERN = r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
BINDING_VARIANTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("bindgen_bundled_version.rs", "bindgen.rs", ("capi-v1",)),
    (
        "bindgen_bundled_version_loadable.rs",
        "bindgen.rs",
        ("capi-v1", "loadable-extension"),
    ),
    ("bindgen_bundled_version_v2.rs", "bindgen_v2.rs", ("capi-v2",)),
    (
        "bindgen_bundled_version_loadable_v2.rs",
        "bindgen_v2.rs",
        ("capi-v2", "loadable-extension-v2"),
    ),
)


def crate_version(value: str) -> str:
    if not re.fullmatch(VERSION_PATTERN, value):
        raise argparse.ArgumentTypeError("expected MAJOR.MINOR.PATCH (e.g. 1.20000.0)")
    return value


def core_version(value: str) -> str:
    identifier = r"(?:0|[1-9][0-9]*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
    prerelease = rf"(?:-{identifier}(?:\.{identifier})*)?"
    metadata = r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?"
    if not re.fullmatch(rf"v?{VERSION_PATTERN}{prerelease}{metadata}", value):
        raise argparse.ArgumentTypeError(
            "expected [v]MAJOR.MINOR.PATCH with an optional prerelease/build suffix"
        )
    return f"v{value.removeprefix('v')}"


def core_commit(value: str) -> str:
    if not re.fullmatch(r"[0-9a-fA-F]{40}", value):
        raise argparse.ArgumentTypeError(
            "expected a full 40-character hexadecimal commit"
        )
    return value.lower()


def core_url(value: str) -> str:
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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
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


def current_crate_version(path: Path) -> str:
    manifest = tomllib.loads(path.read_text(encoding="utf-8"))
    package = (
        manifest["workspace"]["package"]
        if "workspace" in manifest
        else manifest["package"]
    )
    return crate_version(package["version"])


def crate_version_to_core_version(version: str) -> str:
    encoded = int(version.split(".")[1])
    return f"v{encoded // 10000}.{encoded // 100 % 100}.{encoded % 100}"


def read_release_pin(root: Path) -> dict[str, str]:
    path = root / RELEASE_PIN_FILE
    if not path.exists():
        return {}
    return dict(
        line.split("=", 1)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.startswith("DUCKDB_RELEASE_") and "=" in line
    )


def update_release_pin(root: Path, args: argparse.Namespace) -> None:
    path = root / RELEASE_PIN_FILE
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    values = {
        "DUCKDB_RELEASE_COMMIT": args.core_commit,
        "DUCKDB_RELEASE_URL": args.core_url,
        "DUCKDB_RELEASE_VERSION": args.core_version,
    }
    for key, value in values.items():
        line = f"{key}={value}"
        text, count = re.subn(rf"(?m)^{key}=[^\n]*$", line.replace("\\", r"\\"), text)
        if not count:
            if text and not text.endswith("\n"):
                text += "\n"
            text += f"{line}\n"
    path.write_text(text, encoding="utf-8")


def replace_version(text: str, name: str, version: str, count: int = 0) -> str:
    pattern = rf'(?m)^([ \t]*{re.escape(name)}[ \t]*=[ \t]*(?:\{{[^\n]*?\bversion[ \t]*=[ \t]*)?"[=~^]?)[^"\n]+(")'
    return re.sub(
        pattern, lambda match: f"{match[1]}{version}{match[2]}", text, count=count
    )


def update_crate_versions(root: Path, version: str, neo_version: str) -> None:
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


def cargo_target_dir(root: Path) -> Path:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps"],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    return Path(json.loads(result.stdout)["target_directory"])


def regenerate_bindings(
    root: Path,
    target_dir: Path,
    output_name: str,
    generated_name: str,
    features: Sequence[str],
) -> None:
    sys_dir = root / "crates/libduckdb-sys"
    for name in ("bindgen.rs", "bindgen_v2.rs"):
        for path in target_dir.glob(f"**/libduckdb-sys-*/out/{name}"):
            if path.is_file():
                path.unlink()

    print(f"Regenerate {output_name}", flush=True)
    subprocess.run(
        [
            "cargo",
            "check",
            "-p",
            "libduckdb-sys",
            "--no-default-features",
            "--features",
            ",".join(("buildtime_bindgen", *features)),
        ],
        cwd=root,
        check=True,
        env={
            **os.environ,
            "DUCKDB_LIB_DIR": str(sys_dir / "duckdb"),
            "DUCKDB_INCLUDE_DIR": str(sys_dir / "duckdb/src/include"),
        },
    )

    generated = [
        path
        for path in target_dir.glob(f"**/libduckdb-sys-*/out/{generated_name}")
        if path.is_file()
    ]
    if len(generated) != 1:
        raise ValueError(
            f"Expected one generated {generated_name} in {target_dir}, found {len(generated)}"
        )

    output = sys_dir / "src" / output_name
    fd, temp_name = tempfile.mkstemp(dir=output.parent, prefix=f".{output.name}.")
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        shutil.copy(generated[0], temp_path)
        temp_path.replace(output)
    finally:
        temp_path.unlink(missing_ok=True)


def regenerate_bundled_sources(root: Path, commit: str, version: str) -> None:
    sys_dir = root / "crates/libduckdb-sys"
    sources = sys_dir / "duckdb-sources"
    subprocess.run(
        [
            "git",
            "submodule",
            "update",
            "--init",
            "--checkout",
            "--",
            "crates/libduckdb-sys/duckdb-sources",
        ],
        cwd=root,
        check=True,
    )
    try:
        subprocess.run(["git", "fetch", "origin", commit], cwd=sources, check=True)
    except subprocess.CalledProcessError as error:
        raise ValueError(
            f"Failed to fetch DuckDB commit {commit} from origin. "
            "Ensure the commit is reachable from an advertised remote ref."
        ) from error
    subprocess.run(["git", "checkout", "--detach", commit], cwd=sources, check=True)
    subprocess.run(
        [sys.executable, str(sys_dir / "update_sources.py")],
        cwd=sys_dir,
        check=True,
        env={**os.environ, "DUCKDB_VERSION": version},
    )

    target_dir = cargo_target_dir(root)
    for output, generated, features in BINDING_VARIANTS:
        regenerate_bindings(root, target_dir, output, generated, features)
    print("Finished regenerating bundled DuckDB sources and bindings (tests not run)")


def upgrade(args: argparse.Namespace, root: Path) -> None:
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
    regenerate_bundled_sources(root, args.core_commit, args.core_version)


def main(argv: Sequence[str] | None = None) -> int:
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
