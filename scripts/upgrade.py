#!/usr/bin/env python3

import argparse
import gzip
import importlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import urllib.error
import urllib.request
from collections.abc import Iterator, Sequence
from pathlib import Path
from types import ModuleType
from urllib.parse import quote, urlsplit

WORKSPACE_DIR = Path(__file__).resolve().parents[1]
RELEASE_PIN_FILE = Path("crates/libduckdb-sys/.duckdb-release")
DOWNLOAD_SHA_FILE = Path("crates/libduckdb-sys/build_download_sha.rs")
# Archives the linked build downloads, keyed by their constant in DOWNLOAD_SHA_FILE.
DOWNLOAD_ARCHIVES = (
    ("LIBDUCKDB_OSX_SHA", "duckdb-shared-libs-osx-universal.tar.gz"),
    ("LIBDUCKDB_LINUX_AMD64_SHA", "duckdb-shared-libs-linux-amd64.tar.gz"),
    ("LIBDUCKDB_LINUX_ARM64_SHA", "duckdb-shared-libs-linux-arm64.tar.gz"),
    ("LIBDUCKDB_WINDOWS_AMD64_SHA", "duckdb-shared-libs-windows-amd64.tar.gz"),
    ("LIBDUCKDB_WINDOWS_ARM64_SHA", "duckdb-shared-libs-windows-arm64.tar.gz"),
    ("LIBDUCKDB_LINUX_MUSL_AMD64_SHA", "duckdb-shared-libs-linux-amd64-musl.tar.gz"),
    ("LIBDUCKDB_LINUX_MUSL_ARM64_SHA", "duckdb-shared-libs-linux-arm64-musl.tar.gz"),
)
# Directory URLs serving the assets of a tagged GitHub release, followed by the tag.
RELEASE_URL_PREFIXES = (
    "https://github.com/duckdb/duckdb/releases/download/",
    "https://install.duckdb.org/",
)
GITHUB_RELEASE_API = "https://api.github.com/repos/duckdb/duckdb/releases/tags/"
CRATES = ("duckdb", "libduckdb-sys", "duckdb-loadable-macros")
EXTENSIONS = ("core_functions", "parquet", "json", "icu")
EXCLUDED_THIRD_PARTY = ()
# crates.io rejects uploads larger than this.
CRATES_IO_MAX_UPLOAD_SIZE = 10 * 1024 * 1024
# A reproducible timestamp that Windows archive tools can restore.
ARCHIVE_MTIME = 946684800
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
        raise argparse.ArgumentTypeError("expected MAJOR.MINOR.PATCH (e.g. 1.20001.0)")
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
        epilog=(
            "Omitted crate versions keep their current manifest values. "
            "Core options update .duckdb-release and rebuild the bundle; "
            "omitted ones keep their pinned values."
        ),
        allow_abbrev=False,
    )
    parser.add_argument(
        "duckdb_rs_version",
        metavar="duckdb-rs-version",
        nargs="?",
        type=crate_version,
        help="version for duckdb, libduckdb-sys, and duckdb-loadable-macros",
    )
    parser.add_argument(
        "neo_version",
        metavar="neo-version",
        nargs="?",
        type=crate_version,
        help="version for duckdb-neo",
    )
    parser.add_argument(
        "-b",
        "--bundle",
        action="store_true",
        help="regenerate the bundled DuckDB sources and bindings from .duckdb-release",
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
        help="DuckDB build version (e.g. v2.0.1 or v2.0.1-alpha39998)",
    )
    return parser.parse_args(argv)


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
    return {
        key: value.strip()
        for key, value in (
            line.split("=", 1)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.startswith("DUCKDB_RELEASE_") and "=" in line
        )
    }


def resolve_core(root: Path, args: argparse.Namespace) -> dict[str, str]:
    pin = read_release_pin(root)
    options = (
        ("DUCKDB_RELEASE_COMMIT", "--core-commit", args.core_commit, core_commit),
        ("DUCKDB_RELEASE_URL", "--core-url", args.core_url, core_url),
        ("DUCKDB_RELEASE_VERSION", "--core-version", args.core_version, core_version),
    )
    core = {}
    for key, option, value, validate in options:
        if value is None:
            if key not in pin:
                raise ValueError(
                    f"{option} is required: {RELEASE_PIN_FILE} has no {key}"
                )
            try:
                value = validate(pin[key])
            except argparse.ArgumentTypeError as error:
                raise ValueError(
                    f"{RELEASE_PIN_FILE}: invalid {key}: {error}"
                ) from error
        core[key] = value
    return core


def update_release_pin(root: Path, core: dict[str, str]) -> None:
    path = root / RELEASE_PIN_FILE
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    for key, value in core.items():
        line = f"{key}={value}"
        text, count = re.subn(rf"(?m)^{key}=[^\n]*$", line.replace("\\", r"\\"), text)
        if not count:
            if text and not text.endswith("\n"):
                text += "\n"
            text += f"{line}\n"
    path.write_text(text, encoding="utf-8")


def fetch_release_digests(tag: str) -> dict[str, str]:
    request = urllib.request.Request(
        GITHUB_RELEASE_API + quote(tag, safe=""),
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            release = json.load(response)
    except urllib.error.HTTPError as error:
        raise ValueError(
            f"Failed to fetch GitHub release {tag}: HTTP {error.code}"
        ) from error
    digests = {}
    for asset in release.get("assets", []):
        algorithm, _, digest = (asset.get("digest") or "").partition(":")
        if algorithm == "sha256" and re.fullmatch(r"[0-9a-f]{64}", digest):
            digests[asset["name"]] = digest
    return digests


def resolve_download_shas(core: dict[str, str]) -> dict[str, str]:
    url = core["DUCKDB_RELEASE_URL"]
    version = core["DUCKDB_RELEASE_VERSION"]
    if not url.startswith(RELEASE_URL_PREFIXES):
        print(
            f"warning: {url} is not a DuckDB release URL, so {DOWNLOAD_SHA_FILE} "
            "gets no checksums and linked builds will not verify their download",
            file=sys.stderr,
        )
        return {name: "" for name, _ in DOWNLOAD_ARCHIVES}
    if not url.endswith(f"/{version}"):
        raise ValueError(f"Release URL {url} does not match version {version}")
    digests = fetch_release_digests(version)
    missing = [archive for _, archive in DOWNLOAD_ARCHIVES if archive not in digests]
    if missing:
        raise ValueError(
            f"GitHub release {version} has no SHA-256 digest for {', '.join(missing)}"
        )
    return {name: digests[archive] for name, archive in DOWNLOAD_ARCHIVES}


def write_download_shas(root: Path, version: str, shas: dict[str, str]) -> None:
    lines = [
        "// @generated by scripts/upgrade.py from the GitHub release asset digests.",
        "// Empty when the pinned build has no release, e.g. a staging build of main.",
        "",
        "/// The `DUCKDB_RELEASE_VERSION` the checksums below belong to.",
        f'pub const LIBDUCKDB_SHA_VERSION: &str = "{version}";',
        "",
        *(f'pub const {name}: &str = "{sha}";' for name, sha in shas.items()),
    ]
    (root / DOWNLOAD_SHA_FILE).write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def is_excluded(path: str) -> bool:
    normalized = Path(path).as_posix()
    return any(f"/{excluded}" in f"/{normalized}" for excluded in EXCLUDED_THIRD_PARTY)


def get_sources(
    package_build: ModuleType,
    sys_dir: Path,
    extensions: Sequence[str],
    default_linked_extensions: Sequence[str] | None = None,
) -> tuple[set[str], set[str]]:
    kwargs = {}
    if default_linked_extensions is not None:
        kwargs["default_linked_extensions"] = default_linked_extensions
    loader = sys_dir / "duckdb-sources/generated_extension_loader_package_build.cpp"
    try:
        source_list, include_list, _ = package_build.build_package(
            str(sys_dir / "duckdb"), extensions, False, **kwargs
        )
    finally:
        loader.unlink(missing_ok=True)

    prefix = f"{sys_dir.as_posix()}/"
    sources = {path.removeprefix(prefix) for path in source_list}
    return (
        {path for path in sources if not is_excluded(path)},
        {path for path in include_list if not is_excluded(path)},
    )


def package_sources(root: Path, version: str) -> None:
    sys_dir = (root / "crates/libduckdb-sys").resolve()
    target = sys_dir / "duckdb"
    scripts_dir = sys_dir / "duckdb-sources/scripts"
    if not (scripts_dir / "package_build.py").is_file():
        raise FileNotFoundError(
            f"Missing DuckDB packaging script: {scripts_dir / 'package_build.py'}"
        )

    # Upstream packaging mutates cwd, import paths, and extension module caches.
    cwd = Path.cwd()
    import_paths = sys.path[:]
    previous_version = os.environ.get("DUCKDB_VERSION")
    module_names = (
        "package_build",
        "amalgamation",
        "python_helpers",
        *(f"{name}_config" for name in EXTENSIONS),
    )
    modules = {name: sys.modules.pop(name, None) for name in module_names}
    try:
        os.chdir(sys_dir)
        sys.path[:0] = [
            str(scripts_dir),
            *(str(sys_dir / "duckdb-sources/extension" / name) for name in EXTENSIONS),
        ]
        os.environ["DUCKDB_VERSION"] = version
        importlib.invalidate_caches()
        package_build = importlib.import_module("package_build")
        amalgamation = importlib.import_module("amalgamation")
        amalgamation.compile_directories[:] = [
            os.path.normpath(path) for path in amalgamation.compile_directories
        ]

        try:
            shutil.rmtree(target)
        except FileNotFoundError:
            pass
        target.mkdir()
        base_sources, base_includes = get_sources(package_build, sys_dir, [])
        extension_sources = {}
        for extension in EXTENSIONS:
            sources, includes = get_sources(package_build, sys_dir, [extension])
            extension_sources[extension] = {
                "cpp_files": sorted(sources - base_sources),
                "include_dirs": sorted(includes - base_includes),
            }

        # Cargo feature defines select which of the packaged extensions to link.
        get_sources(package_build, sys_dir, EXTENSIONS, default_linked_extensions=[])
        manifest = {
            "base": {
                "cpp_files": sorted(base_sources),
                "include_dirs": sorted(base_includes),
            },
            "extensions": extension_sources,
        }
        for excluded in EXCLUDED_THIRD_PARTY:
            shutil.rmtree(target / excluded, ignore_errors=True)
        with (target / "manifest.json").open(
            "w", encoding="utf-8", newline="\n"
        ) as file:
            json.dump(manifest, file, indent=2, sort_keys=True)
            file.write("\n")
    finally:
        os.chdir(cwd)
        sys.path[:] = import_paths
        if previous_version is None:
            os.environ.pop("DUCKDB_VERSION", None)
        else:
            os.environ["DUCKDB_VERSION"] = previous_version
        for name, module in modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    replace_archive_if_changed(target, sys_dir / "duckdb.tar.gz")


def iter_archive_paths(root: Path) -> Iterator[Path]:
    yield root
    for dirpath, dirnames, filenames in os.walk(root):
        directory = Path(dirpath)
        dirnames.sort()
        filenames.sort()
        for dirname in dirnames:
            yield directory / dirname
        for filename in filenames:
            yield directory / filename


def normalized_tarinfo(path: Path, base_dir: Path) -> tarfile.TarInfo:
    tarinfo = tarfile.TarInfo(path.relative_to(base_dir).as_posix())
    tarinfo.uid = 0
    tarinfo.gid = 0
    tarinfo.uname = ""
    tarinfo.gname = ""
    tarinfo.mtime = ARCHIVE_MTIME
    tarinfo.pax_headers = {}
    if path.is_symlink():
        tarinfo.type = tarfile.SYMTYPE
        tarinfo.mode = 0o777
        tarinfo.linkname = str(path.readlink())
    elif path.is_dir():
        tarinfo.type = tarfile.DIRTYPE
        tarinfo.mode = 0o755
    elif path.is_file():
        tarinfo.type = tarfile.REGTYPE
        tarinfo.mode = 0o644
        tarinfo.size = path.stat().st_size
    else:
        filemode = stat.filemode(path.lstat().st_mode)
        raise RuntimeError(f"unsupported archive entry type: {path} ({filemode})")
    return tarinfo


def write_archive_to(target: Path, archive_path: Path) -> None:
    with (
        archive_path.open("wb") as archive_file,
        gzip.GzipFile(
            filename="", mode="wb", fileobj=archive_file, mtime=0, compresslevel=9
        ) as gzip_file,
        tarfile.open(fileobj=gzip_file, mode="w", format=tarfile.PAX_FORMAT) as tar,
    ):
        for path in iter_archive_paths(target):
            tarinfo = normalized_tarinfo(path, target.parent)
            if tarinfo.isfile():
                with path.open("rb") as file:
                    tar.addfile(tarinfo, file)
            else:
                tar.addfile(tarinfo)


def replace_archive_if_changed(target: Path, archive_path: Path) -> None:
    fd, temp_name = tempfile.mkstemp(
        dir=archive_path.parent, prefix=f".{archive_path.name}."
    )
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        write_archive_to(target, temp_path)
        # Cargo uses timestamps to detect changes to bundled sources.
        if (
            archive_path.exists()
            and temp_path.read_bytes() == archive_path.read_bytes()
        ):
            return
        temp_path.replace(archive_path)
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
    package_sources(root, version)

    target_dir = cargo_target_dir(root)
    for output, generated, features in BINDING_VARIANTS:
        regenerate_bindings(root, target_dir, output, generated, features)
    print("Finished regenerating bundled DuckDB sources and bindings (tests not run)")


def check_package_size(root: Path, version: str) -> None:
    subprocess.run(
        [
            "cargo",
            "package",
            "-p",
            "libduckdb-sys",
            "--no-verify",
            "--allow-dirty",
        ],
        cwd=root,
        check=True,
    )
    package = cargo_target_dir(root) / "package" / f"libduckdb-sys-{version}.crate"
    size = package.stat().st_size
    limit = CRATES_IO_MAX_UPLOAD_SIZE
    print(
        f"libduckdb-sys package is {size / 2**20:.2f} MiB "
        f"({size / limit:.0%} of the {limit / 2**20:.0f} MiB crates.io limit)"
    )
    if size > limit:
        raise ValueError(
            f"{package} is {size} bytes, over the crates.io limit of {limit} bytes"
        )


def upgrade(args: argparse.Namespace, root: Path) -> None:
    current = current_crate_version(root / "Cargo.toml")
    current_neo = current_crate_version(root / "crates/duckdb-neo/Cargo.toml")
    version = args.duckdb_rs_version or current
    neo_version = args.neo_version or current_neo
    update_pin = any(
        value is not None
        for value in (args.core_commit, args.core_url, args.core_version)
    )
    rebuild_bundle = args.bundle or update_pin
    core = resolve_core(root, args) if rebuild_bundle else {}
    download_shas = resolve_download_shas(core) if rebuild_bundle else {}
    commit = core.get("DUCKDB_RELEASE_COMMIT")
    new_core = core.get("DUCKDB_RELEASE_VERSION")
    if update_pin:
        current_core = read_release_pin(root).get(
            "DUCKDB_RELEASE_VERSION", crate_version_to_core_version(current)
        )
        print(f"Upgrade DuckDB pin from {current_core} to {new_core} ({commit})")
    if rebuild_bundle:
        print(f"Rebuild bundled DuckDB {new_core} ({commit})")
    print(f"Update crate version from {current} to {version}")
    print(f"Update duckdb-neo version from {current_neo} to {neo_version}")
    update_crate_versions(root, version, neo_version)
    if update_pin:
        update_release_pin(root, core)
        workflow = root / ".github/workflows/rust.yaml"
        text = workflow.read_text(encoding="utf-8")
        text = re.sub(rf"{re.escape(current_core)}(?![0-9A-Za-z.+-])", new_core, text)
        workflow.write_text(text, encoding="utf-8")
    if rebuild_bundle:
        write_download_shas(root, new_core, download_shas)
        regenerate_bundled_sources(root, commit, new_core)
        check_package_size(root, version)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        upgrade(args, WORKSPACE_DIR)
    except (
        OSError,
        ValueError,
        ImportError,
        RuntimeError,
        tarfile.TarError,
        argparse.ArgumentTypeError,
        subprocess.CalledProcessError,
    ) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
