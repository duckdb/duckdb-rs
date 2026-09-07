#!/usr/bin/env python3
"""Generate a Markdown type/trait coverage matrix from rustdoc JSON. 
The result is indicative only. Some types have special implementations.

By default this runs the repository's nightly rustdoc command and writes the
matrix to stdout:

    python3 scripts/generate_type_coverage.py > type-coverage.md

Pass ``--input target/doc/duckdb_rs.json`` to reuse existing rustdoc JSON.
Run with ``--help`` for all options.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


TRAITS = (
    "DuckDBType",
    "ToValue",
    "FromValue",
    "VectorElement",
    "WritableVectorElement",
)

TRAIT_PATH_SUFFIXES = {
    "DuckDBType": ("types", "DuckDBType"),
    "ToValue": ("types", "ToValue"),
    "FromValue": ("types", "FromValue"),
    "VectorElement": ("vector", "element", "VectorElement"),
    "WritableVectorElement": ("vector", "element", "WritableVectorElement"),
}


class RustdocError(RuntimeError):
    """Raised when the rustdoc JSON does not contain the expected data."""


class TypeRenderer:
    def __init__(self, canonical: bool = False) -> None:
        self.canonical = canonical
        self.type_generics: dict[str, str] = {}
        self.const_generics: dict[str, str] = {}
        self.lifetimes: dict[str, str] = {}

    def render(self, type_: dict[str, Any]) -> str:
        if "primitive" in type_:
            return type_["primitive"]
        if "generic" in type_:
            return self._generic(type_["generic"])
        if "resolved_path" in type_:
            path = type_["resolved_path"]
            name = "Opt" if path["path"] == "Option" else path["path"]
            return name + self._generic_args(path.get("args"))
        if "tuple" in type_:
            elements = [self.render(element) for element in type_["tuple"]]
            if len(elements) == 1:
                return f"({elements[0]},)"
            return f"({', '.join(elements)})"
        if "array" in type_:
            array = type_["array"]
            return f"[{self.render(array['type'])}; {self._const(array['len'])}]"
        if "borrowed_ref" in type_:
            reference = type_["borrowed_ref"]
            lifetime = self._lifetime(reference.get("lifetime"))
            mutable = "mut " if reference["is_mutable"] else ""
            prefix = "&" + (lifetime + " " if lifetime else "")
            return prefix + mutable + self.render(reference["type"])
        raise RustdocError(f"unsupported rustdoc type: {json.dumps(type_, sort_keys=True)}")

    def _generic_args(self, args: dict[str, Any] | None) -> str:
        if not args:
            return ""
        if "angle_bracketed" in args:
            angle = args["angle_bracketed"]
            rendered = [self._generic_arg(arg) for arg in angle["args"]]
            if angle["constraints"]:
                raise RustdocError("associated type constraints are not impl targets")
            return f"<{', '.join(rendered)}>"
        raise RustdocError(
            f"unsupported rustdoc generic arguments: {json.dumps(args, sort_keys=True)}"
        )

    def _generic_arg(self, arg: dict[str, Any]) -> str:
        if "type" in arg:
            return self.render(arg["type"])
        if "lifetime" in arg:
            return self._lifetime(arg["lifetime"])
        if "const" in arg:
            constant = arg["const"]
            return self._const(constant.get("expr") or constant.get("value") or "_")
        raise RustdocError(
            f"unsupported rustdoc generic argument: {json.dumps(arg, sort_keys=True)}"
        )

    def _generic(self, name: str) -> str:
        if not self.canonical:
            return name
        return self.type_generics.setdefault(name, f"T{len(self.type_generics)}")

    def _const(self, expression: Any) -> str:
        text = str(expression)
        if not self.canonical or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
            return text
        return self.const_generics.setdefault(text, f"C{len(self.const_generics)}")

    def _lifetime(self, lifetime: str | None) -> str:
        if not lifetime or lifetime in ("'static", "'_") or not self.canonical:
            return lifetime or ""
        return self.lifetimes.setdefault(lifetime, f"'L{len(self.lifetimes)}")


def item_by_id(document: dict[str, Any], item_id: Any) -> dict[str, Any]:
    try:
        return document["index"][str(item_id)]
    except KeyError as error:
        raise RustdocError(f"rustdoc item {item_id!r} is missing from the index") from error


def discover_traits(document: dict[str, Any]) -> dict[str, str]:
    root = item_by_id(document, document["root"])
    local_crate_id = root["crate_id"]
    discovered: dict[str, str] = {}

    for name in TRAITS:
        candidates = []
        preferred = []
        suffix = TRAIT_PATH_SUFFIXES[name]
        for item_id, summary in document["paths"].items():
            path = tuple(summary.get("path", ()))
            if (
                summary.get("crate_id") == local_crate_id
                and summary.get("kind") == "trait"
                and path
                and path[-1] == name
            ):
                candidates.append((item_id, path))
                if path[-len(suffix) :] == suffix:
                    preferred.append((item_id, path))

        matches = preferred or candidates
        if len(matches) != 1:
            paths = ", ".join("::".join(path) for _, path in matches) or "none"
            raise RustdocError(
                f"expected one local trait named {name}, found {len(matches)}: {paths}"
            )
        discovered[name] = str(matches[0][0])

    return discovered


def generic_args_match(
    pattern: dict[str, Any] | None, candidate: dict[str, Any] | None
) -> bool:
    if pattern is None or candidate is None:
        return pattern is None and candidate is None
    pattern_args = pattern.get("angle_bracketed")
    candidate_args = candidate.get("angle_bracketed")
    if pattern_args is None or candidate_args is None:
        return pattern == candidate
    if pattern_args["constraints"] or candidate_args["constraints"]:
        return pattern == candidate
    if len(pattern_args["args"]) != len(candidate_args["args"]):
        return False
    return all(
        generic_arg_matches(left, right)
        for left, right in zip(pattern_args["args"], candidate_args["args"])
    )


def generic_arg_matches(pattern: dict[str, Any], candidate: dict[str, Any]) -> bool:
    if "type" in pattern and "type" in candidate:
        return type_pattern_matches(pattern["type"], candidate["type"])
    if "lifetime" in pattern and "lifetime" in candidate:
        return pattern["lifetime"] != "'static" or candidate["lifetime"] == "'static"
    if "const" in pattern and "const" in candidate:
        constant = pattern["const"]
        if isinstance(constant, dict):
            expression = str(constant.get("expr") or constant.get("value") or "_")
            is_literal = constant.get("is_literal", False)
        else:
            expression = str(constant)
            is_literal = False
        if not is_literal and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", expression):
            return True
        return pattern == candidate
    return False


def type_pattern_matches(pattern: dict[str, Any], candidate: dict[str, Any]) -> bool:
    if "generic" in pattern:
        return True
    if pattern.keys() != candidate.keys():
        return False
    if "primitive" in pattern:
        return pattern["primitive"] == candidate["primitive"]
    if "resolved_path" in pattern:
        left = pattern["resolved_path"]
        right = candidate["resolved_path"]
        return left["id"] == right["id"] and generic_args_match(
            left.get("args"), right.get("args")
        )
    if "tuple" in pattern:
        left = pattern["tuple"]
        right = candidate["tuple"]
        return len(left) == len(right) and all(
            type_pattern_matches(left_item, right_item)
            for left_item, right_item in zip(left, right)
        )
    if "array" in pattern:
        left = pattern["array"]
        right = candidate["array"]
        return type_pattern_matches(left["type"], right["type"]) and generic_arg_matches(
            {"const": left["len"]}, {"const": right["len"]}
        )
    if "borrowed_ref" in pattern:
        left = pattern["borrowed_ref"]
        right = candidate["borrowed_ref"]
        return (
            left["is_mutable"] == right["is_mutable"]
            and generic_arg_matches(
                {"lifetime": left.get("lifetime") or "'_"},
                {"lifetime": right.get("lifetime") or "'_"},
            )
            and type_pattern_matches(left["type"], right["type"])
        )
    return pattern == candidate


def collect_coverage(
    document: dict[str, Any], trait_ids: dict[str, str]
) -> dict[str, dict[str, Any]]:
    names_by_id = {item_id: name for name, item_id in trait_ids.items()}
    rows: dict[str, dict[str, Any]] = {}
    implementations: list[tuple[dict[str, Any], str]] = []

    for item in document["index"].values():
        implementation = item.get("inner", {}).get("impl")
        trait = implementation and implementation.get("trait")
        if not trait:
            continue
        trait_name = names_by_id.get(str(trait.get("id")))
        if not trait_name:
            continue

        target = implementation["for"]
        implementations.append((target, trait_name))
        key = TypeRenderer(canonical=True).render(target)
        display = TypeRenderer().render(target)
        row = rows.setdefault(
            key,
            {
                "displays": set(),
                "traits": set(),
                "kind": next(iter(target)),
                "target": target,
            },
        )
        row["displays"].add(display)
        row["traits"].add(trait_name)

    for row in rows.values():
        for pattern, trait_name in implementations:
            if type_pattern_matches(pattern, row["target"]):
                row["traits"].add(trait_name)

    return rows


def display_for(row: dict[str, Any]) -> str:
    def preference(display: str) -> tuple[int, int, str]:
        conventional_t = bool(re.search(r"(?:<|[(,; ])T(?:[>,); ]|$)", display))
        return (not conventional_t, len(display), display.casefold())

    return min(row["displays"], key=preference)


def render_markdown(rows: dict[str, dict[str, Any]]) -> str:
    kind_order = {
        "primitive": 0,
        "borrowed_ref": 1,
        "array": 2,
        "tuple": 3,
        "resolved_path": 4,
    }

    def natural_key(text: str) -> tuple[Any, ...]:
        return tuple(
            int(part) if part.isdigit() else part.casefold()
            for part in re.split(r"(\d+)", text)
        )

    def row_key(row: dict[str, Any]) -> tuple[Any, ...]:
        detail: tuple[Any, ...]
        if row["kind"] == "tuple":
            detail = (len(row["target"]["tuple"]),)
        else:
            detail = natural_key(display_for(row))
        return (kind_order.get(row["kind"], 5), detail)

    ordered = sorted(rows.values(), key=row_key)

    table = [["Implementation target", *TRAITS]]
    for row in ordered:
        target = display_for(row).replace("|", r"\|")
        marks = ["✓" if trait in row["traits"] else "—" for trait in TRAITS]
        table.append([f"`{target}`", *marks])

    widths = [max(len(row[index]) for row in table) for index in range(len(table[0]))]
    lines = [
        "# DuckDB Rust type coverage",
        "",
        "Generated from `duckdb_rs` rustdoc JSON.",
        "",
        "| " + " | ".join(cell.ljust(widths[index]) for index, cell in enumerate(table[0])) + " |",
        "| "
        + " | ".join(
            "-" * width if index == 0 else "-" * (width - 1) + ":"
            for index, width in enumerate(widths)
        )
        + " |",
    ]
    for row in table[1:]:
        cells = [
            cell.ljust(widths[index]) if index == 0 else cell.rjust(widths[index])
            for index, cell in enumerate(row)
        ]
        lines.append("| " + " | ".join(cells) + " |")
    lines.append("")
    return "\n".join(lines)


def generate_rustdoc(repo_root: Path) -> Path:
    command = [
        "cargo",
        "+nightly",
        "rustdoc",
        "-p",
        "duckdb-rs",
        "--lib",
        "--",
        "-Z",
        "unstable-options",
        "--output-format",
        "json",
    ]
    print("+ " + " ".join(command), file=sys.stderr)
    subprocess.run(command, cwd=repo_root, check=True, stdout=sys.stderr)
    return repo_root / "target" / "doc" / "duckdb_rs.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a Markdown matrix of DuckDBType, ToValue, FromValue, "
            "VectorElement, and WritableVectorElement implementations."
        ),
        epilog=(
            "Without --input, the script runs cargo +nightly rustdoc because "
            "rustdoc JSON is not available on stable Rust."
        ),
    )
    parser.add_argument(
        "--input",
        type=Path,
        metavar="JSON",
        help="reuse an existing rustdoc JSON file instead of invoking cargo",
    )
    parser.add_argument(
        "--output",
        type=Path,
        metavar="MARKDOWN",
        help="write the matrix to this file instead of stdout",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]

    try:
        json_path = args.input or generate_rustdoc(repo_root)
        if not json_path.is_absolute():
            json_path = repo_root / json_path
        with json_path.open(encoding="utf-8") as rustdoc_file:
            document = json.load(rustdoc_file)
        traits = discover_traits(document)
        coverage = collect_coverage(document, traits)
        markdown = render_markdown(coverage)
    except (OSError, json.JSONDecodeError, subprocess.CalledProcessError, RustdocError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    try:
        if args.output:
            output_path = args.output
            if not output_path.is_absolute():
                output_path = repo_root / output_path
            output_path.write_text(markdown, encoding="utf-8")
        else:
            sys.stdout.write(markdown)
    except OSError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
