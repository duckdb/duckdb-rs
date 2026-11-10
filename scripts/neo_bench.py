#!/usr/bin/env python3
"""Run the duckdb-neo `functions` benchmarks and compare two versions of the code.

`compare` builds a baseline git ref and the working tree (or a second ref), then runs
both benchmark binaries alternately on the same machine and compares their median times.
`run` benchmarks a single version. See crates/duckdb-neo/benches/README.md.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shutil
import statistics
import subprocess
import sys
import tarfile
from dataclasses import dataclass, field
from pathlib import Path

WORKSPACE_DIR = Path(__file__).resolve().parents[1]
BENCH_FILE = Path("crates/duckdb-neo/benches/functions.rs")
CARGO_BENCH = ["cargo", "bench", "-p", "duckdb-neo", "--bench", "functions"]
UNITS_NS = {"ps": 1e-3, "ns": 1.0, "µs": 1e3, "μs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9}
TREE_ROW = re.compile(r"^(?P<prefix>(?:[│ ]  )*)[├╰]─ (?P<name>\S+)")
DURATION = re.compile(r"^(?P<value>[0-9.]+)\s*(?P<unit>ps|ns|µs|μs|us|ms|s)$")


class BenchError(Exception):
    pass


@dataclass
class Tree:
    """A version of the code to benchmark: the working tree or an exported git ref."""

    label: str
    path: Path
    ref: str | None = None
    sha: str | None = None

    def describe(self) -> str:
        if self.ref is None:
            return f"working tree ({git('rev-parse', '--short', 'HEAD')}{' + changes' if dirty() else ''})"
        return f"{self.ref} ({self.sha[:10]})"

    def has_benchmarks(self) -> bool:
        return (self.path / BENCH_FILE).is_file()


@dataclass
class Options:
    filters: list[str]
    rows: str | None
    threads: str | None
    sample_count: int | None
    max_time: float | None
    locked: bool
    target_dir: Path
    log_dir: Path | None
    divan_args: list[str] = field(default_factory=list)


def git(*args: str, cwd: Path = WORKSPACE_DIR) -> str:
    result = subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    )
    return result.stdout.strip()


def dirty() -> bool:
    return bool(git("status", "--porcelain", "--untracked-files=no"))


def export_ref(ref: str, target_dir: Path) -> Tree:
    """Extract `ref` into the target directory, reusing a previous export of the same commit."""
    # CI checkouts only have remote-tracking branches, so `main` falls back to `origin/main`.
    for candidate in (ref, f"origin/{ref}"):
        try:
            sha = git("rev-parse", "--verify", "--quiet", f"{candidate}^{{commit}}")
            break
        except subprocess.CalledProcessError:
            continue
    else:
        raise BenchError(f"unknown git ref {ref!r}")
    path = target_dir / "neo-bench" / "trees" / sha
    marker = path / ".neo-bench-complete"
    if not marker.exists():
        shutil.rmtree(path, ignore_errors=True)
        path.mkdir(parents=True)
        archive = subprocess.Popen(
            ["git", "archive", "--format=tar", sha],
            cwd=WORKSPACE_DIR,
            stdout=subprocess.PIPE,
        )
        with tarfile.open(fileobj=archive.stdout, mode="r|") as tar:
            tar.extractall(path, filter="data")
        if archive.wait() != 0:
            raise BenchError(f"git archive {sha} failed")
        marker.touch()
    return Tree(label=ref, path=path, ref=ref, sha=sha)


def bench_env(options: Options) -> dict[str, str]:
    env = dict(
        os.environ, CARGO_TARGET_DIR=str(options.target_dir), CARGO_TERM_COLOR="never"
    )
    if options.rows:
        env["NEO_BENCH_ROWS"] = options.rows
    if options.threads:
        env["NEO_BENCH_THREADS"] = options.threads
    return env


def cargo_args(options: Options) -> list[str]:
    return [*CARGO_BENCH, *(["--locked"] if options.locked else [])]


def build(tree: Tree, options: Options) -> None:
    print(f"==> Building {tree.label}: {tree.describe()}", file=sys.stderr, flush=True)
    result = subprocess.run(
        [*cargo_args(options), "--no-run"],
        check=False,
        cwd=tree.path,
        env=bench_env(options),
    )
    if result.returncode != 0:
        raise BenchError(f"building the benchmarks of {tree.label} failed")


def divan_args(options: Options) -> list[str]:
    args = ["--color", "never", *options.filters]
    if options.sample_count is not None:
        args += ["--sample-count", str(options.sample_count)]
    if options.max_time is not None:
        args += ["--max-time", str(options.max_time)]
    return args + options.divan_args


def run_once(tree: Tree, options: Options, run: int) -> dict[str, float]:
    """Run the benchmarks of `tree` once and return the median time of each, in nanoseconds."""
    print(f"==> Run {run}: {tree.label}", file=sys.stderr, flush=True)
    command = [*cargo_args(options), "--", *divan_args(options)]
    result = subprocess.run(
        command,
        check=False,
        cwd=tree.path,
        env=bench_env(options),
        capture_output=True,
        encoding="utf-8",
        errors="replace",
    )
    if options.log_dir is not None:
        options.log_dir.mkdir(parents=True, exist_ok=True)
        log = (
            options.log_dir
            / f"run-{run:02}-{re.sub(r'[^A-Za-z0-9_.-]', '_', tree.label)}.txt"
        )
        log.write_text(result.stdout + result.stderr)
    if result.returncode != 0:
        sys.stderr.write(result.stdout[-4000:] + result.stderr[-4000:])
        raise BenchError(
            f"the benchmarks of {tree.label} failed (exit code {result.returncode})"
        )
    medians = parse_divan(result.stdout)
    if not medians:
        raise BenchError(
            f"no benchmark results in the output of {tree.label}; do the filters match anything?"
        )
    return medians


def parse_duration(text: str) -> float:
    match = DURATION.match(text.strip())
    if match is None:
        raise BenchError(f"cannot parse divan duration {text!r}")
    return float(match["value"]) * UNITS_NS[match["unit"]]


def parse_divan(output: str) -> dict[str, float]:
    """Map `group/benchmark/argument` names to the median column of divan's table."""
    medians: dict[str, float] = {}
    lines = output.splitlines()
    header = next(
        (
            index
            for index, line in enumerate(lines)
            if "fastest" in line and "median" in line
        ),
        None,
    )
    if header is None:
        return medians
    value_column = lines[header].index("fastest")
    columns = [column.strip() for column in lines[header][value_column:].split("│")]
    median_index = columns.index("median")
    path: list[str] = []
    for line in lines[header + 1 :]:
        row = TREE_ROW.match(line[:value_column])
        if row is None:
            # Throughput rows and blank lines carry no name.
            continue
        depth = len(row["prefix"]) // 3
        path[depth:] = [row["name"]]
        values = [value.strip() for value in line[value_column:].split("│")]
        if values[0]:
            medians["/".join(path)] = round(parse_duration(values[median_index]), 1)
    return medians


def format_duration(ns: float) -> str:
    for unit, scale in (("s", 1e9), ("ms", 1e6), ("µs", 1e3)):
        if ns >= scale:
            return f"{ns / scale:.3g} {unit}"
    return f"{ns:.3g} ns"


def machine_description() -> str:
    load = (
        ", ".join(f"{value:.2f}" for value in os.getloadavg())
        if hasattr(os, "getloadavg")
        else "n/a"
    )
    return f"{platform.system()} {platform.machine()}, {os.cpu_count()} cores, load average {load}"


@dataclass
class Comparison:
    name: str
    base: list[float]
    head: list[float]
    status: str = ""
    change: float | None = None

    def classify(self, threshold: float) -> None:
        if not self.base:
            self.status = "new"
            return
        if not self.head:
            self.status = "removed"
            return
        base, head = statistics.median(self.base), statistics.median(self.head)
        self.change = head / base - 1
        if self.change > threshold:
            # Every head run must be slower than every base run, so one noisy run cannot fail CI.
            self.status = (
                "regression" if min(self.head) > max(self.base) else "inconclusive"
            )
        elif self.change < -threshold:
            self.status = (
                "improvement" if max(self.head) < min(self.base) else "inconclusive"
            )
        else:
            self.status = "unchanged"

    def to_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "change": self.change,
            "base_ns": self.base,
            "head_ns": self.head,
            "base_median_ns": statistics.median(self.base) if self.base else None,
            "head_median_ns": statistics.median(self.head) if self.head else None,
        }


STATUS_ICONS = {
    "regression": "🔴 regression",
    "improvement": "🟢 improvement",
    "inconclusive": "🟡 inconclusive",
    "unchanged": "unchanged",
    "new": "new",
    "removed": "removed",
}


def spread(values: list[float]) -> str:
    if len(values) < 2:
        return ""
    return f" ±{(max(values) - min(values)) / 2 / statistics.median(values):.1%}"


def markdown_report(
    base: Tree | None,
    head: Tree,
    results: list[Comparison],
    threshold: float,
    runs: int,
    machine: str,
) -> str:
    lines = ["## duckdb-neo benchmarks", ""]
    lines.append(f"- Head: {head.describe()}")
    lines.append(
        f"- Base: {base.describe() if base else 'none (the base has no benchmarks)'}"
    )
    lines.append(f"- {runs} alternating run(s) per side on {machine} (before the runs)")
    lines.append(
        f"- A change beyond ±{threshold:.0%} counts only when every head run is outside the range of the base runs"
    )
    lines += [
        "",
        "| Benchmark | Base | Head | Change | Status |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        base_time = (
            format_duration(statistics.median(result.base)) + spread(result.base)
            if result.base
            else "–"
        )
        head_time = (
            format_duration(statistics.median(result.head)) + spread(result.head)
            if result.head
            else "–"
        )
        change = f"{result.change:+.1%}" if result.change is not None else "–"
        lines.append(
            f"| `{result.name}` | {base_time} | {head_time} | {change} | {STATUS_ICONS[result.status]} |"
        )
    counts = {
        status: sum(result.status == status for result in results)
        for status in STATUS_ICONS
    }
    summary = ", ".join(
        f"{count} {status}" for status, count in counts.items() if count
    )
    lines += ["", f"**Summary:** {summary}", ""]
    return "\n".join(lines)


def collect(
    trees: list[Tree], options: Options, repetitions: int
) -> dict[str, dict[str, list[float]]]:
    """Run every tree `repetitions` times, alternating their order to cancel drift."""
    samples: dict[str, dict[str, list[float]]] = {tree.label: {} for tree in trees}
    run = 0
    for repetition in range(repetitions):
        order = trees if repetition % 2 == 0 else list(reversed(trees))
        for tree in order:
            run += 1
            for name, median in run_once(tree, options, run).items():
                samples[tree.label].setdefault(name, []).append(median)
    return samples


def write_outputs(
    args: argparse.Namespace, report: str, payload: dict[str, object]
) -> None:
    print(report)
    if args.markdown:
        with open(args.markdown, "a", encoding="utf-8") as file:
            file.write(report + "\n")
    if args.json:
        Path(args.json).write_text(
            json.dumps(payload, indent=2) + "\n", encoding="utf-8"
        )


def tree_for(ref: str | None, options: Options) -> Tree:
    return (
        export_ref(ref, options.target_dir)
        if ref
        else Tree(label="working tree", path=WORKSPACE_DIR)
    )


def command_compare(args: argparse.Namespace, options: Options) -> int:
    head = tree_for(args.head, options)
    base = export_ref(args.base, options.target_dir)
    if base.ref == head.ref:
        base.label = f"base {base.label}"
    if not head.has_benchmarks():
        raise BenchError(f"{head.describe()} has no {BENCH_FILE}")
    trees = [base, head] if base.has_benchmarks() else [head]
    if len(trees) == 1:
        print(
            f"==> {base.describe()} has no benchmarks; only running {head.label}",
            file=sys.stderr,
        )
    for tree in trees:
        build(tree, options)

    machine = machine_description()
    samples = collect(trees, options, args.repetitions)
    base_samples = samples.get(base.label, {})
    head_samples = samples[head.label]
    names = list(head_samples) + [
        name for name in base_samples if name not in head_samples
    ]
    results = [
        Comparison(name, base_samples.get(name, []), head_samples.get(name, []))
        for name in names
    ]
    threshold = args.threshold / 100
    for result in results:
        result.classify(threshold)

    report = markdown_report(
        base if len(trees) == 2 else None,
        head,
        results,
        threshold,
        args.repetitions,
        machine,
    )
    payload = {
        "base": {"ref": base.ref, "sha": base.sha, "has_benchmarks": len(trees) == 2},
        "head": {
            "ref": head.ref,
            "sha": head.sha or git("rev-parse", "HEAD"),
            "dirty": head.ref is None and dirty(),
        },
        "threshold": threshold,
        "repetitions": args.repetitions,
        "machine": machine,
        "results": [result.to_json() for result in results],
    }
    write_outputs(args, report, payload)
    regressions = [result.name for result in results if result.status == "regression"]
    if regressions and not args.no_fail:
        print(
            f"==> {len(regressions)} regression(s): {', '.join(regressions)}",
            file=sys.stderr,
        )
        return 1
    return 0


def command_run(args: argparse.Namespace, options: Options) -> int:
    tree = tree_for(args.ref, options)
    build(tree, options)
    machine = machine_description()
    samples = collect([tree], options, args.repetitions)[tree.label]
    lines = [
        f"## duckdb-neo benchmarks: {tree.describe()}",
        "",
        f"- {args.repetitions} run(s) on {machine} (before the runs)",
    ]
    lines += ["", "| Benchmark | Median |", "| --- | ---: |"]
    for name, values in samples.items():
        lines.append(
            f"| `{name}` | {format_duration(statistics.median(values))}{spread(values)} |"
        )
    payload = {
        "ref": tree.ref,
        "sha": tree.sha or git("rev-parse", "HEAD"),
        "machine": machine,
        "results": [
            {"name": name, "median_ns": statistics.median(values), "runs_ns": values}
            for name, values in samples.items()
        ],
    }
    write_outputs(args, "\n".join(lines) + "\n", payload)
    return 0


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, Options]:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--filter",
        action="append",
        default=[],
        help="divan name filter, e.g. scalar or copy::base56_to (repeatable)",
    )
    common.add_argument(
        "--rows", help="comma-separated row counts, e.g. 1000000 (sets NEO_BENCH_ROWS)"
    )
    common.add_argument(
        "--threads",
        help="DuckDB threads per benchmark (sets NEO_BENCH_THREADS, default 1)",
    )
    common.add_argument(
        "--sample-count", type=int, help="divan samples per benchmark (default 100)"
    )
    common.add_argument(
        "--max-time", type=float, help="divan's time limit per benchmark, in seconds"
    )
    common.add_argument(
        "--quick",
        action="store_true",
        help="shorthand for --sample-count 20 and one run per side",
    )
    common.add_argument("--locked", action="store_true", help="pass --locked to cargo")
    common.add_argument(
        "--target-dir",
        type=Path,
        default=WORKSPACE_DIR / "target",
        help="cargo target directory shared by all builds",
    )
    common.add_argument(
        "--log-dir", type=Path, help="write each run's raw divan output here"
    )
    common.add_argument("--json", help="write the results as JSON to this file")
    common.add_argument(
        "--markdown",
        help="append the markdown report to this file, e.g. $GITHUB_STEP_SUMMARY",
    )
    common.add_argument("divan_args", nargs="*", help="extra divan arguments, after --")

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    compare = commands.add_parser(
        "compare",
        parents=[common],
        help="compare a base ref against the working tree or --head",
    )
    compare.add_argument(
        "--base", default="origin/main", help="baseline git ref (default origin/main)"
    )
    compare.add_argument(
        "--head", help="git ref to compare instead of the working tree"
    )
    compare.add_argument(
        "--repetitions",
        type=int,
        help="alternating runs per side (default 3, 1 with --quick)",
    )
    compare.add_argument(
        "--threshold",
        type=float,
        default=10.0,
        help="percent change that counts as a regression (default 10)",
    )
    compare.add_argument(
        "--no-fail", action="store_true", help="exit 0 even when regressions are found"
    )
    run = commands.add_parser(
        "run", parents=[common], help="benchmark the working tree or --ref"
    )
    run.add_argument("--ref", help="git ref to benchmark instead of the working tree")
    run.add_argument("--repetitions", type=int, help="runs (default 1)")

    args = parser.parse_args(argv)
    if args.quick and args.sample_count is None:
        args.sample_count = 20
    if args.repetitions is None:
        args.repetitions = 3 if args.command == "compare" and not args.quick else 1
    if args.repetitions < 1:
        parser.error("--repetitions must be at least 1")
    target_dir = args.target_dir.resolve()
    options = Options(
        filters=args.filter,
        rows=args.rows,
        threads=args.threads,
        sample_count=args.sample_count,
        max_time=args.max_time,
        locked=args.locked,
        target_dir=target_dir,
        log_dir=args.log_dir,
        divan_args=args.divan_args,
    )
    return args, options


def main(argv: list[str]) -> int:
    args, options = parse_args(argv)
    try:
        return (
            command_compare(args, options)
            if args.command == "compare"
            else command_run(args, options)
        )
    except BenchError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
