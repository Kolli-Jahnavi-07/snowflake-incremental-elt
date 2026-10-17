"""Command line.

python -m elt generate            # write the synthetic feed to data/batches/
python -m elt simulate            # replay it batch by batch through Snowpipe + the task graph (DuckDB)
python -m elt render              # print the Snowflake DDL for streams and tasks
python -m elt deploy              # run setup + tables + rendered DDL on Snowflake (needs SNOWFLAKE_* env)
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

from .pipeline import SNOWFLAKE_DIR, Pipeline


def simulate(batches_dir: Path, workdir: Path, verbose: bool = False) -> int:
    from .local.engine import LocalSnowflake, table_counts

    if workdir.exists():
        shutil.rmtree(workdir)
    stage = workdir / "stage"
    stage.mkdir(parents=True)
    sf = LocalSnowflake(database=str(workdir / "warehouse.duckdb"))
    batches = sorted(p for p in batches_dir.iterdir() if p.is_dir())
    if not batches:
        print(f"no batches in {batches_dir}; run `python -m elt generate` first", file=sys.stderr)
        return 1
    t0 = time.perf_counter()
    runs = {"SUCCEEDED": 0, "FAILED": 0, "SKIPPED": 0}
    load_status: dict[str, int] = {}
    for b in batches:
        # Upstream drops this batch's files into the bucket...
        shutil.copytree(b, stage, dirs_exist_ok=True)
        # ...S3 notifies Snowpipe...
        for r in sf.ingest(stage):
            if r.status != "SKIPPED_ALREADY_LOADED":
                load_status[r.status] = load_status.get(r.status, 0) + 1
            if verbose and r.status == "LOAD_FAILED":
                print(f"  {r.file_name}: {r.error} (file skipped)")
        # ...and the scheduled task graph picks up whatever the streams hold.
        run = sf.run_graph()
        runs[run.state] += 1
        if verbose:
            print(f"batch {b.name}: {run.state} " + ", ".join(f"{t.task.split('.')[-1]}={t.rows_affected}" for t in run.tasks))
    # One more tick with nothing new: the root's WHEN condition is false, so nothing runs.
    runs[sf.run_graph().state] += 1
    elapsed = time.perf_counter() - t0

    print(f"\n{len(batches)} batches in {elapsed:.1f} s")
    print("files:      " + ", ".join(f"{k.lower()} {v}" for k, v in sorted(load_status.items())))
    print("task runs:  " + ", ".join(f"{k.lower()} {v}" for k, v in runs.items()))
    for name, n in table_counts(sf).items():
        print(f"  {name:<28} {n:>7,}")
    rejects = sf.sql("SELECT reason, COUNT(*) FROM CORE.REJECTED_EVENTS GROUP BY 1 ORDER BY 2 DESC")
    print("rejected:   " + ", ".join(f"{r} ({n})" for r, n in rejects))
    return 0


def deploy(dry_run: bool) -> int:
    pipeline = Pipeline.load()
    scripts = [
        ("tables/raw.sql", (SNOWFLAKE_DIR / "tables/raw.sql").read_text()),
        ("setup/02_ingest.sql", (SNOWFLAKE_DIR / "setup/02_ingest.sql").read_text()),
        ("tables/core.sql", (SNOWFLAKE_DIR / "tables/core.sql").read_text()),
        ("tables/mart.sql", (SNOWFLAKE_DIR / "tables/mart.sql").read_text()),
        ("rendered pipeline", pipeline.render()),
    ]
    if dry_run:
        for name, sql in scripts:
            print(f"-- ===== {name}\n{sql}")
        return 0
    import os

    import snowflake.connector  # optional dependency: pip install snowflake-connector-python

    con = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        role=os.environ.get("SNOWFLAKE_ROLE", "LOANS_TRANSFORMER"),
        database=pipeline.database,
    )
    try:
        for name, sql in scripts:
            print(f"running {name}")
            for cur in con.execute_string(sql):
                cur.close()
    finally:
        con.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m elt")
    sub = ap.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate", help="write the synthetic feed")
    g.add_argument("--days", type=int, default=90)
    g.add_argument("--loans-per-day", type=int, default=25)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--out", type=Path, default=Path("data/batches"))
    s = sub.add_parser("simulate", help="replay the feed through the local emulator")
    s.add_argument("--batches", type=Path, default=Path("data/batches"))
    s.add_argument("--workdir", type=Path, default=Path("data/local"))
    s.add_argument("-v", "--verbose", action="store_true")
    sub.add_parser("render", help="print Snowflake DDL for streams and tasks")
    d = sub.add_parser("deploy", help="deploy to Snowflake")
    d.add_argument("--dry-run", action="store_true", help="print the SQL instead of running it")
    args = ap.parse_args(argv)

    if args.cmd == "generate":
        from .generator import generate, write

        batches = generate(args.days, args.loans_per_day, args.seed)
        write(batches, args.out)
        print(
            f"{len(batches)} batches, {sum(len(b.loan_events) for b in batches):,} loan events, "
            f"{sum(len(b.payments) for b in batches):,} payments -> {args.out}"
        )
        return 0
    if args.cmd == "simulate":
        return simulate(args.batches, args.workdir, args.verbose)
    if args.cmd == "render":
        print(Pipeline.load().render(), end="")
        return 0
    if args.cmd == "deploy":
        return deploy(args.dry_run)
    return 2


if __name__ == "__main__":
    sys.exit(main())
