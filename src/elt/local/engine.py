"""A small Snowflake emulator on DuckDB: Snowpipe, streams and task graphs.

It exists so the pipeline's SQL and its incremental behaviour can be tested on every commit
without a Snowflake account. It models the parts of Snowflake the pipeline relies on:

* **Snowpipe**: files under a stage prefix are loaded once. Load history is kept per pipe, so a
  file re-delivered under the same name is skipped; with ON_ERROR = SKIP_FILE a file with a bad
  record is skipped whole.
* **Streams**: each stream remembers an offset (here, a snapshot of its source table). Reading it
  returns the net change since the offset with METADATA$ACTION / METADATA$ISUPDATE. Append-only
  streams return inserted rows only. The offset moves only when a DML statement that read the
  stream commits; a failed task leaves it where it was.
* **Task graphs**: the root runs only when its WHEN condition holds; children run after their
  predecessors succeed. A failed task cancels its descendants; other branches still run.

What it does not model: time travel, retention/staleness, concurrency, or row-id based stream
tracking (standard streams diff on a declared key instead).
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import duckdb

from ..pipeline import SNOWFLAKE_DIR, Pipeline, Stream, Task
from .dialect import to_duckdb

TABLE_FILES = ["tables/raw.sql", "tables/core.sql", "tables/mart.sql"]


@dataclass
class LoadResult:
    pipe: str
    file_name: str
    status: str  # LOADED | LOAD_FAILED | SKIPPED_ALREADY_LOADED
    rows: int = 0
    error: str | None = None


@dataclass
class TaskRun:
    task: str
    state: str  # SUCCEEDED | FAILED | CANCELLED
    rows_affected: int = 0
    error: str | None = None


@dataclass
class GraphRun:
    run_id: str
    state: str  # SKIPPED (root condition false) | SUCCEEDED | FAILED
    tasks: list[TaskRun] = field(default_factory=list)

    def task(self, name: str) -> TaskRun:
        return next(t for t in self.tasks if t.task == name)


def _q(name: str) -> str:
    """Quote a SCHEMA.OBJECT name for DuckDB."""
    return ".".join(f'"{p}"' for p in name.split("."))


def _snap(stream: str) -> str:
    return f'"_streams"."{stream.replace(".", "__")}"'


class LocalSnowflake:
    def __init__(self, pipeline: Pipeline | None = None, database: str = ":memory:"):
        self.pipeline = pipeline or Pipeline.load()
        self.con = duckdb.connect(database)
        self._fail_next: set[str] = set()
        self._setup()

    # ------------------------------------------------------------------ setup
    def _setup(self) -> None:
        for schema in ("RAW", "CORE", "MART", "_streams", "_meta"):
            self.con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        for rel in TABLE_FILES:
            for stmt in to_duckdb((SNOWFLAKE_DIR / rel).read_text()):
                self.con.execute(stmt)
        self.con.execute(
            'CREATE TABLE IF NOT EXISTS "_meta".load_history '
            "(pipe TEXT, file_name TEXT, status TEXT, row_count INT, error TEXT, last_load_time TIMESTAMP)"
        )
        self.con.execute(
            'CREATE TABLE IF NOT EXISTS "_meta".task_history '
            "(run_id TEXT, task TEXT, state TEXT, rows_affected BIGINT, error TEXT, completed_time TIMESTAMP)"
        )
        for s in self.pipeline.streams:
            # CREATE STREAM: the initial offset is "now", so existing rows are not returned.
            self.con.execute(f"CREATE TABLE IF NOT EXISTS {_snap(s.name)} AS SELECT * FROM {_q(s.table)}")

    def sql(self, query: str) -> list[tuple]:
        return self.con.execute(query).fetchall()

    # ------------------------------------------------------------------ Snowpipe
    def ingest(self, stage_root: Path) -> list[LoadResult]:
        """Load new files from every pipe's prefix (what S3 event notifications trigger)."""
        results = []
        for pipe in self.pipeline.pipes:
            loaded = {r[0] for r in self.sql(f"SELECT file_name FROM \"_meta\".load_history WHERE pipe = '{pipe.name}'")}
            for path in sorted((stage_root / pipe.prefix).glob("*")):
                rel = f"{pipe.prefix}{path.name}"
                if rel in loaded:
                    results.append(LoadResult(pipe.name, rel, "SKIPPED_ALREADY_LOADED"))
                    continue
                results.append(self._copy_file(pipe.name, pipe.table, path, rel))
        return results

    def _copy_file(self, pipe: str, table: str, path: Path, rel: str) -> LoadResult:
        rows = []
        line_no = 0
        try:
            for line in path.read_text().splitlines():
                line_no += 1
                if line.strip():
                    rows.append(json.dumps(json.loads(line)))
        except json.JSONDecodeError as e:
            res = LoadResult(pipe, rel, "LOAD_FAILED", 0, f"line {line_no}: {e.msg}")
        else:
            now = datetime.now()
            if rows:
                self.con.executemany(
                    f"INSERT INTO {_q(table)} (payload, file_name, loaded_at) VALUES (?, ?, ?)",
                    [(r, rel, now) for r in rows],
                )
            res = LoadResult(pipe, rel, "LOADED", len(rows))
        self.con.execute(
            'INSERT INTO "_meta".load_history VALUES (?, ?, ?, ?, ?, ?)',
            [pipe, rel, res.status, res.rows, res.error, datetime.now()],
        )
        return res

    # ------------------------------------------------------------------ streams
    def _stream(self, name: str) -> Stream:
        return next(s for s in self.pipeline.streams if s.name == name)

    def _delta_sql(self, s: Stream) -> str:
        src, snap = _q(s.table), _snap(s.name)
        if s.append_only:
            return (
                f'SELECT *, \'INSERT\' AS "METADATA$ACTION", FALSE AS "METADATA$ISUPDATE" '
                f"FROM (SELECT * FROM {src} EXCEPT ALL SELECT * FROM {snap})"
            )
        if not s.key:
            raise ValueError(f"standard stream {s.name} needs a key for local emulation")
        k = f'"{s.key}"'
        changed = (
            f"(SELECT {k} FROM (SELECT * FROM {src} EXCEPT SELECT * FROM {snap}) "
            f"UNION SELECT {k} FROM (SELECT * FROM {snap} EXCEPT SELECT * FROM {src}))"
        )
        return (
            f"SELECT n.*, 'INSERT' AS \"METADATA$ACTION\", "
            f'n.{k} IN (SELECT {k} FROM {snap}) AS "METADATA$ISUPDATE" '
            f"FROM {src} n WHERE n.{k} IN {changed} "
            f"UNION ALL "
            f"SELECT o.*, 'DELETE' AS \"METADATA$ACTION\", "
            f'o.{k} IN (SELECT {k} FROM {src}) AS "METADATA$ISUPDATE" '
            f"FROM {snap} o WHERE o.{k} IN {changed}"
        )

    def read_stream(self, name: str) -> list[tuple]:
        """SELECT * FROM <stream> without consuming it (a plain query never moves the offset)."""
        return self.sql(self._delta_sql(self._stream(name)))

    def has_data(self, name: str) -> bool:
        """SYSTEM$STREAM_HAS_DATA."""
        return bool(self.sql(f"SELECT EXISTS ({self._delta_sql(self._stream(name))})")[0][0])

    # ------------------------------------------------------------------ tasks
    def fail_next_run_of(self, task: str) -> None:
        """Test hook: make the next execution of `task` fail as if its SQL had errored."""
        self._fail_next.add(task)

    def execute_task(self, task: Task) -> int:
        streams = [self._stream(n) for n in self.pipeline.streams_read_by(task)]
        # Materialise each stream's delta under the stream's own name so the task SQL runs unchanged.
        for s in streams:
            self.con.execute(f"CREATE OR REPLACE TABLE {_q(s.name)} AS {self._delta_sql(s)}")
        affected = 0
        try:
            self.con.execute("BEGIN TRANSACTION")
            if task.name in self._fail_next:
                self._fail_next.discard(task.name)
                raise RuntimeError(f"injected failure in {task.name}")
            for stmt in to_duckdb(task.body):
                res = self.con.execute(stmt).fetchall()
                affected += sum(r[0] for r in res if r and isinstance(r[0], int))
            # The DML committed, so every stream it read advances to the state it read.
            for s in streams:
                self._advance(s)
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        finally:
            for s in streams:
                self.con.execute(f"DROP TABLE IF EXISTS {_q(s.name)}")
        return affected

    def _advance(self, s: Stream) -> None:
        self.con.execute(f"DELETE FROM {_snap(s.name)}")
        self.con.execute(f"INSERT INTO {_snap(s.name)} SELECT * FROM {_q(s.table)}")

    def run_graph(self) -> GraphRun:
        """One scheduled run of the task graph (EXECUTE TASK <root>)."""
        run = GraphRun(run_id=uuid.uuid4().hex[:12], state="SUCCEEDED")
        root = self.pipeline.root()
        if root.when_streams_have_data and not any(self.has_data(s) for s in root.when_streams_have_data):
            run.state = "SKIPPED"
            return run
        failed: set[str] = set()
        cancelled: set[str] = set()
        for task in self.pipeline.topological_order():
            if task.name in cancelled:
                run.tasks.append(TaskRun(task.name, "CANCELLED"))
                continue
            try:
                n = self.execute_task(task)
                run.tasks.append(TaskRun(task.name, "SUCCEEDED", n))
            except Exception as e:  # noqa: BLE001 - recorded in task history, like Snowflake
                run.tasks.append(TaskRun(task.name, "FAILED", error=str(e).splitlines()[0]))
                failed.add(task.name)
                cancelled |= self.pipeline.descendants(task.name)
        if failed:
            run.state = "FAILED"
        now = datetime.now()
        self.con.executemany(
            'INSERT INTO "_meta".task_history VALUES (?, ?, ?, ?, ?, ?)',
            [(run.run_id, t.task, t.state, t.rows_affected, t.error, now) for t in run.tasks],
        )
        return run


def table_counts(sf: LocalSnowflake) -> dict[str, int]:
    names = [
        "RAW.RAW_LOAN_EVENTS",
        "RAW.RAW_PAYMENTS",
        "CORE.LOAN_ACCOUNTS",
        "CORE.LOAN_ACCOUNT_HISTORY",
        "CORE.PAYMENTS",
        "CORE.REJECTED_EVENTS",
        "MART.DAILY_COLLECTIONS",
    ]
    return {n: sf.sql(f"SELECT COUNT(*) FROM {_q(n)}")[0][0] for n in names}
