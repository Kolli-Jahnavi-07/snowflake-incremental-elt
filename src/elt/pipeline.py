"""Pipeline definition (snowflake/pipeline.yml) and rendering it to Snowflake DDL."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SNOWFLAKE_DIR = ROOT / "snowflake"


@dataclass(frozen=True)
class Pipe:
    name: str
    prefix: str
    table: str


@dataclass(frozen=True)
class Stream:
    name: str
    table: str
    append_only: bool
    key: str | None = None


@dataclass(frozen=True)
class Task:
    name: str
    sql_path: Path
    after: tuple[str, ...] = ()
    root: bool = False
    when_streams_have_data: tuple[str, ...] = ()

    @property
    def body(self) -> str:
        return self.sql_path.read_text().strip().rstrip(";")


@dataclass
class Pipeline:
    database: str
    schedule: str
    warehouse: str | None
    serverless_initial_size: str
    task_timeout_ms: int
    suspend_after_failures: int
    pipes: list[Pipe] = field(default_factory=list)
    streams: list[Stream] = field(default_factory=list)
    tasks: list[Task] = field(default_factory=list)

    @classmethod
    def load(cls, path: Path = SNOWFLAKE_DIR / "pipeline.yml") -> Pipeline:
        cfg = yaml.safe_load(path.read_text())
        base = path.parent
        p = cls(
            database=cfg["database"],
            schedule=cfg["schedule"],
            warehouse=cfg.get("warehouse"),
            serverless_initial_size=cfg.get("serverless_initial_size", "XSMALL"),
            task_timeout_ms=int(cfg.get("task_timeout_ms", 3_600_000)),
            suspend_after_failures=int(cfg.get("suspend_after_failures", 0)),
            pipes=[Pipe(**x) for x in cfg.get("pipes", [])],
            streams=[Stream(**x) for x in cfg.get("streams", [])],
            tasks=[
                Task(
                    name=t["name"],
                    sql_path=base / t["sql"],
                    after=tuple(t.get("after", [])),
                    root=bool(t.get("root", False)),
                    when_streams_have_data=tuple(t.get("when_streams_have_data", [])),
                )
                for t in cfg["tasks"]
            ],
        )
        p.validate()
        return p

    # ------------------------------------------------------------------ checks
    def validate(self) -> None:
        names = {t.name for t in self.tasks}
        roots = [t for t in self.tasks if t.root]
        if len(roots) != 1:
            raise ValueError(f"exactly one root task required, found {len(roots)}")
        for t in self.tasks:
            if t.root and t.after:
                raise ValueError(f"root task {t.name} cannot have predecessors")
            if not t.root and not t.after:
                raise ValueError(f"task {t.name} is not connected to the graph")
            if t.when_streams_have_data and not t.root:
                raise ValueError(f"{t.name}: conditions belong on the root task only")
            for a in t.after:
                if a not in names:
                    raise ValueError(f"{t.name} runs after unknown task {a}")
            if not t.sql_path.exists():
                raise ValueError(f"{t.name}: missing {t.sql_path}")
        stream_names = [s.name for s in self.streams]
        if len(stream_names) != len(set(stream_names)):
            raise ValueError("duplicate stream names")
        # A stream's offset moves whenever a DML statement that reads it commits, so two tasks
        # reading the same stream would each see only part of the changes.
        consumers: dict[str, str] = {}
        for t in self.tasks:
            for s in self.streams_read_by(t):
                if s in consumers:
                    raise ValueError(f"stream {s} is consumed by both {consumers[s]} and {t.name}")
                consumers[s] = t.name
        list(self.topological_order())  # raises on cycles
        root = self.root()
        missing = sorted(set(consumers) - set(root.when_streams_have_data) - set(self.streams_read_by(root)))
        if root.when_streams_have_data and missing:
            raise ValueError(
                f"root condition must cover every consumed stream, or a failed task is not retried "
                f"until new data arrives: missing {', '.join(missing)}"
            )

    def streams_read_by(self, task: Task) -> list[str]:
        body = task.body.upper()
        return [s.name for s in self.streams if re.search(rf"(?<![\w.]){re.escape(s.name.upper())}(?!\w)", body)]

    def topological_order(self) -> list[Task]:
        by_name = {t.name: t for t in self.tasks}
        order = TopologicalSorter({t.name: set(t.after) for t in self.tasks}).static_order()
        return [by_name[n] for n in order]

    def root(self) -> Task:
        return next(t for t in self.tasks if t.root)

    def descendants(self, name: str) -> set[str]:
        out: set[str] = set()
        frontier = [name]
        while frontier:
            cur = frontier.pop()
            for t in self.tasks:
                if cur in t.after and t.name not in out:
                    out.add(t.name)
                    frontier.append(t.name)
        return out

    # ------------------------------------------------------------------ render
    def render(self) -> str:
        """Snowflake DDL for streams and the task graph, in dependency order."""
        out = [
            "-- Generated by `python -m elt render` from snowflake/pipeline.yml. Do not edit by hand.",
            f"USE DATABASE {self.database};",
            "",
            "-- Suspend the graph while it is (re)defined; children must be resumed before the root.",
            f"ALTER TASK IF EXISTS {self.root().name} SUSPEND;",
            "",
        ]
        for s in self.streams:
            mode = " APPEND_ONLY = TRUE" if s.append_only else ""
            out.append(f"CREATE STREAM IF NOT EXISTS {s.name} ON TABLE {s.table}{mode};")
        out.append("")
        for t in self.topological_order():
            lines = [f"CREATE OR REPLACE TASK {t.name}"]
            if self.warehouse:
                lines.append(f"  WAREHOUSE = {self.warehouse}")
            else:
                lines.append(f"  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = '{self.serverless_initial_size}'")
            lines.append(f"  USER_TASK_TIMEOUT_MS = {self.task_timeout_ms}")
            if t.root:
                lines.append(f"  SCHEDULE = '{self.schedule}'")
                if self.suspend_after_failures:
                    lines.append(f"  SUSPEND_TASK_AFTER_NUM_FAILURES = {self.suspend_after_failures}")
            else:
                lines.append(f"  AFTER {', '.join(t.after)}")
            if t.when_streams_have_data:
                cond = " OR ".join(f"SYSTEM$STREAM_HAS_DATA('{s}')" for s in t.when_streams_have_data)
                lines.append(f"  WHEN {cond}")
            lines.append("AS")
            lines.append(t.body + ";")
            out.append("\n".join(lines))
            out.append("")
        out.append("-- Resume children first, then the root (only a resumed root starts runs).")
        for t in reversed(self.topological_order()):
            out.append(f"ALTER TASK {t.name} RESUME;")
        return "\n".join(out) + "\n"
