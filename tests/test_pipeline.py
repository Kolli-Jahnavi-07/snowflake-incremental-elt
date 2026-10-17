from dataclasses import replace
from graphlib import CycleError
from pathlib import Path

import pytest
import sqlglot

from elt.pipeline import SNOWFLAKE_DIR, Pipeline

GENERATED = SNOWFLAKE_DIR / "generated" / "pipeline.sql"


@pytest.fixture(scope="module")
def pipeline() -> Pipeline:
    return Pipeline.load()


def test_definition_is_valid(pipeline):
    order = [t.name.split(".")[-1] for t in pipeline.topological_order()]
    assert order[0] == "T_REJECT_EVENTS"
    assert order.index("T_LOAD_ACCOUNTS") < order.index("T_LOAN_HISTORY")
    assert order.index("T_LOAD_PAYMENTS") < order.index("T_DAILY_COLLECTIONS")


def test_each_stream_has_exactly_one_consumer(pipeline):
    consumers = {}
    for t in pipeline.tasks:
        for s in pipeline.streams_read_by(t):
            consumers.setdefault(s, []).append(t.name)
    assert set(consumers) == {s.name for s in pipeline.streams}
    assert all(len(v) == 1 for v in consumers.values())


def test_second_consumer_is_rejected(pipeline):
    t = pipeline.tasks[1]
    dup = replace(t, name="CORE.T_DUPLICATE")
    bad = replace(pipeline, tasks=[*pipeline.tasks, dup])
    with pytest.raises(ValueError, match="consumed by both"):
        bad.validate()


def test_cycles_and_dangling_tasks_are_rejected(pipeline):
    a, b = pipeline.tasks[1], pipeline.tasks[2]
    cyclic = [replace(a, after=(b.name,)) if t is a else t for t in pipeline.tasks]
    with pytest.raises(CycleError):
        replace(pipeline, tasks=cyclic).validate()
    orphan = [replace(b, after=()) if t is b else t for t in pipeline.tasks]
    with pytest.raises(ValueError, match="not connected"):
        replace(pipeline, tasks=orphan).validate()


def test_root_condition_must_cover_downstream_streams(pipeline):
    root = pipeline.root()
    narrowed = replace(root, when_streams_have_data=root.when_streams_have_data[:2])
    bad = replace(pipeline, tasks=[narrowed if t is root else t for t in pipeline.tasks])
    with pytest.raises(ValueError, match="CORE.LOAN_ACCOUNTS_STREAM"):
        bad.validate()


def test_render_produces_streams_and_task_graph(pipeline):
    sql = pipeline.render()
    assert "CREATE STREAM IF NOT EXISTS RAW.RAW_PAYMENTS_STREAM ON TABLE RAW.RAW_PAYMENTS APPEND_ONLY = TRUE;" in sql
    assert "CREATE STREAM IF NOT EXISTS CORE.LOAN_ACCOUNTS_STREAM ON TABLE CORE.LOAN_ACCOUNTS;" in sql
    assert "SCHEDULE = 'USING CRON */5 * * * * UTC'" in sql
    assert "WHEN SYSTEM$STREAM_HAS_DATA('RAW.RAW_LOAN_EVENTS_STREAM') OR SYSTEM$STREAM_HAS_DATA('RAW.RAW_PAYMENTS_STREAM')" in sql
    assert "SYSTEM$STREAM_HAS_DATA('CORE.LOAN_ACCOUNTS_STREAM')" in sql
    assert "AFTER CORE.T_LOAD_ACCOUNTS" in sql
    resumes = [line for line in sql.splitlines() if line.endswith("RESUME;")]
    assert resumes[-1] == "ALTER TASK CORE.T_REJECT_EVENTS RESUME;", "root must be resumed last"


def test_generated_ddl_is_up_to_date(pipeline):
    assert GENERATED.read_text() == pipeline.render(), "run `python -m elt render > snowflake/generated/pipeline.sql`"


@pytest.mark.parametrize("path", sorted(Path(SNOWFLAKE_DIR).glob("t*/*.sql")), ids=lambda p: f"{p.parent.name}/{p.name}")
def test_snowflake_sql_parses_strictly(path):
    statements = sqlglot.parse(path.read_text(), read="snowflake", error_level=sqlglot.ErrorLevel.RAISE)
    assert statements and all(s is not None for s in statements)
