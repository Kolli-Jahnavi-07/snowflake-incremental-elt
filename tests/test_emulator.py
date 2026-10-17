"""The emulator must behave like Snowflake where the pipeline depends on it."""

import json

import pytest

from elt.local.engine import LocalSnowflake


@pytest.fixture()
def sf():
    return LocalSnowflake()


def put(stage, prefix, name, rows):
    p = stage / prefix / name
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("".join(json.dumps(r) + "\n" for r in rows))


def event(loan="L1", ts="2026-01-05 10:00:00", balance="1000.00", status="CURRENT", etype="opened"):
    return {
        "event_type": etype,
        "event_ts": ts,
        "loan_id": loan,
        "customer_id": "C1",
        "state": "OH",
        "principal": "1000.00",
        "balance": balance,
        "apr": "0.0500",
        "status": status,
    }


def test_snowpipe_loads_each_file_once_and_skips_bad_files(sf, tmp_path):
    put(tmp_path, "loan_events/", "a.jsonl", [event()])
    (tmp_path / "loan_events" / "broken.jsonl").write_text('{"loan_id": "L2"}\n{"loan_id": ')
    first = {r.file_name: r.status for r in sf.ingest(tmp_path)}
    assert first == {"loan_events/a.jsonl": "LOADED", "loan_events/broken.jsonl": "LOAD_FAILED"}
    second = {r.status for r in sf.ingest(tmp_path)}
    assert second == {"SKIPPED_ALREADY_LOADED"}
    assert sf.sql("SELECT COUNT(*) FROM RAW.RAW_LOAN_EVENTS") == [(1,)]


def test_root_condition_skips_runs_when_streams_are_empty(sf):
    assert sf.run_graph().state == "SKIPPED"


def test_reading_a_stream_does_not_consume_it(sf, tmp_path):
    put(tmp_path, "loan_events/", "a.jsonl", [event()])
    sf.ingest(tmp_path)
    assert len(sf.read_stream("RAW.RAW_LOAN_EVENTS_STREAM")) == 1
    assert sf.has_data("RAW.RAW_LOAN_EVENTS_STREAM")
    sf.run_graph()
    assert not sf.has_data("RAW.RAW_LOAN_EVENTS_STREAM")


def test_standard_stream_reports_update_as_delete_plus_insert(sf, tmp_path):
    put(tmp_path, "loan_events/", "1.jsonl", [event()])
    sf.ingest(tmp_path)
    sf.run_graph()
    put(tmp_path, "loan_events/", "2.jsonl", [event(ts="2026-02-05 10:00:00", balance="900.00", etype="balance_update")])
    sf.ingest(tmp_path)
    sf.fail_next_run_of("CORE.T_LOAN_HISTORY")  # keep the stream unconsumed so we can look at it
    sf.run_graph()
    rows = sf.read_stream("CORE.LOAN_ACCOUNTS_STREAM")
    meta = sorted((r[-2], r[-1]) for r in rows)
    assert meta == [("DELETE", True), ("INSERT", True)]


def test_failed_task_keeps_its_offset_and_cancels_descendants(sf, tmp_path):
    put(tmp_path, "loan_events/", "1.jsonl", [event()])
    sf.ingest(tmp_path)
    sf.fail_next_run_of("CORE.T_LOAD_ACCOUNTS")
    run = sf.run_graph()
    assert run.state == "FAILED"
    assert run.task("CORE.T_LOAD_ACCOUNTS").state == "FAILED"
    assert run.task("CORE.T_LOAN_HISTORY").state == "CANCELLED"
    assert run.task("CORE.T_LOAD_PAYMENTS").state == "SUCCEEDED"  # other branch unaffected
    assert sf.sql("SELECT COUNT(*) FROM CORE.LOAN_ACCOUNTS") == [(0,)]
    assert sf.has_data("RAW.RAW_LOAN_EVENTS_STREAM")
    # The root condition covers the unconsumed stream, so the next scheduled tick retries it.
    retry = sf.run_graph()
    assert retry.state == "SUCCEEDED"
    assert sf.sql("SELECT COUNT(*) FROM CORE.LOAN_ACCOUNTS") == [(1,)]
    assert sf.sql("SELECT COUNT(*) FROM CORE.LOAN_ACCOUNT_HISTORY") == [(1,)]
    assert sf.run_graph().state == "SKIPPED"
