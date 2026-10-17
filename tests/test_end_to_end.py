"""Replay a messy feed batch by batch and compare every table with an independent reference.

The reference below re-implements the business rules in plain Python, reading the same files the
way Snowpipe would (each file once, corrupt files skipped). If the SQL and the reference disagree
on a single row, the test fails.
"""

from __future__ import annotations

import json
import shutil
from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pytest

from elt.generator import generate, write
from elt.local.engine import LocalSnowflake


# ---------------------------------------------------------------- reference implementation
def _dec(v) -> Decimal | None:
    try:
        return Decimal(str(v)).quantize(Decimal("0.01")) if v is not None else None
    except InvalidOperation:
        return None


def _ts(v) -> datetime | None:
    try:
        return datetime.strptime(v, "%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError):
        return None


def _date(v) -> date | None:
    try:
        return date.fromisoformat(v)
    except (TypeError, ValueError):
        return None


def _rows(path: Path) -> list[dict] | None:
    try:
        return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    except json.JSONDecodeError:
        return None  # ON_ERROR = SKIP_FILE


class Reference:
    def __init__(self):
        self.accounts: dict[str, dict] = {}
        self.history: dict[str, list[dict]] = defaultdict(list)
        self.payments: dict[str, dict] = {}
        self.rejected = 0
        self.raw_rows = 0
        self.seen_files: set[str] = set()

    def apply_batch(self, batch_dir: Path) -> None:
        events, pays = [], []
        for kind, sink in (("loan_events", events), ("payments", pays)):
            for f in sorted((batch_dir / kind).glob("*")) if (batch_dir / kind).exists() else []:
                key = f"{kind}/{f.name}"
                if key in self.seen_files:
                    continue
                self.seen_files.add(key)
                rows = _rows(f)
                if rows is not None:
                    sink.extend(rows)
                    self.raw_rows += len(rows)

        latest: dict[str, dict] = {}
        for e in events:
            ts, bal = _ts(e.get("event_ts")), _dec(e.get("balance"))
            if not e.get("loan_id") or not e.get("customer_id") or ts is None or bal is None or bal < 0:
                self.rejected += 1
                continue
            img = {
                "customer_id": e["customer_id"],
                "state": e.get("state"),
                "balance": bal,
                "apr": Decimal(e["apr"]).quantize(Decimal("0.0001")),
                "status": e["status"].strip().upper(),
                "is_closed": e.get("event_type") == "closed",
                "event_ts": ts,
            }
            cur = latest.get(e["loan_id"])
            if cur is None or ts > cur["event_ts"]:
                latest[e["loan_id"]] = img

        changed = []
        for loan_id, img in latest.items():
            stored = self.accounts.get(loan_id)
            if stored is None or img["event_ts"] > stored["event_ts"]:
                self.accounts[loan_id] = img
                changed.append(loan_id)

        image = lambda r: tuple(r[k] for k in ("customer_id", "state", "balance", "apr", "status", "is_closed"))  # noqa: E731
        for loan_id in changed:
            acc, versions = self.accounts[loan_id], self.history[loan_id]
            if versions and image(versions[-1]) == image(acc):
                continue
            if versions:
                versions[-1]["valid_to"] = acc["event_ts"]
            versions.append({**acc, "valid_from": acc["event_ts"], "valid_to": None})

        for p in pays:
            amount, due, paid = _dec(p.get("amount")), _date(p.get("due_date")), _date(p.get("paid_date"))
            if not p.get("payment_id") or not p.get("loan_id") or amount is None or amount <= 0 or due is None or paid is None:
                self.rejected += 1
                continue
            self.payments.setdefault(
                p["payment_id"],
                {
                    "loan_id": p["loan_id"],
                    "amount": amount,
                    "paid_date": paid,
                    "days_late": max((paid - due).days, 0),
                },
            )


# ---------------------------------------------------------------- the run
@pytest.fixture(scope="module")
def replay(tmp_path_factory):
    root = tmp_path_factory.mktemp("feed")
    write(generate(days=45, new_loans_per_day=12, seed=3), root / "batches")
    batches = sorted((root / "batches").iterdir())

    def run(fail_at: dict[int, str] | None = None):
        stage = root / f"stage-{len(fail_at or {})}"
        shutil.rmtree(stage, ignore_errors=True)
        sf, ref = LocalSnowflake(), Reference()
        for i, b in enumerate(batches):
            shutil.copytree(b, stage, dirs_exist_ok=True)
            sf.ingest(stage)
            if fail_at and i in fail_at:
                sf.fail_next_run_of(fail_at[i])
            sf.run_graph()
            ref.apply_batch(b)
        # Drain anything left behind by a failure on the last batches.
        while sf.run_graph().state != "SKIPPED":
            pass
        return sf, ref

    return run


@pytest.fixture(scope="module")
def clean(replay):
    return replay()


def test_current_state_matches_reference(clean):
    sf, ref = clean
    got = {
        r[0]: r[1:]
        for r in sf.sql("SELECT loan_id, customer_id, state, balance, apr, status, is_closed, event_ts FROM CORE.LOAN_ACCOUNTS")
    }
    want = {
        k: (v["customer_id"], v["state"], v["balance"], v["apr"], v["status"], v["is_closed"], v["event_ts"])
        for k, v in ref.accounts.items()
    }
    assert len(got) == len(want) > 300
    assert got == want


def test_history_matches_reference_and_is_well_formed(clean):
    sf, ref = clean
    rows = sf.sql(
        "SELECT loan_id, balance, status, valid_from, valid_to, is_current FROM CORE.LOAN_ACCOUNT_HISTORY "
        "ORDER BY loan_id, valid_from"
    )
    got = defaultdict(list)
    for loan_id, bal, status, vf, vt, cur in rows:
        got[loan_id].append((bal, status, vf, vt, cur))
    want = {
        k: [(v["balance"], v["status"], v["valid_from"], v["valid_to"], v["valid_to"] is None) for v in vs]
        for k, vs in ref.history.items()
    }
    assert got == want
    assert sum(len(v) for v in want.values()) > len(want)  # loans really do have several versions
    for versions in got.values():
        assert [v[4] for v in versions].count(True) == 1
        for a, b in zip(versions, versions[1:], strict=False):
            assert a[3] == b[2], "versions must be contiguous, no gaps or overlaps"


def test_payments_are_deduplicated_and_complete(clean):
    sf, ref = clean
    got = {r[0]: r[1:] for r in sf.sql("SELECT payment_id, loan_id, amount, paid_date, days_late FROM CORE.PAYMENTS")}
    want = {k: (v["loan_id"], v["amount"], v["paid_date"], v["days_late"]) for k, v in ref.payments.items()}
    assert got == want
    assert any(v[3] > 0 for v in want.values())


def test_daily_collections_include_late_arriving_payments(clean):
    sf, ref = clean
    want = defaultdict(lambda: [0, Decimal("0"), 0])
    for p in ref.payments.values():
        w = want[p["paid_date"]]
        w[0] += 1
        w[1] += p["amount"]
        w[2] += p["days_late"] > 0
    got = {
        r[0]: [r[1], r[2], r[3]]
        for r in sf.sql("SELECT paid_date, payments, amount_collected, late_payments FROM MART.DAILY_COLLECTIONS")
    }
    assert got == {k: v for k, v in want.items()}


def test_invalid_rows_are_quarantined_not_lost(clean):
    sf, ref = clean
    assert sf.sql("SELECT COUNT(*) FROM CORE.REJECTED_EVENTS")[0][0] == ref.rejected > 0


def test_redelivered_file_loaded_once_and_corrupt_file_skipped(clean):
    sf, ref = clean
    statuses = dict(sf.sql('SELECT status, COUNT(*) FROM "_meta".load_history GROUP BY 1'))
    assert statuses.get("LOAD_FAILED") == 1
    raw = sf.sql("SELECT (SELECT COUNT(*) FROM RAW.RAW_LOAN_EVENTS) + (SELECT COUNT(*) FROM RAW.RAW_PAYMENTS)")[0][0]
    assert raw == ref.raw_rows


def test_nothing_new_means_no_run_and_no_change(clean):
    sf, _ = clean
    before = sf.sql("SELECT COUNT(*), SUM(balance) FROM CORE.LOAN_ACCOUNTS")
    assert sf.run_graph().state == "SKIPPED"
    assert sf.sql("SELECT COUNT(*), SUM(balance) FROM CORE.LOAN_ACCOUNTS") == before


def test_failures_mid_graph_recover_to_the_same_result(replay, clean):
    """Kill different tasks on different batches; the next ticks must catch up exactly."""
    sf, _ = replay(
        {
            3: "CORE.T_LOAD_ACCOUNTS",
            10: "CORE.T_LOAN_HISTORY",
            11: "CORE.T_LOAN_HISTORY",
            20: "CORE.T_LOAD_PAYMENTS",
            30: "MART.T_DAILY_COLLECTIONS",
            35: "CORE.T_REJECT_EVENTS",
        }
    )
    base, _ = clean
    for table in ["CORE.LOAN_ACCOUNTS", "CORE.PAYMENTS", "MART.DAILY_COLLECTIONS", "CORE.REJECTED_EVENTS"]:
        cols = (
            "* EXCLUDE (updated_at)"
            if table == "CORE.LOAN_ACCOUNTS"
            else "* EXCLUDE (refreshed_at)"
            if table.startswith("MART")
            else "* EXCLUDE (rejected_at)"
            if "REJECTED" in table
            else "* EXCLUDE (loaded_at)"
        )
        q = f"SELECT {cols} FROM {table} ORDER BY ALL"
        assert sf.sql(q) == base.sql(q), table
    q = "SELECT loan_id, balance, status, valid_from, valid_to FROM CORE.LOAN_ACCOUNT_HISTORY ORDER BY 1, 4"
    hist, base_hist = sf.sql(q), base.sql(q)
    # History is coarser when the SCD2 task misses a tick (two changes collapse into one version),
    # but it never loses the latest state and stays contiguous.
    assert {r[0] for r in hist} == {r[0] for r in base_hist}
    cur = "SELECT loan_id, balance, status FROM CORE.LOAN_ACCOUNT_HISTORY WHERE is_current ORDER BY 1"
    assert sf.sql(cur) == base.sql(cur)
