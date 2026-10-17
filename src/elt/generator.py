"""Synthetic CDC feed from a loan servicing system.

Writes JSON-lines files in arrival batches, the way an upstream system would drop them in S3:

    data/batches/0001/loan_events/loan_events_0001_a.jsonl
    data/batches/0001/payments/payments_0001.jsonl
    ...

Each loan event carries the full account image after the change (Debezium-style "after").
The feed is deliberately imperfect, because production feeds are:

* duplicates: an event or payment is re-sent in a later batch
* late arrivals: some events and payments show up one to four batches after they happened
* invalid rows: missing keys, negative balances, unparseable timestamps, zero amounts
* a file re-delivered under the same name (Snowpipe must not load it twice)
* a corrupt file with a broken JSON line (Snowpipe skips the whole file)

All values are fictional. Same seed, same files.
"""

from __future__ import annotations

import argparse
import json
import random
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from pathlib import Path

STATES = ["TX", "CA", "FL", "GA", "OH", "MI", "NC", "AZ", "IL", "PA", "NY", "MA"]
CHANNELS = ["autopay", "web", "mobile", "phone", "branch"]
START = date(2026, 1, 1)


@dataclass
class Loan:
    loan_id: str
    customer_id: str
    state: str
    principal: float
    balance: float
    apr: float
    due_day: int
    opened_day: int
    status: str = "CURRENT"
    closed: bool = False


@dataclass
class Batch:
    number: int
    loan_events: list[dict] = field(default_factory=list)
    payments: list[dict] = field(default_factory=list)


def _ts(d: date, rng: random.Random) -> str:
    t = time(rng.randint(6, 21), rng.randint(0, 59), rng.randint(0, 59))
    return datetime.combine(d, t).strftime("%Y-%m-%d %H:%M:%S")


def _image(loan: Loan, event_type: str, ts: str, event_id: str) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_ts": ts,
        "loan_id": loan.loan_id,
        "customer_id": loan.customer_id,
        "state": loan.state,
        "principal": f"{loan.principal:.2f}",
        "balance": f"{loan.balance:.2f}",
        "apr": f"{loan.apr:.4f}",
        "status": loan.status,
    }


def generate(days: int = 90, new_loans_per_day: int = 25, seed: int = 7) -> list[Batch]:
    """One batch per day of activity. Returns batches in arrival order."""
    rng = random.Random(seed)
    loans: list[Loan] = []
    batches = [Batch(number=i + 1) for i in range(days + 5)]  # spare batches for late arrivals
    seq = 0

    def emit(kind: str, day_index: int, row: dict) -> None:
        """Deliver on time, late, or twice."""
        delay = rng.choice([1, 2, 3, 4]) if rng.random() < 0.04 else 0
        getattr(batches[day_index + delay], kind).append(row)
        if rng.random() < 0.02:
            getattr(batches[min(day_index + delay + rng.randint(1, 3), len(batches) - 1)], kind).append(dict(row))

    for di in range(days):
        d = START + timedelta(days=di)

        for _ in range(new_loans_per_day):
            seq += 1
            principal = round(rng.uniform(9_000, 62_000), 2)
            loan = Loan(
                loan_id=f"L{100000 + seq}",
                customer_id=f"C{rng.randint(10000, 99999)}",
                state=rng.choice(STATES),
                principal=principal,
                balance=principal,
                apr=round(rng.uniform(0.039, 0.219), 4),
                due_day=rng.randint(1, 28),
                opened_day=di,
            )
            loans.append(loan)
            emit("loan_events", di, _image(loan, "opened", _ts(d, rng), f"E{seq:06d}-0"))

        for loan in loans:
            if loan.closed or d.day != loan.due_day or di <= loan.opened_day:
                continue
            due = d
            installment = round(min(loan.balance, loan.principal / 60 + loan.balance * loan.apr / 12), 2)
            roll = rng.random()
            if roll < 0.04:  # skipped payment
                loan.status = "DELINQUENT"
                emit("loan_events", di, _image(loan, "status_change", _ts(d, rng), f"{loan.loan_id}-{di}-s"))
                continue
            paid_offset = 0 if roll < 0.80 else rng.randint(1, 20)
            pay_di = min(di + paid_offset, days - 1)
            paid = START + timedelta(days=pay_di)
            if roll > 0.985:  # early payoff
                installment = loan.balance
            loan.balance = round(loan.balance - installment, 2)
            loan.status = "PAID_OFF" if loan.balance <= 0 else "CURRENT"
            loan.balance = max(loan.balance, 0.0)
            pid = f"P{loan.loan_id[1:]}{di:03d}"
            emit(
                "payments",
                pay_di,
                {
                    "payment_id": pid,
                    "loan_id": loan.loan_id,
                    "amount": f"{installment:.2f}",
                    "due_date": due.isoformat(),
                    "paid_date": paid.isoformat(),
                    "channel": rng.choice(CHANNELS),
                },
            )
            emit("loan_events", pay_di, _image(loan, "balance_update", _ts(paid, rng), f"{pid}-b"))
            if loan.status == "PAID_OFF":
                loan.closed = True
                emit("loan_events", pay_di, _image(loan, "closed", f"{paid.isoformat()} 23:59:59", f"{pid}-c"))

    # Invalid rows, sprinkled through the feed.
    for b in batches:
        if rng.random() < 0.5 and b.loan_events:
            bad = dict(rng.choice(b.loan_events))
            problem = rng.choice(["loan_id", "event_ts", "balance"])
            if problem == "loan_id":
                bad.pop("loan_id")
            elif problem == "event_ts":
                bad["event_ts"] = "31/02/2026 25:00"
            else:
                bad["balance"] = "-125.00"
            bad["event_id"] += "-bad"
            b.loan_events.append(bad)
        if rng.random() < 0.3 and b.payments:
            bad = dict(rng.choice(b.payments))
            bad["payment_id"] += "X"
            bad["amount"] = "0.00"
            b.payments.append(bad)
    for b in batches:
        rng.shuffle(b.loan_events)
    return [b for b in batches if b.loan_events or b.payments]


def write(batches: list[Batch], out: Path) -> None:
    if out.exists():
        shutil.rmtree(out)
    for b in batches:
        base = out / f"{b.number:04d}"
        # Upstream splits large extracts into parts.
        half = len(b.loan_events) // 2
        parts = {"a": b.loan_events[:half], "b": b.loan_events[half:]} if half else {"a": b.loan_events}
        for suffix, rows in parts.items():
            _jsonl(base / "loan_events" / f"loan_events_{b.number:04d}_{suffix}.jsonl", rows)
        if b.payments:
            _jsonl(base / "payments" / f"payments_{b.number:04d}.jsonl", b.payments)

    numbers = [b.number for b in batches]
    # A file re-delivered under the same name in a later batch: must not be loaded twice.
    src = out / f"{numbers[10]:04d}" / "payments" / f"payments_{numbers[10]:04d}.jsonl"
    dst = out / f"{numbers[12]:04d}" / "payments" / src.name
    if src.exists():
        shutil.copy(src, dst)
    # A partial upload: a copy of a good file with its last line cut off. Snowpipe skips it.
    good = out / f"{numbers[20]:04d}" / "loan_events" / f"loan_events_{numbers[20]:04d}_a.jsonl"
    text = good.read_text()
    (good.parent / f"loan_events_{numbers[20]:04d}_retry.jsonl").write_text(text[: len(text) - 40])


def _jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(r, separators=(",", ":")) + "\n" for r in rows))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--loans-per-day", type=int, default=25)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", type=Path, default=Path("data/batches"))
    args = ap.parse_args()
    batches = generate(args.days, args.loans_per_day, args.seed)
    write(batches, args.out)
    print(
        f"{len(batches)} batches, {sum(len(b.loan_events) for b in batches):,} loan events, "
        f"{sum(len(b.payments) for b in batches):,} payments -> {args.out}"
    )


if __name__ == "__main__":
    main()
