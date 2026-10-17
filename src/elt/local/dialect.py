"""Run Snowflake SQL on DuckDB.

sqlglot does most of the translation. A few Snowflake behaviours need help:

* ``payload:field::STRING`` must return the unquoted JSON scalar (DuckDB ``->>``), not the
  JSON-encoded value that a plain ``->`` + CAST gives.
* ``TRY_TO_TIMESTAMP_NTZ`` has no DuckDB counterpart; it becomes ``TRY_CAST(... AS TIMESTAMP)``.
* ``VARIANT`` columns become ``JSON``; PRIMARY KEY constraints are dropped (not enforced in Snowflake).
* Table options DuckDB doesn't know (``CHANGE_TRACKING``) are dropped, and so are comments.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

_TIMESTAMP_FUNCS = {"TRY_TO_TIMESTAMP_NTZ", "TRY_TO_TIMESTAMP", "TO_TIMESTAMP_NTZ"}


def _fix(node: exp.Expression) -> exp.Expression | None:
    if isinstance(node, exp.ColumnConstraint) and isinstance(node.args.get("kind"), exp.PrimaryKeyColumnConstraint):
        return None  # Snowflake doesn't enforce primary keys; tests check uniqueness instead
    if isinstance(node, exp.JSONExtract) and node.args.get("variant_extract"):
        return exp.JSONExtractScalar(this=node.this, expression=node.expression)
    if isinstance(node, exp.DataType) and node.this == exp.DataType.Type.VARIANT:
        return exp.DataType.build("JSON")
    if isinstance(node, exp.Anonymous) and node.name.upper() in _TIMESTAMP_FUNCS:
        return exp.TryCast(this=node.expressions[0].transform(_fix), to=exp.DataType.build("TIMESTAMP"))
    return node


def to_duckdb(sql: str) -> list[str]:
    """Translate one or more Snowflake statements to DuckDB SQL."""
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"\)\s*CHANGE_TRACKING\s*=\s*TRUE", ")", sql, flags=re.IGNORECASE)
    out = []
    for stmt in sqlglot.parse(sql, read="snowflake"):
        if stmt is None:
            continue
        out.append(stmt.transform(_fix).sql(dialect="duckdb"))
    return out
