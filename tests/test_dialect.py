import duckdb

from elt.local.dialect import to_duckdb


def run(sql: str):
    con = duckdb.connect()
    doc = '{"id": "A1", "amt": "12.50", "ts": "2026-03-01 08:30:00", "n": {"x": 3}}'
    con.execute(f"CREATE TABLE t AS SELECT '{doc}'::JSON AS payload")
    return con.execute(to_duckdb(sql)[0]).fetchone()


def test_variant_path_returns_unquoted_scalars():
    assert run("SELECT payload:id::STRING, payload:n.x::INT FROM t") == ("A1", 3)


def test_try_conversions_return_null_instead_of_failing():
    row = run(
        "SELECT TRY_TO_DECIMAL(payload:amt::STRING, 12, 2), TRY_TO_DECIMAL('abc', 12, 2), "
        "TRY_TO_TIMESTAMP_NTZ(payload:ts::STRING), TRY_TO_TIMESTAMP_NTZ('31/02/2026'), TRY_TO_DATE('2026-02-30') FROM t"
    )
    assert str(row[0]) == "12.50" and row[1] is None
    assert row[2].isoformat() == "2026-03-01T08:30:00" and row[3] is None and row[4] is None


def test_qualify_and_datediff():
    row = run("SELECT DATEDIFF(day, '2026-01-01'::DATE, '2026-01-31'::DATE) FROM t QUALIFY ROW_NUMBER() OVER (ORDER BY 1) = 1")
    assert row == (30,)


def test_ddl_maps_variant_and_drops_unenforced_keys():
    stmts = to_duckdb("CREATE TABLE r (id STRING PRIMARY KEY, payload VARIANT) CHANGE_TRACKING = TRUE; -- note")
    assert len(stmts) == 1
    assert "JSON" in stmts[0] and "PRIMARY KEY" not in stmts[0] and "CHANGE_TRACKING" not in stmts[0]
