import csv
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

import duckdb

from dbt_replay.compare import quote, relation
from dbt_replay.scenario import ReplayError

NULL = "\\N"


def convert(value, typ):
    if value == NULL:
        return None
    if typ == "VARCHAR":
        return value
    if typ == "BOOLEAN":
        if value not in ("true", "false"):
            raise ValueError("BOOLEAN requires true or false")
        return value == "true"
    if typ in ("TINYINT", "SMALLINT", "INTEGER", "BIGINT"):
        if not re.fullmatch(r"[+-]?\d+", value):
            raise ValueError("Invalid integer")
        number = int(value)
        bits = {"TINYINT": 8, "SMALLINT": 16, "INTEGER": 32, "BIGINT": 64}[typ]
        if not -(2 ** (bits - 1)) <= number < 2 ** (bits - 1):
            raise ValueError("Integer out of range")
        return number
    if typ == "DATE":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("DATE requires YYYY-MM-DD")
        return date.fromisoformat(value)
    if typ == "TIMESTAMP":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?", value):
            raise ValueError(
                "TIMESTAMP requires timezone-free ISO seconds, up to six fractional digits"
            )
        return datetime.fromisoformat(value)
    precision, scale = map(int, re.findall(r"\d+", typ))
    if not re.fullmatch(r"[+-]?\d+(\.\d+)?", value):
        raise ValueError("Invalid decimal")
    number = Decimal(value)
    _, digits, exponent = number.as_tuple()
    fractional = max(0, -exponent)
    whole = max(0, len(digits) + exponent) if number else 0
    if fractional > scale or whole > precision - scale:
        raise ValueError(f"Decimal does not fit {typ} exactly")
    return number


def read_fixture(path, columns):
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream, strict=True)
        if next(reader, None) != list(columns):
            raise ReplayError(f"{path}: CSV header must exactly match declared columns and order")
        rows = []
        for line, row in enumerate(reader, 2):
            if len(row) != len(columns):
                raise ReplayError(f"{path}:{line}: Expected {len(columns)} fields, got {len(row)}")
            try:
                rows.append(tuple(convert(value, typ) for value, typ in zip(row, columns.values())))
            except (ValueError, InvalidOperation) as error:
                raise ReplayError(f"{path}:{line}: {error}") from error
    return rows


def load_sources(database, sources, definitions, state):
    with duckdb.connect(str(database)) as con:
        con.execute("BEGIN")
        try:
            for name, node in sources.items():
                con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote(node['schema'])}")
                columns = definitions[name]["columns"]
                declarations = ", ".join(f"{quote(col)} {typ}" for col, typ in columns.items())
                con.execute(f"CREATE OR REPLACE TABLE {relation(node)} ({declarations})")
                if state[name]:
                    placeholders = ",".join("?" for _ in columns)
                    con.executemany(
                        f"INSERT INTO {relation(node)} VALUES ({placeholders})", state[name]
                    )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
