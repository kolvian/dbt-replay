import math
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal

import duckdb

from dbt_replay.scenario import ReplayError


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def relation(node):
    return ".".join(quote(node[x]) for x in ("database", "schema", "alias") if node.get(x))


def scalar(value):
    if isinstance(value, float) and math.isnan(value):
        return ("float", "NaN")
    if value is not None and not isinstance(value, (str, int, float, Decimal, date, datetime)):
        raise ReplayError(
            "Only scalar string, numeric, boolean, date and timestamp outputs are supported"
        )
    return value


def json_value(value):
    if isinstance(value, (Decimal, date, datetime)):
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, (list, tuple)):
        return [json_value(x) for x in value]
    if isinstance(value, dict):
        return {k: json_value(v) for k, v in value.items()}
    return value


@dataclass
class Table:
    schema: list
    rows: list


def snapshot(database, node, columns=None):
    with duckdb.connect(str(database), read_only=True) as con:
        con.execute("SET TimeZone = 'UTC'")
        cursor = con.execute(f"SELECT * FROM {relation(node)} LIMIT 0")
        schema = [(x[0], str(x[1])) for x in cursor.description]
        available = [x[0] for x in schema]
        if columns and not set(columns) <= set(available):
            raise ReplayError(f"Comparison columns absent from {relation(node)}: {columns}")
        selected = columns or available
        types = dict(schema)
        expressions = [
            f"CAST({quote(col)} AS VARCHAR)"
            if types[col] == "DATE" or types[col].startswith("TIMESTAMP")
            else quote(col)
            for col in selected
        ]
        cursor = con.execute(f"SELECT {', '.join(expressions)} FROM {relation(node)}")
        # ponytail: We keep small tables in memory until SQL comparisons become necessary.
        rows = [tuple(scalar(value) for value in row) for row in cursor.fetchall()]
    return Table([(x, dict(schema)[x]) for x in selected], rows)


def compare(actual, expected, keys=(), limit=10):
    result = {
        "equal": True,
        "schema": None,
        "key_errors": [],
        "counts": {"missing": 0, "extra": 0, "changed": 0},
        "samples": {"missing": [], "extra": [], "changed": []},
    }
    if actual.schema != expected.schema:
        result.update(equal=False, schema={"actual": actual.schema, "expected": expected.schema})
        return result
    columns = [x[0] for x in actual.schema]
    if not set(keys) <= set(columns):
        raise ReplayError("Diagnostic keys are absent from the output")
    indices = [columns.index(x) for x in keys]

    def key(row):
        return tuple(row[i] for i in indices)

    for label, table in (("actual", actual), ("expected", expected)):
        if keys:
            key_counts = Counter(key(row) for row in table.rows)
            nulls = sum(n for k, n in key_counts.items() if None in k)
            duplicates = sum(n - 1 for n in key_counts.values() if n > 1)
            if nulls or duplicates:
                result["key_errors"].append(
                    {"table": label, "null_rows": nulls, "duplicate_rows": duplicates}
                )
    missing = Counter(expected.rows) - Counter(actual.rows)
    extra = Counter(actual.rows) - Counter(expected.rows)
    if keys and not result["key_errors"]:
        missing_keys = {key(row): row for row in missing}
        extra_keys = {key(row): row for row in extra}
        for k in sorted(missing_keys.keys() & extra_keys.keys(), key=repr):
            before, after = missing_keys[k], extra_keys[k]
            result["counts"]["changed"] += 1
            if len(result["samples"]["changed"]) < limit:
                result["samples"]["changed"].append(
                    {
                        "key": dict(zip(keys, k)),
                        "expected": dict(zip(columns, before)),
                        "actual": dict(zip(columns, after)),
                    }
                )
            del missing[before]
            del extra[after]
    for label, rows in (("missing", missing), ("extra", extra)):
        result["counts"][label] = sum(rows.values())
        for row, count in rows.items():
            if len(result["samples"][label]) >= limit:
                break
            result["samples"][label].append({"row": dict(zip(columns, row)), "count": count})
    result["equal"] = not (result["key_errors"] or any(result["counts"].values()))
    return result
