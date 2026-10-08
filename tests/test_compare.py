import csv
from datetime import datetime
from decimal import Decimal

import pytest

from dbt_replay.compare import Table, compare, json_value
from dbt_replay.fixtures import convert, read_fixture
from dbt_replay.scenario import ReplayError, load_scenario


def test_multiset_counts_and_bounded_samples():
    schema = [("id", "BIGINT")]
    result = compare(
        Table(schema, [(1,)] * 3), Table(schema, [(1,)] + [(i,) for i in range(2, 52)]), limit=2
    )
    assert result["counts"] == {"missing": 50, "extra": 2, "changed": 0}
    assert len(result["samples"]["missing"]) == 2
    assert result["samples"]["extra"][0]["count"] == 2
    assert not result["equal"]
    assert compare(Table(schema, [(2,), (1,)]), Table(schema, [(1,), (2,)]))["equal"]


def test_keyed_changes_nulls_duplicates_and_schema():
    schema = [("id", "BIGINT"), ("amount", "DECIMAL(12,2)")]
    actual = Table(schema, [(1, Decimal("12.34"))])
    expected = Table(schema, [(1, Decimal("10.00"))])
    result = compare(actual, expected, ["id"])
    assert result["counts"] == {"missing": 0, "extra": 0, "changed": 1}
    assert json_value(result)["samples"]["changed"][0]["actual"]["amount"] == "12.34"
    invalid = Table(schema, [(None, None), (1, None), (1, None)])
    errors = compare(invalid, invalid, ["id"])["key_errors"]
    assert errors[0]["null_rows"] == errors[0]["duplicate_rows"] == 1
    assert not compare(actual, Table([("id", "INTEGER")], []))["equal"]


@pytest.mark.parametrize(
    "value,typ",
    [
        ("1.001", "DECIMAL(12,2)"),
        ("1000", "DECIMAL(3,0)"),
        ("128", "TINYINT"),
        ("1.0", "INTEGER"),
        ("", "BIGINT"),
        ("yes", "BOOLEAN"),
        ("2026-01-01", "TIMESTAMP"),
        ("2026-01-01 00:00:00.0000001", "TIMESTAMP"),
        ("2026-01-01 00:00:00+00:00", "TIMESTAMP"),
        ("2026-02-30", "DATE"),
    ],
)
def test_strict_types(value, typ):
    with pytest.raises(ValueError):
        convert(value, typ)


def test_null_decimal_timestamp_and_empty_string():
    assert convert("\\N", "VARCHAR") is None
    assert convert("", "VARCHAR") == ""
    assert convert("12.34", "DECIMAL(12,2)") == Decimal("12.34")
    assert convert("2026-01-01 00:00:00.123456", "TIMESTAMP") == datetime.fromisoformat(
        "2026-01-01 00:00:00.123456"
    )


@pytest.mark.parametrize("text", ["x\n1\n", "id\n1,2\n", "id\ninvalid\n", 'id\n"bad\n'])
def test_invalid_fixtures(tmp_path, text):
    fixture = tmp_path / "bad.csv"
    fixture.write_text(text)
    with pytest.raises((ReplayError, ValueError, csv.Error)):
        read_fixture(fixture, {"id": "BIGINT"})


def test_unknown_configuration_and_duplicate_yaml_keys(tmp_path):
    scenario = tmp_path / "scenario.yml"
    scenario.write_text("version: 1\nversion: 1\n")
    with pytest.raises(ReplayError, match="unique"):
        load_scenario(scenario)
    scenario.write_text("version: 1\nunrecognized: true\n")
    with pytest.raises(ReplayError, match="fields"):
        load_scenario(scenario)


def test_timestamp_nanoseconds_and_infinities_are_exact(tmp_path):
    import duckdb

    from dbt_replay.compare import snapshot

    path = tmp_path / "replay.duckdb"
    with duckdb.connect(str(path)) as con:
        con.execute(
            "create table actual as select '2026-01-01 00:00:00.000000001'::timestamp_ns as t, 'infinity'::date as d"
        )
        con.execute(
            "create table expected as select '2026-01-01 00:00:00.000000002'::timestamp_ns as t, '9999-12-31'::date as d"
        )
    a = snapshot(path, {"database": "replay", "schema": "main", "alias": "actual"})
    b = snapshot(path, {"database": "replay", "schema": "main", "alias": "expected"})
    assert a.rows[0] == ("2026-01-01 00:00:00.000000001", "infinity")
    assert not compare(a, b)["equal"]


def test_artifact_versions_and_nested_evidence_copy(tmp_path):
    import json

    from dbt_replay.project import artifact, prepare

    path = tmp_path / "manifest.json"
    path.write_text(
        json.dumps(
            {"metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v11.json"}}
        )
    )
    with pytest.raises(ReplayError, match="Unsupported"):
        artifact(path, "manifest", 12)
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: demo\nversion: '1.0'\nconfig-version: 2\n")
    workspace = project / "evidence/run/incremental"
    workspace.parent.mkdir(parents=True)
    prepare(project, workspace)
    assert not (workspace / "project/evidence").exists()
    assert (workspace / "project/dbt_project.yml").exists()
    assert convert("0", "DECIMAL(2,2)") == Decimal(0)
