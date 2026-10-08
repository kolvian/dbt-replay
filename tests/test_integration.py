import json
import shutil
from pathlib import Path

import duckdb
import pytest
import yaml

from dbt_replay.cli import main
from dbt_replay.runner import EXIT_CODES, run

ROOT = Path(__file__).resolve().parents[1]
CONFIG = "{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='order_id') }}\n"
APPEND = "{{ config(materialized='incremental', incremental_strategy='append') }}\n"
SELECT = "select * from {{ ref('stg_orders') }}\n"
WATERMARK = "{% if is_incremental() %}where ordered_at > (select max(ordered_at) from {{ this }}){% endif %}"


def scenario(tmp_path, sql, fixtures=("initial", "late"), assertions=None, key=("order_id",)):
    project = tmp_path / "analytics"
    shutil.copytree(ROOT / "examples/analytics", project)
    (project / "models/fct_orders.sql").write_text(sql)
    replay = tmp_path / "replay"
    shutil.copytree(ROOT / "examples/replay", replay)
    data = yaml.safe_load((replay / "orders.yml").read_text())
    data["compare"] = {"key": list(key)}
    data["steps"] = [
        {
            "name": f"step_{i}",
            "sources": {"raw.orders": f"fixtures/{fixture}.csv"} if fixture else {},
            "assert": assertions[i] if assertions else ["full_refresh_equivalent"],
        }
        for i, fixture in enumerate(fixtures)
    ]
    path = replay / "scenario.yml"
    path.write_text(yaml.safe_dump(data, sort_keys=False))
    return path, project


def execute(path, tmp_path, **kwargs):
    report = run(path, output=tmp_path / "evidence", **kwargs)
    persisted = json.loads((Path(report["workspace"]) / "report.json").read_text())
    assert persisted["status"] == report["status"]
    return report


def test_demo_fails_then_passes_with_model_fix(tmp_path):
    report = run(ROOT / "examples/replay/orders.yml", tmp_path / "bad", sample_limit=1)
    assert report["status"] == "FAIL", report
    assert report["first_failing_step"] == "late_arrival"
    diff = report["steps"][3]["assertions"][0]["diff"]
    assert diff["samples"]["missing"][0]["row"]["order_id"] == 1042
    assert report["steps"][4]["assertions"][0]["diff"]["counts"]["changed"] == 1
    assert (Path(report["workspace"]) / "first-failure-incremental.duckdb").exists()
    fixed = run(ROOT / "examples/replay/orders.yml", tmp_path / "fixed", {"replay_fix": True})
    assert fixed["status"] == "PASS", fixed
    reproduced = run(Path(fixed["workspace"]) / "replay.yml", tmp_path / "reproduced")
    assert reproduced["status"] == "PASS", reproduced


@pytest.mark.parametrize(
    "sql,fixtures,assertions,expected_count,key",
    [
        (CONFIG + SELECT + WATERMARK, ("initial", "equal"), None, "missing", ("order_id",)),
        (CONFIG + SELECT + WATERMARK, ("initial", "update"), None, "changed", ("order_id",)),
        (
            APPEND + SELECT,
            ("initial", None),
            [["full_refresh_equivalent"], ["unchanged"]],
            "extra",
            (),
        ),
        (
            "{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='ordered_at') }}\n"
            "select ordered_at, sum(amount) as amount from {{ ref('stg_orders') }} "
            + WATERMARK
            + " group by ordered_at",
            ("initial", "late"),
            None,
            "missing",
            ("ordered_at",),
        ),
    ],
)
def test_real_materialization_regressions(tmp_path, sql, fixtures, assertions, expected_count, key):
    path, _ = scenario(tmp_path, sql, fixtures, assertions, key)
    directory = path.parent / "fixtures"
    (directory / "equal.csv").write_text(
        (directory / "initial.csv").read_text()
        + "2,2026-01-02 10:00:00,2026-01-02 10:00:00,20.00\n"
    )
    (directory / "update.csv").write_text(
        (directory / "initial.csv").read_text().replace("10.00\n", "15.00\n")
    )
    report = execute(path, tmp_path)
    assert report["status"] == "FAIL", report
    assert report["first_failing_step"] == "step_1"
    assert report["steps"][1]["assertions"][0]["diff"]["counts"][expected_count] > 0


def test_legitimate_append_and_unchanged(tmp_path):
    path, _ = scenario(
        tmp_path,
        APPEND + SELECT + WATERMARK,
        ("initial", "new_batch", None),
        [
            ["full_refresh_equivalent"],
            ["full_refresh_equivalent"],
            ["unchanged", "full_refresh_equivalent"],
        ],
    )
    assert execute(path, tmp_path)["status"] == "PASS"


def test_nulls_duplicates_precision_and_timestamps(tmp_path):
    path, _ = scenario(tmp_path, CONFIG + SELECT, ("edge", None), key=())
    (path.parent / "fixtures/edge.csv").write_text(
        "order_id,ordered_at,updated_at,amount\n"
        "1,2026-01-02 10:00:00.123456,\\N,9999999999.99\n"
        "1,2026-01-02 10:00:00.123456,\\N,9999999999.99\n"
        "2,\\N,\\N,\\N\n"
    )
    report = execute(path, tmp_path)
    assert report["status"] == "PASS", report
    with duckdb.connect(str(Path(report["workspace"]) / "incremental/replay.duckdb")) as con:
        assert con.execute("select count(*) from fct_orders").fetchone()[0] == 3


def test_unstable_reference(tmp_path):
    path, _ = scenario(
        tmp_path,
        CONFIG + "select *, random() as random_value from {{ ref('stg_orders') }}",
        ("initial",),
    )
    report = execute(path, tmp_path)
    assert report["status"] == "INCONCLUSIVE", report
    assert not report["steps"][0]["reference_stability"]["equal"]


@pytest.mark.parametrize(
    "failure", ["dbt", "upstream", "fixture", "remote", "refresh", "seed", "python", "snapshot"]
)
def test_execution_errors(tmp_path, failure):
    path, project = scenario(tmp_path, CONFIG + SELECT, ("initial",))
    if failure == "dbt":
        (project / "models/fct_orders.sql").write_text(
            CONFIG + "select definitely_missing from {{ ref('stg_orders') }}"
        )
    if failure == "upstream":
        (project / "models/stg_orders.sql").write_text(
            CONFIG + "select * from {{ source('raw', 'orders') }}"
        )
    if failure == "fixture":
        (path.parent / "fixtures/initial.csv").write_text("order_id\nwrong\n")
    if failure == "remote":
        sources = project / "models/sources.yml"
        sources.write_text(
            sources.read_text().replace("schema: raw", "database: production\n    schema: raw")
        )
    if failure == "refresh":
        (project / "models/fct_orders.sql").write_text(
            "{{ config(full_refresh=false) }}\n" + CONFIG + SELECT
        )
    if failure == "seed":
        (project / "seeds").mkdir()
        (project / "seeds/orders_seed.csv").write_text("order_id\n1\n")
        (project / "models/stg_orders.sql").write_text("select * from {{ ref('orders_seed') }}")
    if failure == "python":
        (project / "models/stg_orders.sql").unlink()
        (project / "models/stg_orders.py").write_text(
            "def model(dbt, session):\n    dbt.config(materialized='table')\n"
            "    return dbt.source('raw', 'orders')\n"
        )
    if failure == "snapshot":
        (project / "snapshots").mkdir()
        (project / "snapshots/orders_history.sql").write_text(
            "{% snapshot orders_history %}\n"
            "{{ config(target_schema='history', unique_key='order_id', "
            "strategy='timestamp', updated_at='updated_at') }}\n"
            "select * from {{ source('raw', 'orders') }}\n{% endsnapshot %}"
        )
        (project / "models/stg_orders.sql").write_text("select * from {{ ref('orders_history') }}")
    report = execute(path, tmp_path)
    assert report["status"] == "ERROR", report
    expected = {
        "dbt": "dbt run exited",
        "upstream": "Unsupported upstream",
        "fixture": "CSV header",
        "remote": "database must",
        "refresh": "full_refresh",
        "seed": "Only SQL model",
        "python": "Only SQL model",
        "snapshot": "Only SQL model",
    }
    assert expected[failure] in report["error"], report


def test_workspace_profiles_environment_and_controlled_time(tmp_path, monkeypatch):
    path, project = scenario(
        tmp_path,
        CONFIG
        + "select *, '{{ var(\"as_of\") }}'::timestamp as controlled_time from {{ ref('stg_orders') }}",
        ("initial",),
    )
    sentinel = tmp_path / "production"
    sentinel.mkdir()
    (sentinel / "profiles.yml").write_text("Do not touch\n")
    (project / "profiles.yml").write_text("Do not use\n")
    monkeypatch.setenv("DBT_PROFILES_DIR", str(sentinel))
    monkeypatch.setenv("DBT_TARGET_PATH", str(sentinel))
    monkeypatch.setenv("DBT_FULL_REFRESH", "false")
    report = execute(path, tmp_path, variables={"as_of": "2026-01-01 00:00:00"})
    assert report["status"] == "PASS", report
    assert sorted(p.name for p in sentinel.iterdir()) == ["profiles.yml"]
    assert not (project / "target").exists()
    assert (project / "profiles.yml").read_text() == "Do not use\n"
    for branch in ("incremental", "reference"):
        workspace = Path(report["workspace"]) / branch
        profile = yaml.safe_load((workspace / "profiles.yml").read_text())
        assert profile["dbt_replay"]["outputs"]["replay"]["path"] == str(
            workspace / "replay.duckdb"
        )


@pytest.mark.parametrize("status,code", EXIT_CODES.items())
def test_cli_exit_codes(monkeypatch, capsys, status, code):
    monkeypatch.setattr(
        "dbt_replay.cli.run",
        lambda *args: {
            "status": status,
            "steps": [],
            "first_failing_step": None,
            "workspace": "/evidence",
        },
    )
    assert main(["run", "scenario.yml"]) == code
    assert status in capsys.readouterr().out


def test_invalid_cli_vars(capsys):
    assert main(["run", "unused.yml", "--vars", "[]"]) == 2
    assert "ERROR" in capsys.readouterr().out


@pytest.mark.parametrize("materialized", ["table", "ephemeral"])
def test_upstream_models_and_relation_aliases(tmp_path, materialized):
    path, project = scenario(
        tmp_path,
        "{{ config(alias='orders_output', schema='analytics') }}\n" + CONFIG + SELECT,
        ("initial",),
    )
    (project / "models/stg_orders.sql").write_text(
        "{{ config(materialized='" + materialized + "', schema='staging') }}\n"
        "select * from {{ source('raw', 'orders') }}"
    )
    sources = project / "models/sources.yml"
    sources.write_text(
        sources.read_text().replace("schema: raw", "schema: intake")
        + "        identifier: orders_input\n"
    )
    report = execute(path, tmp_path)
    assert report["status"] == "PASS", report
    with duckdb.connect(str(Path(report["workspace"]) / "incremental/replay.duckdb")) as con:
        assert con.execute("select order_id from main_analytics.orders_output").fetchall() == [(1,)]


def test_persisted_schema_difference(tmp_path):
    sql = (
        CONFIG
        + "select order_id, {% if flags.FULL_REFRESH %} amount::varchar {% else %} amount {% endif %} as amount from {{ ref('stg_orders') }}"
    )
    path, _ = scenario(tmp_path, sql, ("initial",))
    report = execute(path, tmp_path)
    assert report["status"] == "FAIL", report
    assert report["steps"][0]["assertions"][0]["diff"]["schema"] is not None
