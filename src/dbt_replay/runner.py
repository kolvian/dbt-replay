import csv
import hashlib
import importlib.metadata
import json
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path

import duckdb
import yaml

from dbt_replay.compare import compare, json_value, snapshot
from dbt_replay.fixtures import load_sources, read_fixture
from dbt_replay.project import (
    executable_models,
    invoke,
    prepare,
    resolve,
    signature,
)
from dbt_replay.scenario import ReplayError, load_scenario

EXIT_CODES = {"PASS": 0, "FAIL": 1, "ERROR": 2, "INCONCLUSIVE": 3}


def write_json(path, data):
    path.write_text(
        json.dumps(json_value(data), indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )


def run(path, output=None, variables=None, sample_limit=10):
    report = {"report_version": 1, "status": "ERROR", "steps": [], "first_failing_step": None}
    output_root = Path(output or ".replay").resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    workspace = Path(tempfile.mkdtemp(prefix="run-", dir=output_root))
    report["workspace"] = str(workspace)
    active = None
    try:
        scenario = load_scenario(path)
        if variables:
            scenario["vars"].update(variables)
        report.update(
            name=scenario["name"], model=scenario["model"], scenario=str(scenario["path"])
        )
        report["versions"] = {
            name: importlib.metadata.version(name)
            for name in ("dbt-replay", "dbt-core", "dbt-duckdb", "duckdb")
        }
        report["python"] = sys.version
        report["vars"] = scenario["vars"]
        shutil.copy2(scenario["path"], workspace / "scenario.original.yml")
        branches = {name: workspace / name for name in ("incremental", "reference")}
        resolved = {}
        for name, branch in branches.items():
            prepare(scenario["project"], branch)
            manifest = invoke(branch, "parse", scenario["vars"], "parse")
            resolved[name] = resolve(manifest, scenario["model"], scenario["sources"])
        baseline = signature(resolved["incremental"])
        if baseline != signature(resolved["reference"]):
            raise ReplayError("Branches resolved different model or source configurations")
        selected, models, sources = resolved["incremental"]
        selected_ids = executable_models(models)
        columns = scenario["compare"].get("columns")
        keys = scenario["compare"]["key"]
        state, previous_state, previous = {}, None, None
        normalized = {k: v for k, v in scenario.items() if k != "path"}
        normalized["project"] = "incremental/project"
        (workspace / "fixtures").mkdir()
        for i, step in enumerate(scenario["steps"]):
            active = {"name": step["name"], "status": "PASS", "assertions": []}
            report["steps"].append(active)
            for name, fixture in step["sources"].items():
                retained = workspace / "fixtures" / f"{i:03d}-{name}.csv"
                shutil.copy2(fixture, retained)
                state[name] = read_fixture(retained, scenario["sources"][name]["columns"])
                step["sources"][name] = str(retained.relative_to(workspace))
            if (
                "unchanged" in step["assert"]
                and previous_state is not None
                and any(Counter(state[k]) != Counter(previous_state[k]) for k in state)
            ):
                raise ReplayError(f"unchanged requires unchanged source inputs: {step['name']}")
            current = {}
            for name, branch in branches.items():
                load_sources(branch / "replay.duckdb", sources, scenario["sources"], state)
                manifest = invoke(
                    branch,
                    "run",
                    scenario["vars"],
                    f"{i:03d}-run",
                    selected_ids,
                    refresh=name == "reference",
                )
                if signature(resolve(manifest, scenario["model"], scenario["sources"])) != baseline:
                    raise ReplayError("Model or source configuration changed during execution")
                current[name] = snapshot(branch / "replay.duckdb", selected, columns)
            unstable = False
            if "full_refresh_equivalent" in step["assert"]:
                branch = branches["reference"]
                load_sources(branch / "replay.duckdb", sources, scenario["sources"], state)
                manifest = invoke(
                    branch, "run", scenario["vars"], f"{i:03d}-repeat", selected_ids, refresh=True
                )
                if signature(resolve(manifest, scenario["model"], scenario["sources"])) != baseline:
                    raise ReplayError("Reference configuration changed during repeated execution")
                repeated = snapshot(branch / "replay.duckdb", selected, columns)
                stability = compare(repeated, current["reference"], limit=sample_limit)
                if not stability["equal"]:
                    unstable = True
                    active["status"] = "INCONCLUSIVE"
                    active["reference_stability"] = stability
            for assertion in step["assert"]:
                if assertion == "full_refresh_equivalent" and unstable:
                    active["assertions"].append({"name": assertion, "status": "INCONCLUSIVE"})
                    continue
                expected = previous if assertion == "unchanged" else current["reference"]
                difference = compare(current["incremental"], expected, keys, sample_limit)
                status = "PASS" if difference["equal"] else "FAIL"
                active["assertions"].append(
                    {"name": assertion, "status": status, "diff": difference}
                )
                if status == "FAIL":
                    active["status"] = "FAIL"
            previous, previous_state = current["incremental"], dict(state)
            write_json(workspace / f"step-{i:03d}.json", active)
            if active["status"] == "FAIL" and report["first_failing_step"] is None:
                report["first_failing_step"] = step["name"]
                for name, branch in branches.items():
                    shutil.copy2(
                        branch / "replay.duckdb", workspace / f"first-failure-{name}.duckdb"
                    )
        report["status"] = "PASS"
        for status in ("INCONCLUSIVE", "FAIL"):
            if any(step["status"] == status for step in report["steps"]):
                report["status"] = status
        normalized["vars"] = scenario["vars"]
        (workspace / "replay.yml").write_text(
            yaml.safe_dump(normalized, sort_keys=False), encoding="utf-8"
        )
        report["reproduce"] = f"dbt-replay run {workspace / 'replay.yml'}"
        report["fixture_sha256"] = {
            str(p.relative_to(workspace)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (workspace / "fixtures").glob("*.csv")
        }
    except (
        ReplayError,
        OSError,
        ValueError,
        TypeError,
        KeyError,
        duckdb.Error,
        yaml.YAMLError,
        csv.Error,
    ) as error:
        report.update(status="ERROR", error=str(error))
        if active is not None:
            active.update(status="ERROR", error=str(error))
    write_json(workspace / "report.json", report)
    return report
