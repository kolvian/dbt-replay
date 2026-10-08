import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml

from dbt_replay.scenario import ReplayError, read_yaml


def artifact(path, kind, version):
    data = json.loads(path.read_text(encoding="utf-8"))
    expected = f"https://schemas.getdbt.com/dbt/{kind}/v{version}.json"
    if data.get("metadata", {}).get("dbt_schema_version") != expected:
        raise ReplayError(f"Unsupported {kind} schema in {path}; expected v{version}")
    return data


def prepare(project, workspace):
    destination = workspace / "project"
    ignored = shutil.ignore_patterns(
        ".git",
        ".venv",
        "venv",
        "target",
        "logs",
        ".replay",
        "__pycache__",
        "profiles.yml",
        "*.duckdb",
        "*.duckdb.wal",
    )

    def ignore(directory, children):
        return set(ignored(directory, children)) | {
            child
            for child in children
            if workspace.is_relative_to((Path(directory) / child).resolve())
        }

    shutil.copytree(project, destination, ignore=ignore)
    config = read_yaml(destination / "dbt_project.yml")
    if not isinstance(config, dict):
        raise ReplayError("dbt_project.yml must be a mapping")
    for field in (
        "model-paths",
        "macro-paths",
        "test-paths",
        "seed-paths",
        "snapshot-paths",
        "analysis-paths",
        "docs-paths",
        "asset-paths",
    ):
        paths = config.get(field, [])
        if not isinstance(paths, list):
            raise ReplayError(f"{field} must be a list of project-relative paths")
        for value in paths:
            if not isinstance(value, str) or not (destination / value).resolve().is_relative_to(
                destination
            ):
                raise ReplayError(f"{field} must stay inside the copied project")
    config["profile"] = "dbt_replay"
    config["target-path"] = "target"
    config["log-path"] = "logs"
    (destination / "dbt_project.yml").write_text(yaml.safe_dump(config), encoding="utf-8")
    profiles = {
        "dbt_replay": {
            "target": "replay",
            "outputs": {
                "replay": {
                    "type": "duckdb",
                    "path": str(workspace / "replay.duckdb"),
                    "schema": "main",
                    "threads": 1,
                    "settings": {"TimeZone": "UTC"},
                }
            },
        }
    }
    (workspace / "profiles.yml").write_text(yaml.safe_dump(profiles), encoding="utf-8")


def invoke(workspace, command, variables, label, selected=None, refresh=False):
    build = workspace / "artifacts" / label
    build.mkdir(parents=True)
    executable = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
    if not executable.is_file():
        raise ReplayError(
            "dbt executable missing beside Python; reinstall dbt-replay in a virtualenv"
        )
    args = [
        str(executable),
        command,
        "--project-dir",
        str(workspace / "project"),
        "--profiles-dir",
        str(workspace),
        "--target",
        "replay",
        "--target-path",
        str(build),
        "--log-path",
        str(build / "logs"),
        "--vars",
        yaml.safe_dump(variables),
        "--no-use-colors",
        "--no-partial-parse",
        "--write-json",
    ]
    if selected:
        args.extend(["--select", " ".join(selected.values())])
    if refresh:
        args.append("--full-refresh")
    env = {key: value for key, value in os.environ.items() if not key.startswith("DBT_")}
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    (build / "command.json").write_text(json.dumps(args, indent=2), encoding="utf-8")
    with (build / "console.log").open("w", encoding="utf-8") as log:
        try:
            process = subprocess.run(
                args,
                cwd=workspace / "project",
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=300,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise ReplayError(f"dbt timed out; inspect {build / 'console.log'}") from error
    results_path = build / "run_results.json"
    results = artifact(results_path, "run-results", 6) if results_path.exists() else None
    if process.returncode:
        raise ReplayError(
            f"dbt {command} exited {process.returncode}; inspect {build / 'console.log'}"
        )
    if selected:
        if results is None:
            raise ReplayError(f"dbt did not produce run_results.json in {build}")
        executed = {r["unique_id"] for r in results["results"] if r["status"] == "success"}
        if any(r["status"] != "success" for r in results["results"]) or set(selected) != executed:
            raise ReplayError(
                f"dbt did not successfully execute all selected models; inspect {build}"
            )
    return artifact(build / "manifest.json", "manifest", 12)


def resolve(manifest, model, definitions):
    nodes, source_nodes = manifest["nodes"], manifest["sources"]
    matches = [
        node
        for node in nodes.values()
        if node["resource_type"] == "model"
        and model in (node["name"], node["unique_id"], ".".join(node["fqn"]))
    ]
    if len(matches) != 1:
        raise ReplayError(f"Model must resolve uniquely: {model}; found {len(matches)}")
    selected = matches[0]
    if selected["config"]["materialized"] != "incremental":
        raise ReplayError("Selected model must be incremental")
    if selected["config"].get("incremental_strategy") not in ("append", "delete+insert"):
        raise ReplayError("Set incremental_strategy explicitly to append or delete+insert")
    if selected["config"].get("full_refresh") is not None:
        raise ReplayError("Selected model cannot override full_refresh")
    models, sources, seen = {}, {}, set()

    def visit(identifier):
        if identifier in seen:
            return
        seen.add(identifier)
        if identifier in source_nodes:
            node = dict(source_nodes[identifier])
            name = f"{node['source_name']}.{node['name']}"
            if name in sources or name not in definitions:
                raise ReplayError(f"Source fixture missing or ambiguous: {name}")
            if node.get("external"):
                raise ReplayError(f"External source unsupported: {name}")
            node["alias"] = node["identifier"]
            sources[name] = node
        elif identifier in nodes:
            node = nodes[identifier]
            materialized = node.get("config", {}).get("materialized")
            if node["resource_type"] != "model" or node.get("language") != "sql":
                raise ReplayError(f"Only SQL model dependencies are supported: {identifier}")
            if identifier != selected["unique_id"] and materialized not in (
                "table",
                "view",
                "ephemeral",
            ):
                raise ReplayError(
                    f"Unsupported upstream materialization: {identifier} ({materialized})"
                )
            models[identifier] = node
            for dependency in node["depends_on"]["nodes"]:
                visit(dependency)
        else:
            raise ReplayError(f"Unsupported dependency: {identifier}")
        if node.get("database") != "replay":
            raise ReplayError(f"Relation database must resolve to replay: {identifier}")

    visit(selected["unique_id"])
    if set(definitions) != set(sources):
        raise ReplayError("Configured sources must exactly match the selected dependency closure")
    relations = [
        (node["database"].lower(), node["schema"].lower(), node["alias"].lower())
        for node in [*models.values(), *sources.values()]
        if node.get("config", {}).get("materialized") != "ephemeral"
    ]
    if len(relations) != len(set(relations)):
        raise ReplayError("Source and model relations must be distinct")
    return selected, models, sources


def signature(resolved):
    selected, models, sources = resolved
    return {
        "selected": selected["unique_id"],
        "models": {
            key: {
                field: node.get(field)
                for field in ("database", "schema", "alias", "language", "config", "depends_on")
            }
            for key, node in models.items()
        },
        "sources": {
            key: {field: node.get(field) for field in ("database", "schema", "alias", "columns")}
            for key, node in sources.items()
        },
    }


def executable_models(models):
    return {
        key: "fqn:" + ".".join(node["fqn"])
        for key, node in sorted(models.items())
        if node["config"]["materialized"] != "ephemeral"
    }
