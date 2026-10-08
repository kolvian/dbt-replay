import json
import re
from pathlib import Path

import yaml


class ReplayError(Exception):
    pass


class UniqueLoader(yaml.SafeLoader):
    pass


def _mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, str) or key in result:
            raise ReplayError(f"YAML keys must be unique strings: {key!r}")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def read_yaml(path):
    return yaml.load(Path(path).read_text(encoding="utf-8"), Loader=UniqueLoader)


def fields(value, allowed, required, where):
    if not isinstance(value, dict):
        raise ReplayError(f"{where} must be a mapping")
    if set(value) - set(allowed) or set(required) - set(value):
        raise ReplayError(f"Invalid fields in {where}; allowed: {', '.join(allowed)}")


def names(value, where, empty=False):
    if not isinstance(value, list) or (not value and not empty):
        raise ReplayError(f"{where} must be a {'possibly empty ' if empty else ''}list")
    if any(not isinstance(x, str) or not x for x in value) or len(set(value)) != len(value):
        raise ReplayError(f"{where} must contain unique nonempty strings")
    return value


def resolve(base, value):
    if not isinstance(value, str) or not value:
        raise ReplayError("Paths must be nonempty strings")
    return (base / value).resolve()


def column_type(value):
    if not isinstance(value, str):
        raise ReplayError("Column types must be strings")
    value = re.sub(r"\s+", "", value.upper())
    if value in {
        "BIGINT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "VARCHAR",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
    }:
        return value
    decimal = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", value)
    if decimal and 1 <= int(decimal[1]) <= 38 and 0 <= int(decimal[2]) <= int(decimal[1]):
        return value
    raise ReplayError(f"Unsupported fixture type: {value}")


def load_scenario(path):
    path = Path(path).resolve()
    data = read_yaml(path)
    fields(
        data,
        ["version", "name", "project", "model", "sources", "compare", "steps", "vars"],
        ["version", "name", "project", "model", "sources", "steps"],
        "scenario",
    )
    if type(data["version"]) is not int or data["version"] != 1:
        raise ReplayError("Only scenario version 1 is supported")
    for field in ("name", "model"):
        if not isinstance(data[field], str) or not data[field]:
            raise ReplayError(f"{field} must be a nonempty string")
    data["project"] = resolve(path.parent, data["project"])
    if not (data["project"] / "dbt_project.yml").is_file():
        raise ReplayError("project must contain dbt_project.yml")
    if not isinstance(data["sources"], dict) or not data["sources"]:
        raise ReplayError("sources must be a nonempty mapping")
    for name, source in data["sources"].items():
        if len(name.split(".")) != 2 or not all(name.split(".")):
            raise ReplayError(f"Source must be named source_name.table_name: {name}")
        fields(source, ["columns"], ["columns"], name)
        if not isinstance(source["columns"], dict) or not source["columns"]:
            raise ReplayError(f"{name}.columns must be a nonempty mapping")
        if any(not col or "\x00" in col for col in source["columns"]):
            raise ReplayError(f"Invalid column name in {name}")
        if len({col.lower() for col in source["columns"]}) != len(source["columns"]):
            raise ReplayError(f"Case-insensitive duplicate columns in {name}")
        source["columns"] = {col: column_type(typ) for col, typ in source["columns"].items()}
    compare = data.setdefault("compare", {})
    fields(compare, ["key", "columns"], [], "compare")
    compare["key"] = names(compare.get("key", []), "compare.key", empty=True)
    if "columns" in compare:
        names(compare["columns"], "compare.columns")
        if not set(compare["key"]) <= set(compare["columns"]):
            raise ReplayError("compare.key must be included in compare.columns")
    if not isinstance(data.setdefault("vars", {}), dict):
        raise ReplayError("vars must be a mapping")
    try:
        json.dumps(data["vars"], allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ReplayError("vars must contain JSON-compatible values") from error
    if not isinstance(data["steps"], list) or not data["steps"]:
        raise ReplayError("steps must be a nonempty list")
    step_names = []
    for i, step in enumerate(data["steps"]):
        fields(step, ["name", "sources", "assert"], ["name", "assert"], f"step {i}")
        step_names.append(step["name"])
        assertions = names(step["assert"], f"step {i}.assert")
        if set(assertions) - {"unchanged", "full_refresh_equivalent"}:
            raise ReplayError(f"Unsupported assertion in step {i}")
        if i == 0 and "unchanged" in assertions:
            raise ReplayError("unchanged requires a preceding step")
        fixtures = step.setdefault("sources", {})
        if not isinstance(fixtures, dict) or set(fixtures) - set(data["sources"]):
            raise ReplayError(f"Unknown sources in step {i}")
        if i == 0 and set(fixtures) != set(data["sources"]):
            raise ReplayError("The first step must supply every source fixture")
        step["sources"] = {
            name: resolve(path.parent, fixture) for name, fixture in fixtures.items()
        }
    names(step_names, "step names")
    data["path"] = path
    return data
