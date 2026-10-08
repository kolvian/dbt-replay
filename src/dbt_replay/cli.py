import argparse
import json

import yaml

from dbt_replay import __version__
from dbt_replay.compare import json_value
from dbt_replay.runner import EXIT_CODES, run


def nonnegative(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("Must be nonnegative")
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Replay incremental dbt models in isolated DuckDB databases."
    )
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("run", help="Execute a YAML scenario")
    command.add_argument("scenario")
    command.add_argument(
        "--output", help="Evidence directory root (default: .replay in current directory)"
    )
    command.add_argument("--sample-limit", type=nonnegative, default=10)
    command.add_argument(
        "--vars", default="{}", help="YAML mapping overriding scenario dbt variables"
    )
    args = parser.parse_args(argv)
    try:
        variables = yaml.safe_load(args.vars)
        if not isinstance(variables, dict) or any(not isinstance(k, str) for k in variables):
            raise ValueError("--vars must be a mapping with string keys")
        json.dumps(variables, allow_nan=False)
        report = run(args.scenario, args.output, variables, args.sample_limit)
    except (ValueError, TypeError, OSError, yaml.YAMLError) as error:
        print(f"ERROR {error}")
        return EXIT_CODES["ERROR"]
    print(f"{report.get('model', args.scenario)} · {report.get('name', 'replay')}")
    for step in report["steps"]:
        print(f"{step['status']:12} {step['name']}")
        for assertion in step["assertions"]:
            print(f"  {assertion['status']:12} {assertion['name']}")
            diff = assertion.get("diff", {})
            if assertion["status"] == "FAIL":
                print("    " + json.dumps(json_value(diff), allow_nan=False))
        if step.get("reference_stability"):
            print("  Repeated full refresh changed the reference output.")
    if report.get("error"):
        print(f"ERROR {report['error']}")
    if report["first_failing_step"]:
        print(f"First failing step: {report['first_failing_step']}")
    print(f"{report['status']} · Report: {report['workspace']}/report.json")
    return EXIT_CODES[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
