# dbt-replay

Test the **persisted table** of an incremental dbt model across a sequence of source changes. A model can execute successfully and pass uniqueness checks while silently missing a late arrival. dbt-replay runs its actual materialization and reports the first step where a declared property fails.

This is an early local MVP, validated on synthetic examples. It has not yet been validated against an external project.

## Install and try it

Python 3.10–3.13, a virtual environment, and no warehouse credentials:

```sh
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e .
dbt-replay run examples/replay/orders.yml
```

The example intentionally exits **1**. Its watermark filter misses order `1042` at `late_arrival`, then misses an amount correction. Run the same scenario with the model's demonstration fix enabled:

```sh
dbt-replay run examples/replay/orders.yml --vars '{replay_fix: true}'
```

This exits **0**. The fix removes the watermark filter and uses `delete+insert` with `order_id` as its unique key. It rereads the entire source snapshot, which is a correctness demonstration, not a performance recommendation.

These commands run real `dbt parse` and `dbt run` subprocesses. They do not simulate incremental SQL or require a default dbt profile.

## Scenarios

See [the complete five-step example](examples/replay/orders.yml). The minimal shape is:

```yaml
version: 1
name: late_arriving_order
project: ../analytics
model: fct_orders
vars: {}
sources:
  raw.orders:
    columns:
      order_id: BIGINT
      ordered_at: TIMESTAMP
      updated_at: TIMESTAMP
      amount: DECIMAL(12, 2)
compare:
  key: [order_id]
  columns: [order_id, ordered_at, amount]
steps:
  - name: initial
    sources:
      raw.orders: fixtures/initial.csv
    assert: [full_refresh_equivalent]
  - name: rerun
    assert: [unchanged, full_refresh_equivalent]
  - name: late_arrival
    sources:
      raw.orders: fixtures/late.csv
    assert: [full_refresh_equivalent]
```

All configuration paths resolve against the scenario file, regardless of the shell's working directory. `--output` resolves against the shell's working directory and defaults to `.replay`. Each invocation gets a new directory; previous evidence is retained.

A fixture is the **complete contents** of one source table at that step. It replaces the table, so include earlier rows you still want. Omitted sources retain their preceding fixture state. The first step must provide every source fixture in the selected model's dependency closure. Unknown fields, duplicate YAML keys, ambiguous model names, and unsupported assertions are errors. Models can be identified by a unique name, fully qualified name, or manifest unique ID.

### CSV rules

CSV is UTF-8, comma-delimited, with ordinary CSV quoting. Headers must match declared columns in name and order. Every data row must have exactly that number of fields. Header-only files represent empty tables.

The exact string `\N` means SQL NULL, including when quoted. Empty strings are preserved for `VARCHAR` and rejected for other types. There is no escape for a literal `\N` string in this version. No type inference or lossy coercion is performed.

Supported fixture types:

- `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`: integral strings within the type's range.
- `DECIMAL(p,s)`: precision up to 38, with no exponent notation or excess fractional digits.
- `VARCHAR`, `BOOLEAN`: booleans must be exactly `true` or `false`.
- `DATE`: `YYYY-MM-DD`.
- `TIMESTAMP`: timezone-free `YYYY-MM-DD HH:MM:SS[.ffffff]` or the equivalent with `T`; at most six fractional digits.

Invalid values stop execution with `ERROR`, identifying the fixture and row. Fixtures are loaded with typed, parameterized inserts.

### Assertions and comparison

Assertions are opt-in contracts:

- `full_refresh_equivalent` compares the incremental table with a full rebuild from the current source snapshot. A second full rebuild checks reference stability. A changing reference makes this assertion `INCONCLUSIVE`.
- `unchanged` compares with the preceding incremental table. Source inputs must be unchanged as row multisets, or the scenario returns `ERROR`. The first step cannot use it.

Comparison ignores row order and preserves duplicate multiplicities. It uses exact values, including decimals and timestamps, with no tolerances. Output types supported are scalar strings, booleans, numbers, dates and timestamps. NaNs compare equal to NaNs. Nested values, binary values, and other output types are rejected.

`compare.columns` explicitly selects the compared output and schema; omitted columns are outside the assertion. Without it, all columns and their types/order are compared. A missing requested column is an `ERROR`; differing available schemas are a `FAIL`. There are no automatic exclusions for changing columns.

Optional diagnostic keys must be included in compared columns and must be unique and non-null on both sides. Invalid keys fail the assertion. Matching valid keys with differing values are `changed`; unmatched records are `missing` or `extra`. Without valid keys, differences are counted as row multisets. Counts cover all discrepancies; `--sample-limit` bounds samples **per category per assertion** in terminal and JSON output. Repeated identical rows occupy one sample with a multiplicity count. Zero suppresses row samples.

Explicit expected-output fixture assertions are deferred. Full-refresh equivalence is unsuitable for some history-dependent models and does not establish business correctness when both branches share incorrect SQL.

### Controlled time

Declare scenario `vars` or pass `--vars '{as_of: "2026-01-01 00:00:00"}'`. Both branches receive identical variables. This controls time only when your model actually uses those variables. Repeated-reference checks detect observed instability, not every possible source of nondeterminism.

## Results and evidence

| State | Exit code | Meaning |
| --- | --- | --- |
| PASS | 0 | Every requested assertion passed. |
| FAIL | 1 | Execution succeeded but a declared property failed. |
| ERROR | 2 | Configuration, fixture, unsupported feature, or dbt execution error. |
| INCONCLUSIVE | 3 | Reference instability prevented meaningful comparison. |

Execution continues after assertion failures so later steps can be inspected. Execution errors stop the sequence. Overall precedence is `ERROR`, then `FAIL`, then `INCONCLUSIVE`, then `PASS`: an independent proven failure still fails CI when another assertion is inconclusive.

The terminal prints each step, assertion, the first failing step, and the report path. Each retained run directory contains:

- `report.json`, with versioned results, complete counts, bounded samples, runtime versions, variables, and fixture hashes.
- `step-NNN.json` results, copied fixtures, and separate copied projects/profiles/databases for each branch.
- Per-command manifests, run-results, command arguments, and dbt logs under each branch's `artifacts/` directory.
- Copies of both databases at the first assertion failure.
- `replay.yml`, a scenario using retained project/fixture paths, on completed runs. The report includes its rerun command.

Rerun with `dbt-replay run /path/to/run/replay.yml`. Reproduction needs the recorded runtime versions and any project-specific non-dbt environment variables. Evidence may include sensitive fixture data, variables, SQL and logs; manage it as project data.

## Execution and isolation

The selected model and ordinary SQL ancestors are resolved from dbt's manifest. Each step loads identical snapshots into isolated databases, runs the incremental branch normally, and rebuilds the reference branch with `--full-refresh`. Upstream table/view models execute each time; ephemeral models are compiled by dbt. Run-results must confirm the exact selected executable model set succeeded.

Both workspaces use the target name `replay`, schema `main`, UTC database timezone, and basename `replay.duckdb`. Source/model schemas and identifiers come from the manifest. The original project is copied and its profile is replaced in the copies. Dedicated profiles, build paths and log paths are supplied explicitly. Inherited `DBT_*` variables are removed from dbt subprocesses, and dbt telemetry is disabled. Other environment variables remain available for project `env_var()` calls. dbt commands have a five-minute timeout.

**Macros and hooks are executable project code. Local database isolation is not a security sandbox.** Run trusted projects only. SQL or hooks can access files, network services, and other resources. Undeclared reads and hook side effects are outside the fixture contract. Default production profiles are never loaded by the runner.

## Compatibility and limitations

Locally verified: Python 3.11.14, dbt Core 1.11.15, dbt-duckdb 1.11.0, DuckDB 1.5.6 on macOS ARM64. The install range is Python 3.10–3.13, dbt Core 1.11.x, dbt-duckdb 1.11.0, and DuckDB 1.5.x. CI is configured to check Python 3.11 and 3.13 on Linux; those hosted runs have not been executed as part of local development.

Only manifest **v12** and run-results **v6** artifacts are accepted. Artifact versions are checked separately from package versions. The supported strategies are explicitly configured `append` and `delete+insert`. Models overriding `full_refresh` are rejected.

Initially excluded: upstream incremental models, snapshots, seeds, Python models, remote/external sources, other databases, other incremental strategies, expected-output assertions, automatic scenario generation, and failure minimization. All dependencies must resolve through declared `source()` and `ref()` edges to fixture-backed sources and supported SQL models. Existing installed dbt packages can be copied with the project; dbt-replay does not run `dbt deps`. Project resource paths must stay inside the copied project.

Results and fixture snapshots are held in memory, and fixture inserts favor simplicity over bulk-loading throughput. This release targets small fixtures and tables. Scenarios run serially and evidence is not automatically pruned.

**DuckDB tests do not validate Snowflake or Spark behavior.** Before claiming real-world usefulness, validate against an external project and expand the compatibility suite deliberately.

## Development

```sh
pip install -e '.[dev]'
ruff check src tests
ruff format --check src tests
pytest -q
python -m build
```

Integration tests execute real dbt materializations for corrected models, equal-timestamp watermark loss, late-arrival aggregates, updates, rerun duplicates, legitimate append behavior, nulls/decimals/timestamps, execution errors, unsupported dependencies, unstable references, controlled time, profile isolation and retained-scenario reruns. Unit checks cover strict fixtures, schema changes, diagnostic keys and bounded multiset differences. All example data is synthetic.

Future work: generate valid source-change sequences with Hypothesis stateful testing, then shrink failing sequences and fixtures into small reproducers.

## References

- [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests)
- [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt manifest artifacts](https://docs.getdbt.com/reference/artifacts/manifest-json)
- [dbt run-results artifacts](https://docs.getdbt.com/reference/artifacts/run-results-json)
- [dbt-duckdb](https://github.com/duckdb/dbt-duckdb)
- [Hypothesis stateful testing](https://hypothesis.readthedocs.io/en/latest/stateful.html)

## License

MIT. See [LICENSE](LICENSE).
