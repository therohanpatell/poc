# BigQuery snapshot backup and restore

Two independent Python CLIs:

- `backup.py` creates native snapshots of multiple tables in their original datasets.
- `restore.py` restores one snapshot into an existing empty table, accommodating nullable new columns and removed columns.

**Each script is self-contained.** You can copy either script to another folder or deploy it alone. Neither imports the other, and there is no shared application module. Each uses only Python's standard library and the external `google-cloud-bigquery` package. Tests and this README are optional at runtime.

## Install and authenticate

Use Python 3.10 or later:

```powershell
python -m venv .venv
.\.venv\Scripts\python -m pip install -r requirements.txt
```

Use Application Default Credentials: an attached service account/workload identity in production, or `gcloud auth application-default login` for local development if the Google Cloud CLI is installed. No credential files or secrets are bundled. Use a principal with full access to all rows and columns being backed up/restored; row filtering or data masking is incompatible with a full-data restore.

The examples below use `python`; activate the environment or substitute `.\.venv\Scripts\python`.

## Backup

```powershell
python backup.py --tables sample-project.hr.employee sample-project.hr.departments --dry-run
python backup.py --tables sample-project.hr.employee sample-project.hr.departments --audit-log backup-audit.jsonl
python backup.py --tables sample-project.hr.employee --retention-days 14 --partition-column edh_partition_dt
```

`--tables` accepts one or more fully qualified names and may be repeated. `--table` is an alias. All tables in an invocation use the configured partition name. Use separate invocations for tables with different partition names.

Snapshots use the naming convention from the supplied example:

```text
<source_table>_snapshot_<YYYYMMDD>_<HHMMSS>_vw
sample-project.hr.employee_snapshot_20261026_163000_vw
```

The timestamp is **UTC**, shared across the batch. `_vw` is a name suffix only: the resource is a native SNAPSHOT, not a view. Each snapshot uses the same source project and dataset. Expiration defaults to seven days from the batch snapshot timestamp; `--retention-days` accepts integers from 1 through 36500. Name collisions fail without replacing the existing resource; a second invocation within the same second can collide.

Backup validates all source metadata, partition NULL counts, destination availability, and server-side DDL dry runs before creating the first snapshot. Sources must be native tables physically partitioned on the configured DATE REQUIRED field. Ingestion-time and integer-range partitioning are unsupported. Sources with an active streaming buffer fail validation because snapshots exclude buffered data. Pause streaming and other ingest activity before taking a full backup.

The DDL uses `FOR SYSTEM_TIME AS OF @snapshot_time`, and source count validation reads that same instant. New snapshots are verified for resource type, base table, timestamp, expiration, partition metadata, and row count. Expired/unavailable historical versions fail rather than falling back to another instant. Ensure table/dataset expiration policies allow the required retention; verification reports unexpected expiration or lost rows.

Snapshot creation across multiple tables is **not atomic**. If a later creation fails, earlier snapshots remain. The audit records created resources, failed jobs, and partial success. No automatic deletion or cleanup occurs.

## Restore

The target must already exist as a native TABLE and must contain zero rows.

```powershell
python restore.py --source-snapshot sample-project.hr.employee_snapshot_20261026_163000_vw --target-table sample-project.hr.employee_new --dry-run
python restore.py --source-snapshot sample-project.hr.employee_snapshot_20261026_163000_vw --target-table sample-project.hr.employee_new --audit-log restore-audit.jsonl
```

Override the partition name for both tables:

```powershell
python restore.py --source-snapshot sample-project.hr.employee_snapshot_20261026_163000_vw --target-table sample-project.hr.employee_new --partition-column edh_partition_dt
```

Map different source and target partition names:

```powershell
python restore.py --source-snapshot sample-project.hr.employee_snapshot_20261026_163000_vw --target-table sample-project.hr.employee_new --source-partition-column partition_dt --target-partition-column edh_partition_dt
```

Each specific partition flag overrides `--partition-column` for that side; the shared default is `partition_dt`. Names resolve case-insensitively to actual schema fields. Ambiguous renames that collide with other columns fail validation.

Restore behavior:

- Source must be a native SNAPSHOT. Target must be an existing, empty native TABLE in the same location.
- Both tables must be physically partitioned on their configured DATE REQUIRED column. NULL partition rows cause failure.
- Common fields must have compatible identical types, modes, precision, scale, maximum length, collation, rounding mode, RANGE element type, and recursively matching nested schemas/order. BigQuery type aliases such as INTEGER/INT64 are normalized. No automatic casts are generated.
- Target-only NULLABLE fields receive explicit `NULL`, including fields that have defaults. New REQUIRED or REPEATED fields fail validation; values are never invented.
- Source-only fields are ignored. Column lists follow target schema order. Common repeated/nested fields are supported when their schemas match; nested schema evolution is rejected.
- The generated INSERT selects explicit columns and always includes `WHERE source_partition_column IS NOT NULL`. There is no partition-date option: this is a full-table restore.
- Every validation scan has a partition predicate, including NULL checks. Total counts combine separate NULL/non-NULL scans so a table requiring a partition filter can still be checked completely.
- Dry-run performs metadata/data validation, source partition grouping, and a BigQuery dry run of the generated INSERT, then prints the SQL. It performs no data writes. Read-only queries still execute and can incur query charges. The principal also needs permission for BigQuery to validate the intended INSERT.

Execution submits one BigQuery transaction. It checks target emptiness again, performs the INSERT, captures `@@row_count`, and asserts row-count and per-partition-count agreement **before committing**. A server assertion failure rolls the transaction back automatically. The CLI then independently checks source/target counts and target partition counts after the commit.

**Give restore exclusive access to write the target from preflight through final verification.** BigQuery permits concurrent appends, including streaming and load jobs, during an insert-only transaction. Its snapshot isolation cannot lock out those writers. Disable competing writers/restore runs and schema changes through your orchestrator. The CLI does not alter or delete rows to acquire locks.

An error after COMMIT (for example, concurrent appended data or a failed verification query) cannot undo that commit. The audit explicitly records `committed: true`. Inspect the target and recorded job before any retry; the utility never truncates, drops, creates, overwrites, or cleans up the restore target. A fresh invocation rejects a nonempty target.

Validation guarantees row totals and exact counts grouped by DATE. It does not provide content checksums or prove that every individual value matches; optional checksum mode was not implemented.

## Operational options and audit

Both scripts support:

| Option | Meaning |
| --- | --- |
| `--dry-run` | Validate without creating snapshots or inserting rows |
| `--partition-column` | Shared partition name, default `partition_dt` |
| `--billing-project` | Project charged for jobs; restore defaults to target project, backup to each source project |
| `--location` | Optional location constraint; actual job location is taken from table metadata |
| `--audit-log PATH` | Append JSON lines to an existing local directory; log file is created if needed |

Human-readable SQL/reports go to stdout. Structured JSON audit events and readable errors go to stderr. `--audit-log` additionally persists JSON events; its path is opened before any BigQuery client or write operation. Directories are not automatically created.

Restore audit includes execution ID/time, source/target IDs, partition names, source/initial/final counts, schema category counts, mismatch and NULL counts, inserted rows, status, error message, and write job ID. Fields that cannot be determined because validation failed early remain `null`. Backup audit contains per-table results and retention information. Job labels also include the execution ID.

Write jobs get explicit stable job IDs recorded **before** submission. Whole-job re-execution is disabled. If a network failure or interruption leaves the outcome uncertain, inspect that job in its billing project and location before rerunning. A failed client call does not prove that the server did not commit/create a snapshot. In the final audit, `committed: null` or `created: null` means the write outcome is unknown.

| Exit code | Meaning |
| --- | --- |
| `0` | Success, successful dry-run, or `--help` |
| `1` | CLI, metadata, schema, or data validation failure |
| `2` | Authentication, permissions, API, network, or local execution failure |

Table IDs deliberately accept standard 6-63 character project IDs and ASCII letters/digits/underscores for dataset/table names (up to 1024 characters each). Decorators, wildcards, domain-scoped project IDs, and flexible table names are rejected. Flexible column names are supported through metadata resolution and escaped backtick quoting.

## Permissions

The job principal needs `bigquery.jobs.create` on the billing project, table metadata/data read access on sources, and metadata/data read access on the restore target. Restore also requires `bigquery.tables.updateData` on the existing target. It requires no target creation or deletion permissions.

Backup additionally requires source `bigquery.tables.createSnapshot`, appropriate dataset access, destination `bigquery.tables.create`/`bigquery.tables.updateData`, and `bigquery.tables.deleteSnapshot` for expiring snapshots. CMEK-protected tables may require access to the encryption key. Grant permissions according to your organization's IAM policy.

These behaviors follow Google's [snapshot creation documentation](https://docs.cloud.google.com/bigquery/docs/table-snapshots-create), [snapshot DDL](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language), [partition filter requirements](https://docs.cloud.google.com/bigquery/docs/querying-partitioned-tables), and [transaction/concurrency semantics](https://docs.cloud.google.com/bigquery/docs/transactions).

## Tests

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

Tests use real SDK metadata/configuration types with mocked BigQuery client calls. They cover CLI parsing, identifiers, snapshots, retention, partition rules, schema evolution and mismatches, SQL safety, dry-run, transaction assertions, count validation, failure/audit behavior, and running each script alone from an isolated directory. They require no GCP project, credentials, or network connection. Actual BigQuery SQL execution requires a project and is not part of these tests.
