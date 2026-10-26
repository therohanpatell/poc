#!/usr/bin/env python3
"""Restore a native BigQuery snapshot into an existing, empty table.

This file is deliberately self-contained: copy it to any directory and install
google-cloud-bigquery. It never imports backup.py or another project module.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence, TextIO

from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery


class ValidationError(Exception):
    """Invalid arguments, metadata, schema, or data; exit code 1."""


class SchemaValidationError(ValidationError):
    """The source cannot be inserted without changing its schema or values."""


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValidationError(f"CLI_VALIDATION_FAILED: {message}")


@dataclass(frozen=True)
class TableId:
    project: str
    dataset: str
    table: str

    def __str__(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"

    @property
    def sql(self) -> str:
        return f"`{self}`"


@dataclass(frozen=True)
class SchemaComparison:
    common: tuple[tuple[bigquery.SchemaField, bigquery.SchemaField], ...]
    target_only: tuple[bigquery.SchemaField, ...]
    source_only: tuple[bigquery.SchemaField, ...]
    mismatches: tuple[str, ...]


@dataclass(frozen=True)
class RowStatistics:
    eligible_rows: int
    null_rows: int

    @property
    def total_rows(self) -> int:
        return self.eligible_rows + self.null_rows


def parse_table_reference(value: str) -> TableId:
    """Accept conservative project.dataset.table IDs, never SQL fragments."""
    parts = value.split(".")
    if (
        len(parts) != 3
        or not re.fullmatch(r"[a-z][a-z0-9-]{4,61}[a-z0-9]", parts[0])
        or not re.fullmatch(r"[A-Za-z0-9_]{1,1024}", parts[1])
        or not re.fullmatch(r"[A-Za-z0-9_]{1,1024}", parts[2])
    ):
        raise ValidationError(
            f"Invalid table identifier: {value!r}. Expected project.dataset.table "
            "with a standard 6-63 character project ID and simple dataset/table names."
        )
    return TableId(*parts)


def quote_column(value: str) -> str:
    """Quote schema names, including flexible names, with GoogleSQL escapes."""
    if not value or any(ord(char) < 32 for char in value):
        raise ValidationError(f"Invalid column name: {value!r}")
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = Parser(description=__doc__)
    parser.add_argument("--source-snapshot", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--partition-column", default="partition_dt")
    parser.add_argument("--source-partition-column")
    parser.add_argument("--target-partition-column")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--billing-project", help="Project charged for query jobs; defaults to target project")
    parser.add_argument("--location", help="Must match both tables; otherwise discovered from metadata")
    parser.add_argument("--audit-log", type=Path, help="Append structured JSON events to this local file")
    args = parser.parse_args(argv)
    args.source = parse_table_reference(args.source_snapshot)
    args.target = parse_table_reference(args.target_table)
    args.source_partition_column = args.source_partition_column or args.partition_column
    args.target_partition_column = args.target_partition_column or args.partition_column
    for name in (args.source_partition_column, args.target_partition_column):
        quote_column(name)  # It must also resolve against trusted metadata below.
    if args.source == args.target:
        raise ValidationError("Source snapshot and target table must be different.")
    return args


def get_bigquery_client(project: str) -> bigquery.Client:
    """Use Application Default Credentials; never load hardcoded credentials."""
    return bigquery.Client(project=project)


def get_table_metadata(client: bigquery.Client, table_id: TableId, role: str) -> bigquery.Table:
    try:
        return client.get_table(str(table_id))
    except NotFound as exc:
        raise ValidationError(f"{role} table does not exist: {table_id}") from exc


def validate_source_snapshot(table: bigquery.Table) -> None:
    if table.table_type != "SNAPSHOT" or table.snapshot_definition is None:
        raise ValidationError("Source must be a native BigQuery SNAPSHOT table.")
    if not table.schema:
        raise SchemaValidationError("SCHEMA_VALIDATION_FAILED: Source schema is empty.")


def validate_target_table(table: bigquery.Table) -> None:
    if table.table_type != "TABLE":
        raise ValidationError("Target must be an existing native BigQuery TABLE.")
    if not table.schema:
        raise SchemaValidationError("SCHEMA_VALIDATION_FAILED: Target schema is empty.")


def schema_index(schema: Sequence[bigquery.SchemaField]) -> dict[str, bigquery.SchemaField]:
    fields: dict[str, bigquery.SchemaField] = {}
    for field in schema:
        quote_column(field.name)
        key = field.name.casefold()
        if key in fields:
            raise SchemaValidationError(f"Duplicate column name: {field.name}")
        fields[key] = field
    return fields


def validate_partition_column(table: bigquery.Table, name: str) -> bigquery.SchemaField:
    """Require a real column partition, not an ingestion-time pseudocolumn."""
    field = schema_index(table.schema).get(name.casefold())
    if field is None:
        raise ValidationError(f"RESTORE_VALIDATION_FAILED: Missing partition column: {name}")
    if field.field_type.upper() != "DATE" or field.mode.upper() != "REQUIRED":
        raise ValidationError(
            f"RESTORE_VALIDATION_FAILED: Partition column {field.name} must be DATE REQUIRED; "
            f"found {field.field_type} {field.mode}."
        )
    partitioning = table.time_partitioning
    if not partitioning or not partitioning.field or partitioning.field.casefold() != field.name.casefold():
        raise ValidationError(f"Table must be physically partitioned on {field.name}.")
    return field


def schema_differences(source: bigquery.SchemaField, target: bigquery.SchemaField, path: str) -> list[str]:
    """Compare insertion-relevant metadata recursively, without automatic casts."""
    aliases = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL", "RECORD": "STRUCT",
               "DECIMAL": "NUMERIC", "BIGDECIMAL": "BIGNUMERIC"}
    left, right = source.to_api_repr(), target.to_api_repr()
    differences = []
    for key, default in (("type", ""), ("mode", "NULLABLE"), ("precision", None),
                         ("scale", None), ("maxLength", None), ("collation", None),
                         ("roundingMode", None), ("rangeElementType", None)):
        a, b = left.get(key, default), right.get(key, default)
        if key == "type":
            a, b = aliases.get(str(a).upper(), str(a).upper()), aliases.get(str(b).upper(), str(b).upper())
        if key in {"precision", "scale", "maxLength"}:
            a, b = str(a) if a is not None else None, str(b) if b is not None else None
        if a != b:
            differences.append(f"{path}: {key} source={a!r}, target={b!r}")
    if len(source.fields) != len(target.fields):
        differences.append(f"{path}: nested field count differs")
    for a, b in zip(source.fields, target.fields):
        if a.name.casefold() != b.name.casefold():
            differences.append(f"{path}: nested field order/name differs ({a.name}, {b.name})")
        differences.extend(schema_differences(a, b, f"{path}.{b.name}"))
    return differences


def compare_schemas(
    source: Sequence[bigquery.SchemaField], target: Sequence[bigquery.SchemaField],
    source_partition: str, target_partition: str,
) -> SchemaComparison:
    """Map the explicit partition rename, then match other fields by name."""
    left, right = schema_index(source), schema_index(target)
    sp, tp = source_partition.casefold(), target_partition.casefold()
    if sp != tp and (tp in left or sp in right):
        raise SchemaValidationError("SCHEMA_VALIDATION_FAILED: Partition rename conflicts with an existing column.")
    common, target_only, mismatches = [], [], []
    consumed: set[str] = set()
    for target_field in target:
        source_name = sp if target_field.name.casefold() == tp else target_field.name.casefold()
        source_field = left.get(source_name)
        if source_field is None:
            target_only.append(target_field)
            continue
        common.append((source_field, target_field))
        consumed.add(source_name)
        issues = schema_differences(source_field, target_field, target_field.name)
        if issues:
            mismatches.append("; ".join(issues))
    source_only = tuple(field for field in source if field.name.casefold() not in consumed)
    return SchemaComparison(tuple(common), tuple(target_only), source_only, tuple(mismatches))


def validate_common_columns(comparison: SchemaComparison) -> None:
    if comparison.mismatches:
        raise SchemaValidationError(
            "SCHEMA_VALIDATION_FAILED: " + "\n".join(comparison.mismatches) + "\nAutomatic casting is not allowed."
        )


def validate_new_target_columns(comparison: SchemaComparison) -> None:
    for field in comparison.target_only:
        if field.mode.upper() != "NULLABLE":
            raise SchemaValidationError(
                f"SCHEMA_VALIDATION_FAILED: Target contains new {field.mode} column: {field.name}. "
                "Source does not contain this column. NULL cannot be inserted. Restore aborted."
            )


def query_rows(
    client: bigquery.Client, sql: str, location: str, execution_id: str,
    *, dry_run: bool = False, parameters: Sequence[Any] = (),
) -> list[Any]:
    """Use explicit job IDs and disable whole-job re-execution for safe writes."""
    config = bigquery.QueryJobConfig(
        use_legacy_sql=False, use_query_cache=False, dry_run=dry_run,
        query_parameters=list(parameters), labels={"utility": "snapshot_restore", "execution_id": execution_id},
    )
    job_id = f"restore_{execution_id}_{uuid.uuid4().hex}"
    job = client.query(sql, job_config=config, location=location, job_id=job_id, job_retry=None)
    return list(job.result(job_retry=None))


def get_row_statistics(client: bigquery.Client, table: TableId, partition: str, location: str, execution_id: str) -> RowStatistics:
    """Count NULL and non-NULL partitions separately so every scan has a filter."""
    column = quote_column(partition)
    sql = (
        f"SELECT\n  (SELECT COUNT(*) FROM {table.sql} WHERE {column} IS NOT NULL) AS eligible_rows,\n"
        f"  (SELECT COUNT(*) FROM {table.sql} WHERE {column} IS NULL) AS null_rows"
    )
    row = query_rows(client, sql, location, execution_id)[0]
    return RowStatistics(int(row["eligible_rows"]), int(row["null_rows"]))


def validate_partition_nulls(stats: RowStatistics, role: str) -> None:
    if stats.null_rows:
        raise ValidationError(f"RESTORE_VALIDATION_FAILED: {role} contains {stats.null_rows:,} NULL partition rows.")


def get_partition_counts(client: bigquery.Client, table: TableId, partition: str, location: str, execution_id: str) -> dict[date, int]:
    column = quote_column(partition)
    sql = (f"SELECT {column} AS partition_value, COUNT(*) AS row_count\n"
           f"FROM {table.sql}\nWHERE {column} IS NOT NULL\nGROUP BY {column}")
    return {row["partition_value"]: int(row["row_count"]) for row in query_rows(client, sql, location, execution_id)}


def build_restore_sql(source: TableId, target: TableId, target_schema: Sequence[bigquery.SchemaField], comparison: SchemaComparison, source_partition: str) -> str:
    """Build an explicit ordered INSERT/SELECT. Target-only values are NULL."""
    mapping = {target_field.name.casefold(): source_field for source_field, target_field in comparison.common}
    columns, expressions = [], []
    for field in target_schema:
        column = quote_column(field.name)
        columns.append(f"    {column}")
        source_field = mapping.get(field.name.casefold())
        expression = f"s.{quote_column(source_field.name)}" if source_field else "NULL"
        expressions.append(f"    {expression} AS {column}")
    return (
        f"INSERT INTO {target.sql}\n(\n" + ",\n".join(columns) + "\n)\nSELECT\n"
        + ",\n".join(expressions) + f"\nFROM {source.sql} AS s\n"
        f"WHERE s.{quote_column(source_partition)} IS NOT NULL"
    )


def build_transaction_sql(insert_sql: str, source: TableId, target: TableId, source_partition: str, target_partition: str) -> str:
    """Validate the inserted data before COMMIT; failed assertions roll back."""
    sp, tp = quote_column(source_partition), quote_column(target_partition)
    return f"""DECLARE inserted_rows INT64 DEFAULT 0;
DECLARE target_final_rows INT64 DEFAULT 0;
BEGIN TRANSACTION;
ASSERT (SELECT COUNT(*) FROM {target.sql} WHERE {tp} IS NOT NULL) = 0
  AS 'RESTORE_VALIDATION_FAILED: Target table is not empty. Restore aborted.';
ASSERT (SELECT COUNT(*) FROM {target.sql} WHERE {tp} IS NULL) = 0
  AS 'RESTORE_VALIDATION_FAILED: Target contains NULL partition rows.';
ASSERT (SELECT COUNT(*) FROM {source.sql} WHERE {sp} IS NULL) = 0
  AS 'RESTORE_VALIDATION_FAILED: Source contains NULL partition rows.';
{insert_sql};
SET inserted_rows = @@row_count;
SET target_final_rows = (SELECT COUNT(*) FROM {target.sql} WHERE {tp} IS NOT NULL);
ASSERT inserted_rows = @expected_rows AND target_final_rows = @expected_rows
  AS 'RESTORE_VALIDATION_FAILED: Row count mismatch.';
ASSERT (SELECT COUNT(*) FROM {target.sql} WHERE {tp} IS NULL) = 0
  AS 'RESTORE_VALIDATION_FAILED: Target contains NULL partition rows.';
ASSERT NOT EXISTS (
  SELECT 1
  FROM (
    SELECT {sp} AS partition_value, COUNT(*) AS row_count
    FROM {source.sql} WHERE {sp} IS NOT NULL GROUP BY {sp}
  ) AS source_counts
  FULL OUTER JOIN (
    SELECT {tp} AS partition_value, COUNT(*) AS row_count
    FROM {target.sql} WHERE {tp} IS NOT NULL GROUP BY {tp}
  ) AS target_counts
  ON source_counts.partition_value = target_counts.partition_value
  WHERE source_counts.row_count IS DISTINCT FROM target_counts.row_count
) AS 'RESTORE_VALIDATION_FAILED: Partition counts mismatch.';
COMMIT TRANSACTION;
SELECT inserted_rows AS rows_inserted, target_final_rows AS target_final_rows;
"""


def validate_restore(source_rows: int, target_rows: int, source_counts: dict[date, int], target_counts: dict[date, int], rows_inserted: int) -> None:
    if source_rows != target_rows or source_rows != rows_inserted:
        raise ValidationError(
            f"RESTORE_VALIDATION_FAILED: Row count mismatch: source={source_rows}, "
            f"inserted={rows_inserted}, target={target_rows}."
        )
    if source_counts != target_counts:
        raise ValidationError("RESTORE_VALIDATION_FAILED: Partition-level counts do not match.")


def print_validation(audit: dict[str, Any], comparison: SchemaComparison, sql: str, dry_run: bool) -> None:
    print("=" * 60 + "\nRESTORE VALIDATION\n" + "=" * 60)
    print(f"Source Snapshot: {audit['source_snapshot']}\nTarget: {audit['target_table']}")
    print(f"Source Rows: {audit['source_row_count']:,}\nTarget Rows: {audit['target_initial_row_count']:,}")
    print(f"Source Columns: {audit['source_column_count']}\nTarget Columns: {audit['target_column_count']}")
    print(f"Common Columns: {len(comparison.common)}\nTarget-only Columns: {len(comparison.target_only)}\nSource-only Columns: {len(comparison.source_only)}")
    print("Target-only nullable columns:")
    for field in comparison.target_only:
        print(f"  {field.name} {field.field_type}")
    if not comparison.target_only:
        print("  None")
    print("Datatype mismatches: None")
    print(f"Partition Column: {audit['source_partition_column']} -> {audit['target_partition_column']} (DATE REQUIRED)")
    print(f"NULL partition rows: {audit['null_partition_count']}\nValidation: PASSED")
    if dry_run:
        print("Restore: NOT EXECUTED (DRY RUN)")
    print("\nGenerated INSERT SQL:\n" + sql + ";\n" + "=" * 60)


def print_summary(audit: dict[str, Any]) -> None:
    print("=" * 60 + "\nRESTORE SUMMARY\n" + "=" * 60)
    print(f"Source: {audit['source_snapshot']}\nTarget: {audit['target_table']}")
    print(f"Source eligible rows : {audit['source_row_count']:,}")
    print(f"Rows inserted        : {audit['rows_inserted']:,}")
    print(f"Target final rows    : {audit['target_final_row_count']:,}")
    print("Schema validation    : PASSED\nPartition validation : PASSED\nRow validation       : PASSED\nPartition counts     : PASSED")
    print("STATUS: SUCCESS\n" + "=" * 60)


def write_audit(audit: dict[str, Any], handle: TextIO | None = None) -> None:
    line = json.dumps(audit, default=str, sort_keys=True)
    print(line, file=sys.stderr, flush=True)
    if handle:
        handle.write(line + "\n")
        handle.flush()


def run_restore(args: argparse.Namespace, client: bigquery.Client, audit: dict[str, Any], audit_handle: TextIO | None) -> None:
    source = get_table_metadata(client, args.source, "Source snapshot")
    target = get_table_metadata(client, args.target, "Target")
    validate_source_snapshot(source)
    validate_target_table(target)
    sp = validate_partition_column(source, args.source_partition_column).name
    tp = validate_partition_column(target, args.target_partition_column).name
    location = source.location
    if not location or not target.location or location.casefold() != target.location.casefold():
        raise ValidationError("Source and target must have the same BigQuery location.")
    if args.location and args.location.casefold() != location.casefold():
        raise ValidationError("--location must match the source and target location.")
    audit.update(source_partition_column=sp, target_partition_column=tp, location=location,
                 source_column_count=len(source.schema), target_column_count=len(target.schema))
    comparison = compare_schemas(source.schema, target.schema, sp, tp)
    audit.update(common_column_count=len(comparison.common), target_only_column_count=len(comparison.target_only),
                 source_only_column_count=len(comparison.source_only), datatype_mismatch_count=len(comparison.mismatches))
    validate_common_columns(comparison)
    validate_new_target_columns(comparison)
    source_stats = get_row_statistics(client, args.source, sp, location, audit["execution_id"])
    audit.update(source_row_count=source_stats.eligible_rows, null_partition_count=source_stats.null_rows)
    validate_partition_nulls(source_stats, "Source snapshot")
    target_stats = get_row_statistics(client, args.target, tp, location, audit["execution_id"])
    audit.update(target_initial_row_count=target_stats.total_rows, target_null_partition_count=target_stats.null_rows)
    if target_stats.total_rows:
        raise ValidationError("Target table is not empty. Restore aborted.")
    validate_partition_nulls(target_stats, "Target")
    source_counts = get_partition_counts(client, args.source, sp, location, audit["execution_id"])
    if sum(source_counts.values()) != source_stats.eligible_rows:
        raise ValidationError("RESTORE_VALIDATION_FAILED: Source partition counts do not sum to its row count.")
    insert_sql = build_restore_sql(args.source, args.target, target.schema, comparison, sp)
    # Compile the actual projection/INSERT on the server as well as inspecting metadata.
    # Read-only validation queries above still execute during a utility dry run.
    query_rows(client, insert_sql, location, audit["execution_id"], dry_run=True)
    print_validation(audit, comparison, insert_sql, args.dry_run)
    if args.dry_run:
        audit.update(status="DRY_RUN_PASSED", rows_inserted=0, target_final_row_count=0)
        return

    sql = build_transaction_sql(insert_sql, args.source, args.target, sp, tp)
    config = bigquery.QueryJobConfig(
        use_legacy_sql=False, use_query_cache=False,
        query_parameters=[bigquery.ScalarQueryParameter("expected_rows", "INT64", source_stats.eligible_rows)],
        labels={"utility": "snapshot_restore", "execution_id": audit["execution_id"]},
    )
    # Record the stable write job ID before submission, including on uncertain network outcomes.
    audit.update(job_id=f"restore_{audit['execution_id']}_insert", status="SUBMITTING", committed=False)
    write_audit(audit, audit_handle)
    audit.update(committed=None, transaction_status="UNKNOWN")
    job = client.query(sql, job_config=config, location=location, job_id=audit["job_id"], job_retry=None)
    try:
        result = list(job.result(job_retry=None))
    except BadRequest as exc:
        if "RESTORE_VALIDATION_FAILED" in str(exc):
            audit.update(committed=False, transaction_status="ROLLED_BACK")
            raise ValidationError(str(exc)) from exc
        raise
    # Successful completion means COMMIT ran, even if a subsequent check fails.
    audit.update(committed=True, transaction_status="COMMITTED")
    if len(result) != 1:
        raise RuntimeError("Committed restore did not return a row-count result; inspect the recorded job ID.")
    inserted = int(result[0]["rows_inserted"])
    audit.update(rows_inserted=inserted, target_final_row_count=int(result[0]["target_final_rows"]))
    final_source = get_row_statistics(client, args.source, sp, location, audit["execution_id"])
    final_target = get_row_statistics(client, args.target, tp, location, audit["execution_id"])
    audit.update(target_final_row_count=final_target.total_rows)
    validate_partition_nulls(final_source, "Source snapshot")
    validate_partition_nulls(final_target, "Target")
    if final_source.eligible_rows != source_stats.eligible_rows:
        raise ValidationError("RESTORE_VALIDATION_FAILED: Source row count changed during restore.")
    final_counts = get_partition_counts(client, args.target, tp, location, audit["execution_id"])
    validate_restore(final_source.eligible_rows, final_target.total_rows, source_counts, final_counts, inserted)
    audit.update(status="SUCCESS")
    print_summary(audit)


def main(argv: Sequence[str] | None = None) -> int:
    audit: dict[str, Any] = {
        "execution_id": uuid.uuid4().hex, "execution_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_snapshot": None, "target_table": None, "source_partition_column": None,
        "target_partition_column": None, "source_row_count": None, "target_initial_row_count": None,
        "target_final_row_count": None, "common_column_count": None, "target_only_column_count": None,
        "source_only_column_count": None, "datatype_mismatch_count": None, "null_partition_count": None,
        "rows_inserted": None, "committed": False, "status": "STARTED", "error_message": None,
    }
    handle: TextIO | None = None
    client: bigquery.Client | None = None
    code = 0
    try:
        args = parse_args(argv)
        audit.update(source_snapshot=str(args.source), target_table=str(args.target),
                     source_partition_column=args.source_partition_column, target_partition_column=args.target_partition_column)
        if args.audit_log:
            handle = args.audit_log.open("a", encoding="utf-8")
        client = get_bigquery_client(args.billing_project or args.target.project)
        run_restore(args, client, audit, handle)
    except ValidationError as exc:
        code = 1
        audit.update(status="VALIDATION_FAILED", error_message=str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        code = 2
        audit.update(status="EXECUTION_FAILED", error_message="Interrupted. Inspect the recorded write job ID before retrying.")
        print(f"ERROR: {audit['error_message']}", file=sys.stderr)
    except Exception as exc:
        code = 2
        audit.update(status="EXECUTION_FAILED", error_message=f"{type(exc).__name__}: {exc}")
        print(f"ERROR: {audit['error_message']}", file=sys.stderr)
    finally:
        try:
            # argparse --help exits normally without a misleading audit failure.
            if audit["status"] != "STARTED":
                write_audit(audit, handle)
        except OSError as exc:
            code = 2
            print(f"ERROR: Could not persist audit log: {exc}", file=sys.stderr)
        finally:
            if handle:
                handle.close()
            if client:
                client.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
