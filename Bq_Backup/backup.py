#!/usr/bin/env python3
"""Create same-dataset native BigQuery snapshots of one or more tables.

Self-contained by design. This file never imports restore.py or a shared local
module. Authentication uses Application Default Credentials.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence, TextIO

from google.api_core.exceptions import BadRequest, Conflict, Forbidden, NotFound
from google.cloud import bigquery


class ValidationError(Exception):
    """Invalid arguments, table metadata, or partition data; exit code 1."""


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
class BackupPlan:
    source: TableId
    snapshot: TableId
    partition: str
    location: str
    source_rows: int


def parse_table_reference(value: str) -> TableId:
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
    if not value or any(ord(char) < 32 for char in value):
        raise ValidationError(f"Invalid column name: {value!r}")
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = Parser(description=__doc__)
    parser.add_argument("--tables", "--table", nargs="+", action="append", required=True,
                        help="One or more project.dataset.table IDs; may be repeated")
    parser.add_argument("--partition-column", default="partition_dt")
    parser.add_argument("--retention-days", type=int, default=7)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--billing-project", help="Project charged for jobs; defaults to each source project")
    parser.add_argument("--location", help="Optional required location for every source")
    parser.add_argument("--audit-log", type=Path, help="Append structured JSON events to this local file")
    args = parser.parse_args(argv)
    if not 1 <= args.retention_days <= 36500:
        raise ValidationError("--retention-days must be an integer between 1 and 36500.")
    args.sources = [parse_table_reference(value) for group in args.tables for value in group]
    if len({str(source).casefold() for source in args.sources}) != len(args.sources):
        raise ValidationError("Duplicate source tables are not allowed.")
    quote_column(args.partition_column)
    return args


def get_bigquery_client(project: str) -> bigquery.Client:
    return bigquery.Client(project=project)


def snapshot_reference(source: TableId, timestamp: datetime) -> TableId:
    """Use one UTC timestamp across the batch, with the supplied _vw suffix."""
    if timestamp.tzinfo is None:
        raise ValidationError("Snapshot timestamp must be timezone-aware.")
    suffix = timestamp.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return parse_table_reference(f"{source.project}.{source.dataset}.{source.table}_snapshot_{suffix}_vw")


def validate_partition_column(table: bigquery.Table, name: str) -> bigquery.SchemaField:
    fields = {field.name.casefold(): field for field in table.schema}
    if len(fields) != len(table.schema):
        raise ValidationError("Source contains duplicate column names.")
    field = fields.get(name.casefold())
    if field is None:
        raise ValidationError(f"BACKUP_VALIDATION_FAILED: Missing partition column: {name}")
    quote_column(field.name)
    if field.field_type.upper() != "DATE" or field.mode.upper() != "REQUIRED":
        raise ValidationError(
            f"BACKUP_VALIDATION_FAILED: Partition column {field.name} must be DATE REQUIRED; "
            f"found {field.field_type} {field.mode}."
        )
    partitioning = table.time_partitioning
    if not partitioning or not partitioning.field or partitioning.field.casefold() != field.name.casefold():
        raise ValidationError(f"Source must be physically partitioned on {field.name}.")
    return field


def query_rows(
    client: bigquery.Client, sql: str, location: str, execution_id: str,
    *, parameters: Sequence[Any] = (), dry_run: bool = False, job_id: str | None = None,
) -> list[Any]:
    config = bigquery.QueryJobConfig(
        use_legacy_sql=False, use_query_cache=False, dry_run=dry_run,
        query_parameters=list(parameters), labels={"utility": "snapshot_backup", "execution_id": execution_id},
    )
    job = client.query(sql, job_config=config, location=location,
                       job_id=job_id or f"backup_{execution_id}_{uuid.uuid4().hex}", job_retry=None)
    return list(job.result(job_retry=None))


def get_row_counts(
    client: bigquery.Client, table: TableId, partition: str, location: str,
    execution_id: str, snapshot_time: datetime | None = None,
) -> tuple[int, int]:
    """Read the same historical instant used by CREATE SNAPSHOT TABLE."""
    column = quote_column(partition)
    clause = " FOR SYSTEM_TIME AS OF @snapshot_time" if snapshot_time else ""
    parameters = [bigquery.ScalarQueryParameter("snapshot_time", "TIMESTAMP", snapshot_time)] if snapshot_time else []
    sql = (
        f"SELECT\n  (SELECT COUNT(*) FROM {table.sql}{clause} WHERE {column} IS NOT NULL) AS eligible_rows,\n"
        f"  (SELECT COUNT(*) FROM {table.sql}{clause} WHERE {column} IS NULL) AS null_rows"
    )
    row = query_rows(client, sql, location, execution_id, parameters=parameters)[0]
    return int(row["eligible_rows"]), int(row["null_rows"])


def build_snapshot_sql(source: TableId, snapshot: TableId) -> str:
    if (source.project, source.dataset) != (snapshot.project, snapshot.dataset):
        raise ValidationError("Snapshots must be created in the same dataset as the source.")
    return (
        f"CREATE SNAPSHOT TABLE {snapshot.sql}\nCLONE {source.sql}\n"
        "FOR SYSTEM_TIME AS OF @snapshot_time\nOPTIONS (expiration_timestamp = @expires_at)"
    )


def preflight(
    client: bigquery.Client, source: TableId, args: argparse.Namespace,
    timestamp: datetime, expires_at: datetime, execution_id: str,
) -> BackupPlan:
    """Complete validation and a server-side DDL dry run before any writes."""
    try:
        table = client.get_table(str(source))
    except NotFound as exc:
        raise ValidationError(f"Source table does not exist: {source}") from exc
    if table.table_type != "TABLE":
        raise ValidationError(f"Source must be a native BigQuery TABLE: {source}")
    if not table.schema:
        raise ValidationError(f"Source schema is empty: {source}")
    if table.streaming_buffer is not None:
        raise ValidationError(f"Source has a streaming buffer that snapshots cannot fully capture: {source}. Retry after it drains.")
    partition = validate_partition_column(table, args.partition_column).name
    if not table.location or (args.location and args.location.casefold() != table.location.casefold()):
        raise ValidationError(f"Source location does not match --location, or metadata has no location: {source}")
    snapshot = snapshot_reference(source, timestamp)
    try:
        client.get_table(str(snapshot))
    except NotFound:
        pass
    else:
        raise ValidationError(f"Snapshot already exists; it will not be overwritten: {snapshot}")
    rows, nulls = get_row_counts(client, source, partition, table.location, execution_id, timestamp)
    if nulls:
        raise ValidationError(f"BACKUP_VALIDATION_FAILED: {source} contains {nulls:,} NULL partition rows.")
    parameters = [bigquery.ScalarQueryParameter("snapshot_time", "TIMESTAMP", timestamp),
                  bigquery.ScalarQueryParameter("expires_at", "TIMESTAMP", expires_at)]
    query_rows(client, build_snapshot_sql(source, snapshot), table.location, execution_id,
               parameters=parameters, dry_run=True)
    return BackupPlan(source, snapshot, partition, table.location, rows)


def write_audit(audit: dict[str, Any], handle: TextIO | None = None) -> None:
    line = json.dumps(audit, default=str, sort_keys=True)
    print(line, file=sys.stderr, flush=True)
    if handle:
        handle.write(line + "\n")
        handle.flush()


def execute_backup(
    client: bigquery.Client, plan: BackupPlan, timestamp: datetime, expires_at: datetime,
    index: int, audit: dict[str, Any], handle: TextIO | None,
) -> None:
    """Create once and verify the native snapshot. Never overwrite or delete."""
    job_id = f"backup_{audit['execution_id']}_{index}"
    record: dict[str, Any] = {
        "source_table": str(plan.source), "snapshot_table": str(plan.snapshot),
        "partition_column": plan.partition, "source_row_count": plan.source_rows,
        "snapshot_row_count": None, "location": plan.location, "job_id": job_id,
        "status": "SUBMITTING", "created": False,
    }
    audit["tables"].append(record)
    write_audit({**audit, "status": "IN_PROGRESS"}, handle)
    parameters = [bigquery.ScalarQueryParameter("snapshot_time", "TIMESTAMP", timestamp),
                  bigquery.ScalarQueryParameter("expires_at", "TIMESTAMP", expires_at)]
    # A transport error after submission cannot establish whether creation ran.
    record["created"] = None
    try:
        query_rows(client, build_snapshot_sql(plan.source, plan.snapshot), plan.location,
                   audit["execution_id"], parameters=parameters, job_id=job_id)
        record.update(created=True, status="CREATED")
        snapshot = client.get_table(str(plan.snapshot))
        definition = snapshot.snapshot_definition
        if snapshot.table_type != "SNAPSHOT" or definition is None:
            raise ValidationError(f"BACKUP_VALIDATION_FAILED: Destination is not a native snapshot: {plan.snapshot}")
        if str(definition.base_table_reference) != str(plan.source):
            raise ValidationError(f"BACKUP_VALIDATION_FAILED: Snapshot base table differs: {plan.snapshot}")
        if definition.snapshot_time != timestamp:
            raise ValidationError(f"BACKUP_VALIDATION_FAILED: Snapshot timestamp differs: {plan.snapshot}")
        if snapshot.expires is None or abs((snapshot.expires - expires_at).total_seconds()) > 0.001:
            raise ValidationError(f"BACKUP_VALIDATION_FAILED: Snapshot retention differs: {plan.snapshot}")
        validate_partition_column(snapshot, plan.partition)
        rows, nulls = get_row_counts(client, plan.snapshot, plan.partition, plan.location, audit["execution_id"])
        record.update(snapshot_row_count=rows, null_partition_count=nulls)
        if nulls or rows != plan.source_rows:
            raise ValidationError(f"BACKUP_VALIDATION_FAILED: Snapshot row/partition validation failed: {plan.snapshot}")
        record["status"] = "SUCCESS"
    except Exception as exc:
        if isinstance(exc, (BadRequest, Conflict, Forbidden, NotFound)) and record["created"] is None:
            record["created"] = False
        record.update(status="FAILED", error_message=str(exc))
        raise


def main(argv: Sequence[str] | None = None) -> int:
    # Millisecond precision matches the snapshot metadata returned by the API.
    timestamp = datetime.now(timezone.utc)
    timestamp = timestamp.replace(microsecond=(timestamp.microsecond // 1000) * 1000)
    audit: dict[str, Any] = {
        "execution_id": uuid.uuid4().hex, "execution_timestamp": timestamp.isoformat(),
        "snapshot_time": timestamp.isoformat(), "retention_days": None, "expires_at": None,
        "tables": [], "status": "STARTED", "error_message": None,
    }
    clients: dict[str, bigquery.Client] = {}
    handle: TextIO | None = None
    code = 0
    try:
        args = parse_args(argv)
        if args.audit_log:
            handle = args.audit_log.open("a", encoding="utf-8")
        expires_at = timestamp + timedelta(days=args.retention_days)
        audit.update(retention_days=args.retention_days, expires_at=expires_at.isoformat())
        plans = []
        for source in args.sources:
            project = args.billing_project or source.project
            if project not in clients:
                clients[project] = get_bigquery_client(project)
            audit["validating_source"] = str(source)
            plans.append(preflight(clients[project], source, args, timestamp, expires_at, audit["execution_id"]))
        audit.pop("validating_source", None)
        print("=" * 60 + "\nBACKUP VALIDATION\n" + "=" * 60)
        print(f"Snapshot time (UTC): {timestamp.isoformat()}\nRetention: {args.retention_days} days\nExpires: {expires_at.isoformat()}")
        for plan in plans:
            print(f"\nSource: {plan.source}\nSnapshot: {plan.snapshot}\nRows: {plan.source_rows:,}\nPartition: {plan.partition} DATE REQUIRED")
            print("Generated SQL:\n" + build_snapshot_sql(plan.source, plan.snapshot) + ";")
        print("Validation: PASSED")
        if args.dry_run:
            audit["tables"] = [dict(source_table=str(plan.source), snapshot_table=str(plan.snapshot),
                                    source_row_count=plan.source_rows, partition_column=plan.partition,
                                    location=plan.location, status="DRY_RUN_PASSED", created=False) for plan in plans]
            audit["status"] = "DRY_RUN_PASSED"
            print("Backup: NOT EXECUTED (DRY RUN)")
        else:
            for index, plan in enumerate(plans):
                client = clients[args.billing_project or plan.source.project]
                execute_backup(client, plan, timestamp, expires_at, index, audit, handle)
                print(f"Created: {plan.snapshot}")
            audit["status"] = "SUCCESS"
            print(f"STATUS: SUCCESS ({len(plans)} snapshots)")
    except ValidationError as exc:
        code = 1
        audit.update(status="VALIDATION_FAILED", error_message=str(exc))
        print(f"ERROR: {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        code = 2
        audit.update(status="EXECUTION_FAILED", error_message="Interrupted. Inspect the recorded snapshot job IDs before retrying.")
        print(f"ERROR: {audit['error_message']}", file=sys.stderr)
    except Exception as exc:
        code = 2
        audit.update(status="EXECUTION_FAILED", error_message=f"{type(exc).__name__}: {exc}")
        print(f"ERROR: {audit['error_message']}", file=sys.stderr)
    finally:
        if code and audit["tables"]:
            audit["partial_success"] = any(table.get("created") for table in audit["tables"])
        try:
            if audit["status"] != "STARTED":
                write_audit(audit, handle)
        except OSError as exc:
            code = 2
            print(f"ERROR: Could not persist audit log: {exc}", file=sys.stderr)
        finally:
            if handle:
                handle.close()
            for client in clients.values():
                client.close()
    return code


if __name__ == "__main__":
    sys.exit(main())
