"""Backup tests never connect to GCP or modify real tables."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery

import backup


SOURCE = "sample-project.hr.employee"
OTHER = "sample-project.hr.departments"
STAMP = datetime(2026, 10, 26, 16, 30, tzinfo=timezone.utc)


def make_table(name=SOURCE, *, partition="partition_dt", mode="REQUIRED", kind="DATE", table_type="TABLE", location="US"):
    table = bigquery.Table(name, schema=[bigquery.SchemaField("id", "INT64"), bigquery.SchemaField(partition, kind, mode=mode)])
    table.time_partitioning = bigquery.TimePartitioning(field=partition)
    table.require_partition_filter = True
    table._properties.update(type=table_type, location=location)
    return table


def job(rows=(), error=None):
    mock = MagicMock()
    if error:
        mock.result.side_effect = error
    else:
        mock.result.return_value = list(rows)
    return mock


class FakeService:
    """A mocked API with native metadata, time-bound counts, and collision rules."""

    def __init__(self, names=(SOURCE, OTHER), partition="partition_dt"):
        self.client = MagicMock(spec=bigquery.Client)
        self.tables = {name: make_table(name, partition=partition) for name in names}
        self.partition = partition
        self.client.get_table.side_effect = self.get_table
        self.client.query.side_effect = self.query

    def get_table(self, name):
        if name not in self.tables:
            raise NotFound("missing")
        return self.tables[name]

    def query(self, sql, **kwargs):
        config = kwargs["job_config"]
        if config.dry_run:
            return job()
        if sql.startswith("CREATE SNAPSHOT TABLE"):
            snapshot_name = sql.split("`")[1]
            source_name = sql.split("`")[3]
            params = {p.name: p.value for p in config.query_parameters}
            timestamp, expires = params["snapshot_time"], params["expires_at"]
            table = make_table(snapshot_name, partition=self.partition, table_type="SNAPSHOT")
            project, dataset, name = source_name.split(".")
            table._properties["snapshotDefinition"] = {
                "baseTableReference": {"projectId": project, "datasetId": dataset, "tableId": name},
                "snapshotTime": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            }
            table.expires = expires
            self.tables[snapshot_name] = table
            return job()
        return job([{"eligible_rows": 3, "null_rows": 0}])


def final_audit(capsys):
    return json.loads(capsys.readouterr().err.splitlines()[-1])


def test_defaults_multiple_tables_and_repeated_flag():
    args = backup.parse_args(["--tables", SOURCE, OTHER])
    assert len(args.sources) == 2
    assert args.retention_days == 7 and args.partition_column == "partition_dt"
    args = backup.parse_args(["--table", SOURCE, "--table", OTHER, "--retention-days", "14", "--partition-column", "edh_partition_dt"])
    assert len(args.sources) == 2
    assert args.retention_days == 14 and args.partition_column == "edh_partition_dt"


@pytest.mark.parametrize("args", [[], ["--tables"], ["--tables", SOURCE, SOURCE],
                                  ["--tables", SOURCE, "--retention-days", "0"],
                                  ["--tables", SOURCE, "--retention-days", "-1"],
                                  ["--tables", SOURCE, "--retention-days", "1.5"],
                                  ["--tables", SOURCE, "--retention-days", "36501"],
                                  ["--tables", "sample-project.hr.x;DROP TABLE t"],
                                  ["--tables", "sample-project.hr.x@100"]])
def test_invalid_arguments(args):
    with pytest.raises(backup.ValidationError):
        backup.parse_args(args)


def test_naming_same_dataset_utc_and_max_length():
    source = backup.parse_table_reference(SOURCE)
    snapshot = backup.snapshot_reference(source, STAMP)
    assert str(snapshot) == "sample-project.hr.employee_snapshot_20261026_163000_vw"
    local = STAMP.astimezone(timezone(timedelta(hours=5, minutes=30)))
    assert backup.snapshot_reference(source, local) == snapshot
    with pytest.raises(backup.ValidationError):
        backup.snapshot_reference(source, STAMP.replace(tzinfo=None))
    with pytest.raises(backup.ValidationError):
        backup.snapshot_reference(backup.TableId("sample-project", "hr", "a" * 1024), STAMP)


def test_native_snapshot_sql_has_bound_retention_and_time():
    source = backup.parse_table_reference(SOURCE)
    sql = backup.build_snapshot_sql(source, backup.snapshot_reference(source, STAMP))
    assert sql.startswith("CREATE SNAPSHOT TABLE")
    assert f"CLONE `{SOURCE}`" in sql
    assert "FOR SYSTEM_TIME AS OF @snapshot_time" in sql
    assert "expiration_timestamp = @expires_at" in sql
    assert "SELECT *" not in sql and "OR REPLACE" not in sql and "IF NOT EXISTS" not in sql
    with pytest.raises(backup.ValidationError, match="same dataset"):
        backup.build_snapshot_sql(source, backup.TableId("sample-project", "other", "snap"))


@pytest.mark.parametrize("kind,mode,name", [("TIMESTAMP", "REQUIRED", "partition_dt"),
                                          ("DATE", "NULLABLE", "partition_dt"),
                                          ("DATE", "REPEATED", "partition_dt"),
                                          ("DATE", "REQUIRED", "other")])
def test_partition_metadata_invalid(kind, mode, name):
    with pytest.raises(backup.ValidationError):
        backup.validate_partition_column(make_table(kind=kind, mode=mode), name)


def test_missing_physical_partition():
    table = make_table()
    table.time_partitioning = None
    with pytest.raises(backup.ValidationError, match="physically"):
        backup.validate_partition_column(table, "partition_dt")


def test_read_counts_use_historical_time_and_partition_filters():
    client = MagicMock()
    client.query.return_value = job([{"eligible_rows": 3, "null_rows": 0}])
    assert backup.get_row_counts(client, backup.parse_table_reference(SOURCE), "partition_dt", "US", "abc", STAMP) == (3, 0)
    sql = client.query.call_args.args[0]
    assert sql.count("FOR SYSTEM_TIME AS OF @snapshot_time") == 2
    assert "WHERE `partition_dt` IS NOT NULL" in sql and "WHERE `partition_dt` IS NULL" in sql
    assert client.query.call_args.kwargs["job_config"].query_parameters[0].value == STAMP


def test_backup_multiple_tables_with_retention_audit_and_no_overwrite(monkeypatch, capsys, tmp_path):
    service = FakeService()
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    path = tmp_path / "audit.jsonl"
    assert backup.main(["--tables", SOURCE, OTHER, "--retention-days", "14", "--audit-log", str(path)]) == 0
    audit = final_audit(capsys)
    assert audit["status"] == "SUCCESS"
    assert len(audit["tables"]) == 2
    assert all(table["snapshot_row_count"] == 3 and table["created"] for table in audit["tables"])
    assert datetime.fromisoformat(audit["expires_at"]) - datetime.fromisoformat(audit["snapshot_time"]) == timedelta(days=14)
    calls = service.client.query.call_args_list
    writes = [call for call in calls if call.args[0].startswith("CREATE SNAPSHOT") and not call.kwargs["job_config"].dry_run]
    assert len(writes) == 2
    for call in writes:
        assert call.kwargs["job_retry"] is None
        assert call.kwargs["job_id"].startswith(f"backup_{audit['execution_id']}_")
    assert json.loads(path.read_text().splitlines()[-1]) == audit
    service.client.close.assert_called_once()


def test_dry_run_has_no_mutation_and_uses_default_seven_days(monkeypatch, capsys):
    service = FakeService()
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE, OTHER, "--dry-run"]) == 0
    audit = final_audit(capsys)
    assert audit["status"] == "DRY_RUN_PASSED" and audit["retention_days"] == 7
    assert len(service.tables) == 2
    assert all(not call.args[0].startswith("CREATE SNAPSHOT") or call.kwargs["job_config"].dry_run
               for call in service.client.query.call_args_list)


def test_custom_partition_end_to_end(monkeypatch, capsys):
    service = FakeService(partition="EDH_Partition_DT")
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE, "--partition-column", "edh_partition_dt"]) == 0
    assert final_audit(capsys)["tables"][0]["partition_column"] == "EDH_Partition_DT"


def test_all_sources_preflight_before_any_creation(monkeypatch, capsys):
    service = FakeService(names=(SOURCE,))
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE, OTHER]) == 1
    assert len(service.tables) == 1
    assert all(call.kwargs["job_config"].dry_run or call.args[0].startswith("SELECT")
               for call in service.client.query.call_args_list)
    assert "does not exist" in final_audit(capsys)["error_message"]


@pytest.mark.parametrize("table_type", ["VIEW", "EXTERNAL", "SNAPSHOT"])
def test_non_native_sources_rejected(monkeypatch, capsys, table_type):
    service = FakeService()
    service.tables[SOURCE]._properties["type"] = table_type
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE]) == 1
    service.client.query.assert_not_called()


def test_streaming_buffer_is_rejected(monkeypatch, capsys):
    service = FakeService()
    service.tables[SOURCE]._properties["streamingBuffer"] = {"estimatedRows": "3", "estimatedBytes": "30"}
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE]) == 1
    assert "streaming buffer" in final_audit(capsys)["error_message"]
    service.client.query.assert_not_called()


def test_existing_snapshot_is_not_overwritten(monkeypatch, capsys):
    service = FakeService()
    service.client.get_table.side_effect = lambda _: make_table()
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE]) == 1
    assert "already exists" in final_audit(capsys)["error_message"]
    service.client.query.assert_not_called()


def test_null_partition_data_aborts(monkeypatch, capsys):
    service = FakeService()
    service.client.query.side_effect = lambda *_, **__: job([{"eligible_rows": 3, "null_rows": 1}])
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE]) == 1
    assert "NULL partition" in final_audit(capsys)["error_message"]
    assert len(service.tables) == 2


def test_wrong_location_aborts(monkeypatch, capsys):
    service = FakeService()
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE, "--location", "EU"]) == 1
    service.client.query.assert_not_called()


def test_partial_creation_is_logged_and_never_deleted(monkeypatch, capsys):
    service = FakeService()
    original = service.query

    def query(sql, **kwargs):
        if "CLONE `" + OTHER + "`" in sql and not kwargs["job_config"].dry_run:
            return job(error=Conflict("snapshot collision"))
        return original(sql, **kwargs)

    service.client.query.side_effect = query
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE, OTHER]) == 2
    audit = final_audit(capsys)
    assert audit["partial_success"] is True
    assert audit["tables"][0]["status"] == "SUCCESS"
    assert audit["tables"][1]["status"] == "FAILED"
    assert len(service.tables) == 3
    assert all("DROP " not in call.args[0] and "DELETE " not in call.args[0] for call in service.client.query.call_args_list)


def test_cli_validation_does_not_initialize_credentials(monkeypatch, capsys):
    factory = MagicMock()
    monkeypatch.setattr(backup, "get_bigquery_client", factory)
    assert backup.main([]) == 1
    factory.assert_not_called()
    assert final_audit(capsys)["status"] == "VALIDATION_FAILED"


def test_network_error_reports_unknown_creation_outcome(monkeypatch, capsys):
    service = FakeService()
    original = service.query

    def query(sql, **kwargs):
        if sql.startswith("CREATE SNAPSHOT TABLE") and not kwargs["job_config"].dry_run:
            # Model a completed server request whose response was lost.
            original(sql, **kwargs)
            raise ConnectionError("response lost")
        return original(sql, **kwargs)

    service.client.query.side_effect = query
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE]) == 2
    audit = final_audit(capsys)
    assert audit["tables"][0]["created"] is None
    assert audit["tables"][0]["job_id"]
    assert len(service.tables) == 3


def test_snapshot_row_mismatch_is_validation_failure_without_cleanup(monkeypatch, capsys):
    service = FakeService()
    original = service.query

    def query(sql, **kwargs):
        if sql.startswith("SELECT") and "_snapshot_" in sql:
            return job([{"eligible_rows": 2, "null_rows": 0}])
        return original(sql, **kwargs)

    service.client.query.side_effect = query
    monkeypatch.setattr(backup, "get_bigquery_client", lambda _: service.client)
    assert backup.main(["--tables", SOURCE]) == 1
    audit = final_audit(capsys)
    assert audit["tables"][0]["created"] is True
    assert "row/partition validation" in audit["error_message"]

