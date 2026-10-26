"""Contract tests using real BigQuery metadata objects and mocked network calls."""

from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud import bigquery

import restore


SOURCE = "sample-project.hr.employee_snapshot_20261026_163000_vw"
TARGET = "sample-project.hr.employee_new"
CLI = ["--source-snapshot", SOURCE, "--target-table", TARGET]
DAY = date(2026, 10, 25)


def field(name, kind="STRING", mode="NULLABLE", **kwargs):
    return bigquery.SchemaField(name, kind, mode=mode, **kwargs)


def make_table(table_id=TARGET, schema=None, kind="TABLE", partition="partition_dt", location="US"):
    table = bigquery.Table(table_id, schema=schema if schema is not None else [
        field("employee_id", "INT64"), field(partition, "DATE", "REQUIRED")])
    table._properties.update(type=kind, location=location)
    if partition:
        table.time_partitioning = bigquery.TimePartitioning(field=partition)
    table.require_partition_filter = True
    if kind == "SNAPSHOT":
        table._properties["snapshotDefinition"] = {
            "baseTableReference": {"projectId": "sample-project", "datasetId": "hr", "tableId": "employee"},
            "snapshotTime": "2026-10-26T16:30:00.000000Z",
        }
    return table


def job(rows=(), error=None):
    result = MagicMock()
    if error:
        result.result.side_effect = error
    else:
        result.result.return_value = list(rows)
    return result


def stats(eligible=2, nulls=0):
    return [{"eligible_rows": eligible, "null_rows": nulls}]


def counts(rows=2, day=DAY):
    return [{"partition_value": day, "row_count": rows}] if rows else []


def client_for_success(source=None, target=None, dry_run=False):
    client = MagicMock(spec=bigquery.Client)
    client.get_table.side_effect = [source or make_table(SOURCE, kind="SNAPSHOT"), target or make_table()]
    responses = [job(stats()), job(stats(0)), job(counts()), job()]
    if not dry_run:
        responses += [job([{"rows_inserted": 2, "target_final_rows": 2}]),
                      job(stats()), job(stats()), job(counts())]
    client.query.side_effect = responses
    return client


def final_audit(capsys):
    return json.loads(capsys.readouterr().err.splitlines()[-1])


@pytest.mark.parametrize("value", ["project.dataset.table", "sample-project.hr.employee", "abc123._dataset.123table"])
def test_table_id(value):
    parsed = restore.parse_table_reference(value)
    assert str(parsed) == value
    assert parsed.sql == f"`{value}`"


@pytest.mark.parametrize("value", ["dataset.table", "a.b.c", "p.d.t.extra", "sample-project.hr.x;DROP TABLE t",
                                  "sample-project.hr.`x`", "sample-project.hr.x$20261025",
                                  "sample-project.hr.x@123", "sample-project.hr.*", " sample-project.hr.x",
                                  "sample-project.hr.x\n", "sample-project:hr.x"])
def test_invalid_table_ids(value):
    with pytest.raises(restore.ValidationError):
        restore.parse_table_reference(value)


def test_cli_defaults_overrides_and_missing_arguments():
    args = restore.parse_args(CLI)
    assert args.source_partition_column == args.target_partition_column == "partition_dt"
    args = restore.parse_args(CLI + ["--partition-column", "edh_partition_dt"])
    assert args.source_partition_column == args.target_partition_column == "edh_partition_dt"
    args = restore.parse_args(CLI + ["--partition-column", "p", "--source-partition-column", "sp",
                                     "--target-partition-column", "tp"])
    assert (args.source_partition_column, args.target_partition_column) == ("sp", "tp")
    with pytest.raises(restore.ValidationError):
        restore.parse_args([])
    with pytest.raises(restore.ValidationError):
        restore.parse_args(CLI + ["--partition-date", "2026-10-25"])
    with pytest.raises(restore.ValidationError):
        restore.parse_args(["--source-snapshot", TARGET, "--target-table", TARGET])


@pytest.mark.parametrize("kind", ["TABLE", "VIEW", "EXTERNAL", "MATERIALIZED_VIEW"])
def test_source_must_be_snapshot(kind):
    with pytest.raises(restore.ValidationError, match="SNAPSHOT"):
        restore.validate_source_snapshot(make_table(SOURCE, kind=kind))


def test_valid_native_source_and_target():
    restore.validate_source_snapshot(make_table(SOURCE, kind="SNAPSHOT"))
    restore.validate_target_table(make_table())


def test_missing_snapshot_definition_rejected():
    table = make_table(SOURCE, kind="SNAPSHOT")
    table._properties.pop("snapshotDefinition")
    with pytest.raises(restore.ValidationError, match="native"):
        restore.validate_source_snapshot(table)


@pytest.mark.parametrize("kind", ["VIEW", "EXTERNAL", "SNAPSHOT", "MATERIALIZED_VIEW"])
def test_invalid_target_type(kind):
    with pytest.raises(restore.ValidationError, match="TABLE"):
        restore.validate_target_table(make_table(kind=kind))


def test_empty_schema_rejected():
    with pytest.raises(restore.SchemaValidationError):
        restore.validate_source_snapshot(make_table(SOURCE, schema=[], kind="SNAPSHOT"))
    with pytest.raises(restore.SchemaValidationError):
        restore.validate_target_table(make_table(schema=[]))


@pytest.mark.parametrize("schema,message", [
    ([field("id", "INT64")], "Missing partition"),
    ([field("partition_dt", "TIMESTAMP", "REQUIRED")], "DATE REQUIRED"),
    ([field("partition_dt", "DATE")], "DATE REQUIRED"),
    ([field("partition_dt", "DATE", "REPEATED")], "DATE REQUIRED"),
])
def test_partition_metadata_failures(schema, message):
    with pytest.raises(restore.ValidationError, match=message):
        restore.validate_partition_column(make_table(schema=schema), "partition_dt")


def test_actual_partition_must_match():
    table = make_table()
    table.time_partitioning = None
    with pytest.raises(restore.ValidationError, match="physically"):
        restore.validate_partition_column(table, "partition_dt")
    table.time_partitioning = bigquery.TimePartitioning()  # ingestion-time partitioning
    with pytest.raises(restore.ValidationError, match="physically"):
        restore.validate_partition_column(table, "partition_dt")


def test_partition_case_is_resolved_from_metadata():
    table = make_table(partition="EDH_Partition_DT")
    assert restore.validate_partition_column(table, "edh_partition_dt").name == "EDH_Partition_DT"


def test_null_partition_validation():
    restore.validate_partition_nulls(restore.RowStatistics(10, 0), "Source")
    with pytest.raises(restore.ValidationError, match="RESTORE_VALIDATION_FAILED"):
        restore.validate_partition_nulls(restore.RowStatistics(10, 1), "Source")


def comparison(source, target):
    return restore.compare_schemas(source, target, "partition_dt", "partition_dt")


def test_schema_evolution_and_target_order():
    source = [field("id", "INT64"), field("removed"), field("partition_dt", "DATE", "REQUIRED")]
    target = [field("department"), field("partition_dt", "DATE", "REQUIRED"), field("id", "INT64")]
    result = comparison(source, target)
    assert [b.name for _, b in result.common] == ["partition_dt", "id"]
    assert [f.name for f in result.target_only] == ["department"]
    assert [f.name for f in result.source_only] == ["removed"]
    restore.validate_common_columns(result)
    restore.validate_new_target_columns(result)
    sql = restore.build_restore_sql(restore.parse_table_reference(SOURCE), restore.parse_table_reference(TARGET),
                                    target, result, "partition_dt")
    assert "NULL AS `department`" in sql
    assert "`removed`" not in sql
    assert "SELECT *" not in sql.upper()
    assert sql.endswith("WHERE s.`partition_dt` IS NOT NULL")
    assert sql.index("`department`") < sql.index("`partition_dt`") < sql.index("`id`")
    assert "CAST(" not in sql.upper()


@pytest.mark.parametrize("mode", ["REQUIRED", "REPEATED"])
def test_new_non_nullable_column_fails(mode):
    result = comparison([field("id")], [field("id"), field("new_column", mode=mode)])
    with pytest.raises(restore.SchemaValidationError, match=mode):
        restore.validate_new_target_columns(result)


@pytest.mark.parametrize("source,target,detail", [
    (field("x", "INT64"), field("x", "NUMERIC"), "type"),
    (field("x", mode="REQUIRED"), field("x"), "mode"),
    (field("x", "NUMERIC", precision=10, scale=2), field("x", "NUMERIC", precision=12, scale=2), "precision"),
    (field("x", "NUMERIC", precision=10, scale=2), field("x", "NUMERIC", precision=10, scale=3), "scale"),
    (field("x", max_length=5), field("x", max_length=10), "maxLength"),
    (field("x", "RECORD", fields=[field("a")]), field("x", "RECORD", fields=[field("a", "INT64")]), "x.a"),
    (field("x", "RECORD", fields=[field("a")]), field("x", "RECORD", fields=[field("b")]), "order/name"),
    (field("x", "RECORD", fields=[field("a")]), field("x", "RECORD", fields=[field("a"), field("b")]), "field count"),
    (field("x", "RANGE", range_element_type="DATE"), field("x", "RANGE", range_element_type="DATETIME"), "rangeElementType"),
    (field("x", "NUMERIC", rounding_mode="ROUND_HALF_EVEN"), field("x", "NUMERIC", rounding_mode="ROUND_HALF_AWAY_FROM_ZERO"), "roundingMode"),
])
def test_schema_mismatches(source, target, detail):
    result = comparison([source], [target])
    assert len(result.mismatches) == 1
    with pytest.raises(restore.SchemaValidationError, match=detail):
        restore.validate_common_columns(result)


def test_collation_compared():
    source = bigquery.SchemaField.from_api_repr({"name": "x", "type": "STRING", "collation": "und:ci"})
    with pytest.raises(restore.SchemaValidationError, match="collation"):
        restore.validate_common_columns(comparison([source], [field("x")]))


def test_nested_required_and_repeated_compatible_and_aliases():
    source = field("data", "RECORD", mode="REPEATED", fields=[field("a", "INTEGER", "REQUIRED")])
    target = field("DATA", "STRUCT", mode="REPEATED", fields=[field("A", "INT64", "REQUIRED")])
    restore.validate_common_columns(comparison([source], [target]))


def test_partition_rename():
    source = [field("id"), field("partition_dt", "DATE", "REQUIRED")]
    target = [field("id"), field("edh_partition_dt", "DATE", "REQUIRED")]
    result = restore.compare_schemas(source, target, "partition_dt", "edh_partition_dt")
    assert len(result.common) == 2
    assert not result.target_only and not result.source_only
    sql = restore.build_restore_sql(restore.parse_table_reference(SOURCE), restore.parse_table_reference(TARGET),
                                    target, result, "partition_dt")
    assert "s.`partition_dt` AS `edh_partition_dt`" in sql
    assert "WHERE s.`partition_dt` IS NOT NULL" in sql


@pytest.mark.parametrize("source,target", [
    ([field("sp"), field("tp")], [field("tp")]),
    ([field("sp")], [field("sp"), field("tp")]),
])
def test_ambiguous_partition_rename_rejected(source, target):
    with pytest.raises(restore.SchemaValidationError, match="conflicts"):
        restore.compare_schemas(source, target, "sp", "tp")


def test_flexible_columns_quoted_and_cli_cannot_inject():
    source = [field("full name"), field("partition_dt", "DATE", "REQUIRED")]
    result = comparison(source, source)
    sql = restore.build_restore_sql(restore.parse_table_reference(SOURCE), restore.parse_table_reference(TARGET), source, result, "partition_dt")
    assert "s.`full name` AS `full name`" in sql
    assert restore.quote_column("a`b\\c") == "`a\\`b\\\\c`"
    with pytest.raises(restore.ValidationError):
        restore.validate_partition_column(make_table(), "partition_dt` IS NULL; DELETE")


def test_row_queries_always_have_partition_filters():
    client = MagicMock()
    client.query.return_value = job(stats(5, 2))
    result = restore.get_row_statistics(client, restore.parse_table_reference(SOURCE), "partition_dt", "US", "abc")
    sql = client.query.call_args.args[0]
    assert "WHERE `partition_dt` IS NULL" in sql
    assert "WHERE `partition_dt` IS NOT NULL" in sql
    assert result.total_rows == 7


def test_successful_restore_queries_transaction_and_audit(monkeypatch, capsys, tmp_path):
    client = client_for_success()
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    audit_path = tmp_path / "audit.jsonl"
    assert restore.main(CLI + ["--audit-log", str(audit_path)]) == 0
    output = capsys.readouterr()
    assert "STATUS: SUCCESS" in output.out
    audit = json.loads(output.err.splitlines()[-1])
    assert audit["committed"] is True
    assert audit["rows_inserted"] == audit["source_row_count"] == audit["target_final_row_count"] == 2
    assert audit["status"] == "SUCCESS"
    assert json.loads(audit_path.read_text().splitlines()[-1]) == audit
    sql = client.query.call_args_list[4].args[0]
    assert "BEGIN TRANSACTION" in sql and "COMMIT TRANSACTION" in sql
    assert sql.index("Target table is not empty") < sql.index("INSERT INTO")
    assert sql.index("Partition counts mismatch") < sql.index("COMMIT TRANSACTION")
    assert "FULL OUTER JOIN" in sql
    assert "SET inserted_rows = @@row_count;" in sql
    assert not any(word in sql.upper() for word in ["SELECT *", "TRUNCATE", "DROP TABLE", "CREATE TABLE", "DELETE FROM"])
    options = client.query.call_args_list[4].kwargs
    assert options["job_retry"] is None
    assert options["job_config"].query_parameters[0].value == 2
    assert options["job_id"] == audit["job_id"]
    client.close.assert_called_once()


def test_dry_run_runs_read_validations_but_does_not_insert(monkeypatch, capsys):
    client = client_for_success(dry_run=True)
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI + ["--dry-run"]) == 0
    assert client.query.call_count == 4
    for call in client.query.call_args_list:
        if "INSERT INTO" in call.args[0]:
            assert call.kwargs["job_config"].dry_run is True
        else:
            assert call.args[0].startswith("SELECT")
    output = capsys.readouterr()
    assert "NOT EXECUTED (DRY RUN)" in output.out
    assert "Generated INSERT SQL" in output.out
    assert json.loads(output.err.splitlines()[-1])["status"] == "DRY_RUN_PASSED"


def test_custom_partition_end_to_end(monkeypatch, capsys):
    source = make_table(SOURCE, kind="SNAPSHOT", partition="source_date")
    target = make_table(partition="target_date")
    client = client_for_success(source, target)
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI + ["--source-partition-column", "source_date", "--target-partition-column", "target_date"]) == 0
    sql = client.query.call_args_list[4].args[0]
    assert "s.`source_date` AS `target_date`" in sql
    assert "WHERE s.`source_date` IS NOT NULL" in sql


@pytest.mark.parametrize("source_nulls,target_rows", [(1, 0), (0, 1)])
def test_data_validation_aborts_before_insert(monkeypatch, capsys, source_nulls, target_rows):
    client = client_for_success()
    client.query.side_effect = [job(stats(2, source_nulls)), job(stats(target_rows))]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    assert all("INSERT INTO" not in call.args[0] for call in client.query.call_args_list)
    assert final_audit(capsys)["status"] == "VALIDATION_FAILED"


def test_target_with_only_null_partition_data_is_not_empty(monkeypatch, capsys):
    client = client_for_success()
    client.query.side_effect = [job(stats()), job(stats(0, 1))]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    assert "not empty" in final_audit(capsys)["error_message"]


@pytest.mark.parametrize("missing", ["source", "target"])
def test_missing_tables_abort(monkeypatch, capsys, missing):
    client = client_for_success()
    client.get_table.side_effect = [NotFound("missing")] if missing == "source" else [make_table(SOURCE, kind="SNAPSHOT"), NotFound("missing")]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    client.query.assert_not_called()
    assert "does not exist" in final_audit(capsys)["error_message"]


@pytest.mark.parametrize("target_location,override", [("EU", None), ("US", "EU")])
def test_wrong_locations_abort(monkeypatch, capsys, target_location, override):
    client = client_for_success(target=make_table(location=target_location))
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI + (["--location", override] if override else [])) == 1
    client.query.assert_not_called()


def test_schema_error_aborts_before_queries_and_records_mismatches(monkeypatch, capsys):
    client = client_for_success(target=make_table(schema=[field("employee_id", "NUMERIC"), field("partition_dt", "DATE", "REQUIRED")]))
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    client.query.assert_not_called()
    assert final_audit(capsys)["datatype_mismatch_count"] == 1


def test_source_partition_total_inconsistency_fails_before_write(monkeypatch, capsys):
    client = client_for_success()
    client.query.side_effect = [job(stats()), job(stats(0)), job(counts(1))]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    assert "do not sum" in final_audit(capsys)["error_message"]
    assert client.query.call_count == 3


@pytest.mark.parametrize("error", ["Target table is not empty", "Row count mismatch", "Partition counts mismatch"])
def test_transaction_assertion_failure_is_validation_error(monkeypatch, capsys, error):
    client = client_for_success()
    client.query.side_effect = [job(stats()), job(stats(0)), job(counts()), job(),
                               job(error=BadRequest(f"RESTORE_VALIDATION_FAILED: {error}"))]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    audit = final_audit(capsys)
    assert audit["transaction_status"] == "ROLLED_BACK" and audit["committed"] is False
    assert client.query.call_count == 5  # no retry or cleanup mutations


def test_post_commit_mismatch_reports_committed_data(monkeypatch, capsys):
    client = client_for_success()
    client.query.side_effect = [job(stats()), job(stats(0)), job(counts()), job(),
                               job([{"rows_inserted": 2, "target_final_rows": 2}]),
                               job(stats()), job(stats(3)), job(counts(3))]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 1
    audit = final_audit(capsys)
    assert audit["committed"] is True
    assert audit["target_final_row_count"] == 3


@pytest.mark.parametrize("rows,expected_counts,actual_counts,inserted", [
    (3, {DAY: 2}, {DAY: 3}, 2),
    (2, {DAY: 2}, {date(2026, 10, 24): 2}, 2),
    (2, {DAY: 2}, {DAY: 2}, 1),
])
def test_post_restore_validations(rows, expected_counts, actual_counts, inserted):
    with pytest.raises(restore.ValidationError, match="RESTORE_VALIDATION_FAILED"):
        restore.validate_restore(2, rows, expected_counts, actual_counts, inserted)


def test_empty_snapshot_restore_supported():
    restore.validate_restore(0, 0, {}, {}, 0)


def test_execution_errors_and_credentials(monkeypatch, capsys):
    client = client_for_success()
    client.query.side_effect = Forbidden("no permission")
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 2
    assert final_audit(capsys)["status"] == "EXECUTION_FAILED"
    monkeypatch.setattr(restore, "get_bigquery_client", MagicMock(side_effect=RuntimeError("no ADC")))
    assert restore.main(CLI) == 2
    assert "no ADC" in final_audit(capsys)["error_message"]


def test_cli_errors_exit_one_without_client(monkeypatch, capsys):
    factory = MagicMock()
    monkeypatch.setattr(restore, "get_bigquery_client", factory)
    assert restore.main([]) == 1
    factory.assert_not_called()
    assert final_audit(capsys)["status"] == "VALIDATION_FAILED"


def test_audit_path_failure_aborts_before_client(monkeypatch, capsys, tmp_path):
    factory = MagicMock()
    monkeypatch.setattr(restore, "get_bigquery_client", factory)
    assert restore.main(CLI + ["--audit-log", str(tmp_path / "missing" / "audit.jsonl")]) == 2
    factory.assert_not_called()


def test_network_error_reports_unknown_commit_outcome(monkeypatch, capsys):
    client = client_for_success()
    client.query.side_effect = [job(stats()), job(stats(0)), job(counts()), job(),
                               job(error=ConnectionError("response lost"))]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 2
    audit = final_audit(capsys)
    assert audit["committed"] is None
    assert audit["transaction_status"] == "UNKNOWN"
    assert audit["job_id"]
    assert client.query.call_count == 5


def test_empty_snapshot_full_workflow(monkeypatch, capsys):
    client = client_for_success()
    client.query.side_effect = [job(stats(0)), job(stats(0)), job([]), job(),
                               job([{"rows_inserted": 0, "target_final_rows": 0}]),
                               job(stats(0)), job(stats(0)), job([])]
    monkeypatch.setattr(restore, "get_bigquery_client", lambda _: client)
    assert restore.main(CLI) == 0
    assert final_audit(capsys)["rows_inserted"] == 0

