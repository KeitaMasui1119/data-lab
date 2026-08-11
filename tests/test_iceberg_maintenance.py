"""Unit tests for expire_old_snapshots(), find_orphan_data_files(), and
delete_orphan_data_files() (src/common/iceberg_maintenance.py).

expire_old_snapshots() is tested against a local SQLite catalog (real
PyIceberg snapshot semantics matter here: branch-head protection).
find_orphan_data_files() and delete_orphan_data_files() are tested against
lightweight fakes, since a local warehouse produces file:// paths while the
function compares against s3://-style keys -- the set-difference logic is
what's under test, not real object storage.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import DateType, NestedField

from common.iceberg_maintenance import (
    delete_orphan_data_files,
    expire_old_snapshots,
    find_orphan_data_files,
)
from common.storage_client import RustFSClient


def _local_table(tmp_path: Path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "test",
        **{
            "uri": f"sqlite:///{tmp_path / 'catalog.db'}",
            "warehouse": f"file://{warehouse}",
        },
    )
    catalog.create_namespace_if_not_exists("silver")
    schema = Schema(NestedField(1, "d", DateType(), required=True))
    table = catalog.create_table("silver.x", schema=schema)
    return catalog, table


def _append_row(table, day: int):
    arrow_table = pa.Table.from_pylist(
        [{"d": date(2026, 1, day)}], schema=table.schema().as_arrow()
    )
    table.append(arrow_table)


def test_expire_old_snapshots_keeps_branch_head_even_when_older_than_cutoff(
    tmp_path: Path,
) -> None:
    catalog, table = _local_table(tmp_path)
    for day in range(1, 6):
        _append_row(table, day)

    table = catalog.load_table("silver.x")
    current_id = table.metadata.current_snapshot_id
    assert len(list(table.snapshots())) == 5

    cutoff = datetime.now(UTC) + timedelta(seconds=1)
    expired = expire_old_snapshots(cast(Catalog, catalog), "silver.x", cutoff)

    table = catalog.load_table("silver.x")
    remaining = list(table.snapshots())
    assert len(remaining) == 1
    assert remaining[0].snapshot_id == current_id
    assert current_id not in expired
    assert len(expired) == 4


def test_expire_old_snapshots_keeps_everything_when_cutoff_predates_all_snapshots(
    tmp_path: Path,
) -> None:
    catalog, table = _local_table(tmp_path)
    cutoff = datetime.now(UTC) - timedelta(days=365)

    for day in range(1, 4):
        _append_row(table, day)

    expired = expire_old_snapshots(cast(Catalog, catalog), "silver.x", cutoff)

    table = catalog.load_table("silver.x")
    assert expired == []
    assert len(list(table.snapshots())) == 3


class _FakeArrowResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def to_pylist(self) -> list[dict[str, Any]]:
        return self._rows


class _FakeSnapshot:
    def __init__(self, snapshot_id: int) -> None:
        self.snapshot_id = snapshot_id


class _FakeInspect:
    def __init__(self, files_by_snapshot: dict[int, list[str]]) -> None:
        self._files_by_snapshot = files_by_snapshot

    def data_files(self, snapshot_id: int | None = None) -> _FakeArrowResult:
        assert snapshot_id is not None
        paths = self._files_by_snapshot[snapshot_id]
        return _FakeArrowResult([{"file_path": path} for path in paths])


class _FakeTable:
    def __init__(
        self, snapshot_ids: list[int], files_by_snapshot: dict[int, list[str]]
    ) -> None:
        self._snapshot_ids = snapshot_ids
        self.inspect = _FakeInspect(files_by_snapshot)

    def snapshots(self) -> list[_FakeSnapshot]:
        return [_FakeSnapshot(sid) for sid in self._snapshot_ids]


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table

    def load_table(self, identifier: str) -> _FakeTable:
        return self._table


class _FakePaginator:
    def __init__(self, keys: list[str]) -> None:
        self._keys = keys

    def paginate(self, Bucket: str, Prefix: str):
        matching = [key for key in self._keys if key.startswith(Prefix)]
        yield {"Contents": [{"Key": key} for key in matching]}


class _FakeS3:
    def __init__(self, keys: list[str]) -> None:
        self._keys = keys
        self.delete_calls: list[dict[str, Any]] = []

    def get_paginator(self, operation_name: str) -> _FakePaginator:
        assert operation_name == "list_objects_v2"
        return _FakePaginator(self._keys)

    def delete_objects(self, Bucket: str, Delete: dict[str, Any]) -> dict[str, Any]:
        self.delete_calls.append({"Bucket": Bucket, "Delete": Delete})
        return {"Deleted": [{"Key": obj["Key"]} for obj in Delete["Objects"]]}


class _FakeClient:
    def __init__(self, s3: _FakeS3) -> None:
        self.s3 = s3


def test_find_orphan_data_files_unions_references_across_all_surviving_snapshots() -> (
    None
):
    """A file referenced only by a non-current (but not-yet-expired) snapshot
    must not be flagged as orphan -- only the union across every remaining
    snapshot is safe to treat as "still referenced"."""
    files_by_snapshot = {
        1: ["s3://bucket/silver/x/data/file1.parquet"],
        2: ["s3://bucket/silver/x/data/file2.parquet"],
    }
    table = _FakeTable(snapshot_ids=[1, 2], files_by_snapshot=files_by_snapshot)
    catalog = _FakeCatalog(table)
    s3 = _FakeS3(
        keys=[
            "silver/x/data/file1.parquet",
            "silver/x/data/file2.parquet",
            "silver/x/data/file3.parquet",
        ]
    )
    client = _FakeClient(s3)

    orphans = find_orphan_data_files(
        cast(Catalog, catalog), "silver.x", cast(RustFSClient, client), "bucket"
    )

    assert orphans == ["s3://bucket/silver/x/data/file3.parquet"]


def test_find_orphan_data_files_flags_files_left_by_an_expired_snapshot() -> None:
    """The realistic post-expire_old_snapshots() scenario: only the current
    snapshot remains, so a file it doesn't reference is correctly orphaned."""
    files_by_snapshot = {2: ["s3://bucket/silver/x/data/file2.parquet"]}
    table = _FakeTable(snapshot_ids=[2], files_by_snapshot=files_by_snapshot)
    catalog = _FakeCatalog(table)
    s3 = _FakeS3(
        keys=[
            "silver/x/data/file1.parquet",
            "silver/x/data/file2.parquet",
        ]
    )
    client = _FakeClient(s3)

    orphans = find_orphan_data_files(
        cast(Catalog, catalog), "silver.x", cast(RustFSClient, client), "bucket"
    )

    assert orphans == ["s3://bucket/silver/x/data/file1.parquet"]


def test_find_orphan_data_files_returns_empty_when_nothing_orphaned() -> None:
    files_by_snapshot = {1: ["s3://bucket/silver/x/data/file1.parquet"]}
    table = _FakeTable(snapshot_ids=[1], files_by_snapshot=files_by_snapshot)
    s3 = _FakeS3(keys=["silver/x/data/file1.parquet"])
    client = _FakeClient(s3)

    orphans = find_orphan_data_files(
        cast(Catalog, _FakeCatalog(table)),
        "silver.x",
        cast(RustFSClient, client),
        "bucket",
    )

    assert orphans == []


def test_delete_orphan_data_files_deletes_and_strips_the_s3_uri_prefix() -> None:
    s3 = _FakeS3(keys=[])
    client = _FakeClient(s3)

    deleted = delete_orphan_data_files(
        cast(RustFSClient, client),
        "bucket",
        ["s3://bucket/silver/x/data/file1.parquet"],
    )

    assert deleted == 1
    assert len(s3.delete_calls) == 1
    assert s3.delete_calls[0]["Delete"]["Objects"] == [
        {"Key": "silver/x/data/file1.parquet"}
    ]


def test_delete_orphan_data_files_batches_over_the_thousand_key_s3_limit() -> None:
    s3 = _FakeS3(keys=[])
    client = _FakeClient(s3)
    keys = [f"s3://bucket/silver/x/data/file{i}.parquet" for i in range(1500)]

    deleted = delete_orphan_data_files(cast(RustFSClient, client), "bucket", keys)

    assert deleted == 1500
    assert len(s3.delete_calls) == 2
    assert len(s3.delete_calls[0]["Delete"]["Objects"]) == 1000
    assert len(s3.delete_calls[1]["Delete"]["Objects"]) == 500


def test_delete_orphan_data_files_is_a_noop_for_an_empty_list() -> None:
    s3 = _FakeS3(keys=[])
    client = _FakeClient(s3)

    deleted = delete_orphan_data_files(cast(RustFSClient, client), "bucket", [])

    assert deleted == 0
    assert s3.delete_calls == []
