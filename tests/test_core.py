"""Tests for aumai-datacommons core module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from aumai_datacommons.core import (
    DatasetCatalog,
    DatasetNotFoundError,
    DatasetValidator,
    DatasetVersionManager,
    compute_sha256,
)
from aumai_datacommons.models import DatasetFormat, DatasetMetadata, DatasetVersion


# ---------------------------------------------------------------------------
# DatasetMetadata model tests
# ---------------------------------------------------------------------------


class TestDatasetMetadata:
    def test_valid_metadata_creation(self, catalog_metadata: DatasetMetadata) -> None:
        assert catalog_metadata.dataset_id == "ds-001"
        assert catalog_metadata.format == DatasetFormat.jsonl
        assert catalog_metadata.size_bytes == 1024000
        assert catalog_metadata.num_records == 5000

    def test_default_version(self) -> None:
        meta = DatasetMetadata(
            dataset_id="x",
            name="X",
            description="desc",
            format=DatasetFormat.csv,
            size_bytes=0,
            num_records=0,
            license="MIT",
        )
        assert meta.version == "1.0.0"

    def test_negative_size_raises(self) -> None:
        with pytest.raises(Exception):
            DatasetMetadata(
                dataset_id="x",
                name="X",
                description="desc",
                format=DatasetFormat.csv,
                size_bytes=-1,
                num_records=0,
                license="MIT",
            )

    def test_negative_num_records_raises(self) -> None:
        with pytest.raises(Exception):
            DatasetMetadata(
                dataset_id="x",
                name="X",
                description="desc",
                format=DatasetFormat.csv,
                size_bytes=0,
                num_records=-1,
                license="MIT",
            )

    def test_tags_default_empty(self) -> None:
        meta = DatasetMetadata(
            dataset_id="x",
            name="X",
            description="desc",
            format=DatasetFormat.csv,
            size_bytes=0,
            num_records=0,
            license="MIT",
        )
        assert meta.tags == []

    def test_all_formats(self) -> None:
        for fmt in DatasetFormat:
            meta = DatasetMetadata(
                dataset_id=f"ds-{fmt.value}",
                name=f"Dataset {fmt.value}",
                description="test",
                format=fmt,
                size_bytes=100,
                num_records=10,
                license="MIT",
            )
            assert meta.format == fmt


# ---------------------------------------------------------------------------
# DatasetCatalog tests
# ---------------------------------------------------------------------------


class TestDatasetCatalog:
    def test_register_and_get(self, catalog_metadata: DatasetMetadata) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        retrieved = catalog.get("ds-001")
        assert retrieved.dataset_id == "ds-001"
        assert retrieved.name == "Agent Traces Dataset"

    def test_register_overwrites(self, catalog_metadata: DatasetMetadata) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        updated = catalog_metadata.model_copy(update={"name": "Updated Name"})
        catalog.register(updated)
        assert catalog.get("ds-001").name == "Updated Name"

    def test_get_missing_raises(self) -> None:
        catalog = DatasetCatalog()
        with pytest.raises(DatasetNotFoundError):
            catalog.get("nonexistent-id")

    def test_get_missing_error_message(self) -> None:
        catalog = DatasetCatalog()
        with pytest.raises(DatasetNotFoundError, match="nonexistent"):
            catalog.get("nonexistent")

    def test_list_all_empty(self) -> None:
        catalog = DatasetCatalog()
        assert catalog.list_all() == []

    def test_list_all_returns_all(
        self,
        catalog_metadata: DatasetMetadata,
        second_metadata: DatasetMetadata,
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        catalog.register(second_metadata)
        items = catalog.list_all()
        assert len(items) == 2

    def test_list_all_pagination_limit(
        self,
        catalog_metadata: DatasetMetadata,
        second_metadata: DatasetMetadata,
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        catalog.register(second_metadata)
        items = catalog.list_all(limit=1)
        assert len(items) == 1

    def test_list_all_pagination_offset(
        self,
        catalog_metadata: DatasetMetadata,
        second_metadata: DatasetMetadata,
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        catalog.register(second_metadata)
        items = catalog.list_all(offset=1)
        assert len(items) == 1

    def test_list_all_offset_beyond_end(
        self, catalog_metadata: DatasetMetadata
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        items = catalog.list_all(offset=100)
        assert items == []

    def test_search_by_query_matches_name(
        self, catalog_metadata: DatasetMetadata
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        results = catalog.search("agent traces")
        assert len(results) == 1
        assert results[0].dataset_id == "ds-001"

    def test_search_case_insensitive(
        self, catalog_metadata: DatasetMetadata
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        results = catalog.search("AGENT TRACES")
        assert len(results) == 1

    def test_search_by_query_matches_description(
        self, catalog_metadata: DatasetMetadata
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        results = catalog.search("autonomous")
        assert len(results) == 1

    def test_search_no_match(self, catalog_metadata: DatasetMetadata) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        results = catalog.search("nonexistent-query-xyz")
        assert results == []

    def test_search_filter_by_format(
        self,
        catalog_metadata: DatasetMetadata,
        second_metadata: DatasetMetadata,
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        catalog.register(second_metadata)
        # Both match empty query "" but only one is jsonl
        results = catalog.search("", dataset_format=DatasetFormat.jsonl)
        assert all(r.format == DatasetFormat.jsonl for r in results)

    def test_search_filter_by_tags(
        self,
        catalog_metadata: DatasetMetadata,
        second_metadata: DatasetMetadata,
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        catalog.register(second_metadata)
        results = catalog.search("", tags=["agents"])
        assert len(results) == 1
        assert results[0].dataset_id == "ds-001"

    def test_search_filter_all_tags_must_match(
        self, catalog_metadata: DatasetMetadata
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        # ds-001 has ["agents", "traces", "benchmarking"] — matches both
        results = catalog.search("", tags=["agents", "traces"])
        assert len(results) == 1
        # ds-001 does NOT have "regression" tag
        results2 = catalog.search("", tags=["agents", "regression"])
        assert results2 == []

    def test_search_format_and_tags_combined(
        self,
        catalog_metadata: DatasetMetadata,
        second_metadata: DatasetMetadata,
    ) -> None:
        catalog = DatasetCatalog()
        catalog.register(catalog_metadata)
        catalog.register(second_metadata)
        results = catalog.search("", dataset_format=DatasetFormat.csv, tags=["tabular"])
        assert len(results) == 1
        assert results[0].dataset_id == "ds-002"


# ---------------------------------------------------------------------------
# DatasetValidator tests
# ---------------------------------------------------------------------------


class TestDatasetValidator:
    def test_validate_schema_valid_file(self, jsonl_file: Path) -> None:
        validator = DatasetValidator()
        schema = {"trace_id": "str", "action": "str", "reward": "float"}
        errors = validator.validate_schema(str(jsonl_file), schema)
        assert errors == []

    def test_validate_schema_missing_field(self, tmp_path: Path) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"trace_id": "t-1"}\n', encoding="utf-8")
        validator = DatasetValidator()
        schema = {"trace_id": "str", "action": "str"}
        errors = validator.validate_schema(str(path), schema)
        assert any("missing required field" in e and "action" in e for e in errors)

    def test_validate_schema_wrong_type(self, tmp_path: Path) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"trace_id": 123, "action": "move"}\n', encoding="utf-8")
        validator = DatasetValidator()
        schema = {"trace_id": "str", "action": "str"}
        errors = validator.validate_schema(str(path), schema)
        assert any("trace_id" in e and "str" in e for e in errors)

    def test_validate_schema_file_not_found(self, tmp_path: Path) -> None:
        validator = DatasetValidator()
        nonexistent = str(tmp_path / "does_not_exist.jsonl")
        errors = validator.validate_schema(nonexistent, {"x": "str"})
        assert len(errors) == 1
        assert "File not found" in errors[0]

    def test_validate_schema_invalid_json_line(self, tmp_path: Path) -> None:
        path = tmp_path / "bad.jsonl"
        path.write_text("not json at all\n", encoding="utf-8")
        validator = DatasetValidator()
        errors = validator.validate_schema(str(path), {"x": "str"})
        assert any("JSON decode error" in e for e in errors)

    def test_validate_schema_non_dict_record(self, tmp_path: Path) -> None:
        path = tmp_path / "array.jsonl"
        path.write_text("[1,2,3]\n", encoding="utf-8")
        validator = DatasetValidator()
        errors = validator.validate_schema(str(path), {"x": "str"})
        assert any("not a JSON object" in e for e in errors)

    def test_validate_schema_empty_lines_skipped(self, tmp_path: Path) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"x": "hello"}\n\n{"x": "world"}\n', encoding="utf-8")
        validator = DatasetValidator()
        errors = validator.validate_schema(str(path), {"x": "str"})
        assert errors == []

    def test_validate_schema_unknown_type_name_ignored(self, tmp_path: Path) -> None:
        path = tmp_path / "data.jsonl"
        path.write_text('{"x": "hello"}\n', encoding="utf-8")
        validator = DatasetValidator()
        # "custom_type" is not in the type_map — should be silently skipped
        errors = validator.validate_schema(str(path), {"x": "custom_type"})
        assert errors == []

    def test_compute_statistics_jsonl(self, jsonl_file: Path) -> None:
        validator = DatasetValidator()
        stats = validator.compute_statistics(str(jsonl_file))
        assert stats["row_count"] == 3
        assert "null_counts" in stats
        assert "type_distribution" in stats

    def test_compute_statistics_csv(self, csv_file: Path) -> None:
        validator = DatasetValidator()
        stats = validator.compute_statistics(str(csv_file))
        assert stats["row_count"] == 3
        # Empty string cell should be counted as null
        assert stats["null_counts"].get("value", 0) == 1  # type: ignore[union-attr]

    def test_compute_statistics_file_not_found(self, tmp_path: Path) -> None:
        validator = DatasetValidator()
        nonexistent = str(tmp_path / "does_not_exist.jsonl")
        result = validator.compute_statistics(nonexistent)
        assert "error" in result

    def test_compute_statistics_type_distribution(self, jsonl_file: Path) -> None:
        validator = DatasetValidator()
        stats = validator.compute_statistics(str(jsonl_file))
        type_dist = stats["type_distribution"]
        assert isinstance(type_dist, dict)
        # trace_id values are all strings
        assert "str" in type_dist["trace_id"]


# ---------------------------------------------------------------------------
# DatasetVersionManager tests
# ---------------------------------------------------------------------------


class TestDatasetVersionManager:
    def test_create_first_version(self) -> None:
        vm = DatasetVersionManager()
        version = vm.create_version("ds-001", "Initial release.")
        assert version.version == "1.0.0"
        assert version.changes == "Initial release."

    def test_create_second_version_increments_minor(self) -> None:
        vm = DatasetVersionManager()
        vm.create_version("ds-001", "Initial release.")
        v2 = vm.create_version("ds-001", "Added new records.")
        assert v2.version == "1.1.0"

    def test_create_multiple_versions(self) -> None:
        vm = DatasetVersionManager()
        vm.create_version("ds-001", "v1")
        vm.create_version("ds-001", "v2")
        v3 = vm.create_version("ds-001", "v3")
        assert v3.version == "1.2.0"

    def test_versions_are_independent_per_dataset(self) -> None:
        vm = DatasetVersionManager()
        vm.create_version("ds-001", "v1 for ds-001")
        v1_ds2 = vm.create_version("ds-002", "v1 for ds-002")
        assert v1_ds2.version == "1.0.0"

    def test_list_versions_empty(self) -> None:
        vm = DatasetVersionManager()
        assert vm.list_versions("ds-001") == []

    def test_list_versions_returns_ordered(self) -> None:
        vm = DatasetVersionManager()
        vm.create_version("ds-001", "v1")
        vm.create_version("ds-001", "v2")
        versions = vm.list_versions("ds-001")
        assert len(versions) == 2
        assert versions[0].version == "1.0.0"
        assert versions[1].version == "1.1.0"

    def test_version_has_created_at(self) -> None:
        vm = DatasetVersionManager()
        version = vm.create_version("ds-001", "test")
        assert version.created_at is not None

    def test_list_versions_returns_copy(self) -> None:
        vm = DatasetVersionManager()
        vm.create_version("ds-001", "v1")
        v1 = vm.list_versions("ds-001")
        v2 = vm.list_versions("ds-001")
        assert v1 is not v2  # Should be independent copies


# ---------------------------------------------------------------------------
# compute_sha256 tests
# ---------------------------------------------------------------------------


class TestComputeSha256:
    def test_sha256_known_content(self, tmp_path: Path) -> None:
        import hashlib

        content = b"hello, world"
        path = tmp_path / "file.bin"
        path.write_bytes(content)
        expected = hashlib.sha256(content).hexdigest()
        result = compute_sha256(str(path))
        assert result == expected

    def test_sha256_returns_hex_string(self, tmp_path: Path) -> None:
        path = tmp_path / "file.bin"
        path.write_bytes(b"test data")
        result = compute_sha256(str(path))
        assert isinstance(result, str)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_sha256_empty_file(self, tmp_path: Path) -> None:
        import hashlib

        path = tmp_path / "empty.bin"
        path.write_bytes(b"")
        expected = hashlib.sha256(b"").hexdigest()
        assert compute_sha256(str(path)) == expected

    def test_sha256_different_files_differ(self, tmp_path: Path) -> None:
        path1 = tmp_path / "a.bin"
        path2 = tmp_path / "b.bin"
        path1.write_bytes(b"data a")
        path2.write_bytes(b"data b")
        assert compute_sha256(str(path1)) != compute_sha256(str(path2))

    def test_sha256_same_content_same_hash(self, tmp_path: Path) -> None:
        path1 = tmp_path / "a.bin"
        path2 = tmp_path / "b.bin"
        path1.write_bytes(b"identical")
        path2.write_bytes(b"identical")
        assert compute_sha256(str(path1)) == compute_sha256(str(path2))
