"""Shared test fixtures for aumai-datacommons."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from aumai_datacommons.models import DatasetFormat, DatasetMetadata


@pytest.fixture()
def catalog_metadata() -> DatasetMetadata:
    """A valid DatasetMetadata object for use in tests."""
    return DatasetMetadata(
        dataset_id="ds-001",
        name="Agent Traces Dataset",
        description="Traces collected from autonomous agent runs for benchmarking.",
        format=DatasetFormat.jsonl,
        size_bytes=1024000,
        num_records=5000,
        license="Apache-2.0",
        tags=["agents", "traces", "benchmarking"],
        version="1.0.0",
        schema={"trace_id": "str", "action": "str", "reward": "float"},
    )


@pytest.fixture()
def second_metadata() -> DatasetMetadata:
    """A second DatasetMetadata for multi-entry tests."""
    return DatasetMetadata(
        dataset_id="ds-002",
        name="CSV Tabular Dataset",
        description="Tabular data for regression tasks.",
        format=DatasetFormat.csv,
        size_bytes=512000,
        num_records=2000,
        license="MIT",
        tags=["tabular", "regression"],
        version="2.1.0",
    )


@pytest.fixture()
def jsonl_file(tmp_path: Path) -> Path:
    """A temporary JSONL file with valid records."""
    path = tmp_path / "data.jsonl"
    records = [
        {"trace_id": "t-001", "action": "move", "reward": 1.0},
        {"trace_id": "t-002", "action": "stop", "reward": 0.0},
        {"trace_id": "t-003", "action": "jump", "reward": 2.5},
    ]
    lines = [json.dumps(r) for r in records]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


@pytest.fixture()
def csv_file(tmp_path: Path) -> Path:
    """A temporary CSV file with valid records."""
    path = tmp_path / "data.csv"
    path.write_text("id,value,label\n1,3.14,cat\n2,2.71,dog\n3,,fish\n", encoding="utf-8")
    return path


@pytest.fixture()
def schema_file(tmp_path: Path) -> Path:
    """A temporary JSON schema file."""
    path = tmp_path / "schema.json"
    path.write_text(
        json.dumps({"trace_id": "str", "action": "str", "reward": "float"}),
        encoding="utf-8",
    )
    return path


@pytest.fixture()
def dataset_config_file(tmp_path: Path, catalog_metadata: DatasetMetadata) -> Path:
    """A temporary JSON file representing a dataset registration config."""
    path = tmp_path / "dataset.json"
    path.write_text(catalog_metadata.model_dump_json(), encoding="utf-8")
    return path
