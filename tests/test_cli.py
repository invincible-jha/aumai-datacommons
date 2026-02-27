"""Tests for aumai-datacommons CLI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from aumai_datacommons.cli import main
from aumai_datacommons.models import DatasetMetadata


def _extract_json(text: str) -> dict:  # type: ignore[type-arg]
    """Extract the first JSON object from a string (handles leading log lines)."""
    start = text.index("{")
    depth = 0
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(text[start : i + 1])
    raise ValueError("No JSON object found")


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def registered_catalog(tmp_path: Path, runner: CliRunner) -> str:
    """Register a dataset via the CLI and return its dataset_id."""
    config = {
        "dataset_id": "cli-ds-001",
        "name": "CLI Test Dataset",
        "description": "A dataset registered via CLI for testing.",
        "format": "jsonl",
        "size_bytes": 2048,
        "num_records": 100,
        "license": "MIT",
        "tags": ["cli", "test"],
    }
    config_file = tmp_path / "dataset.json"
    config_file.write_text(json.dumps(config), encoding="utf-8")

    result = runner.invoke(main, ["register", "--config", str(config_file)])
    assert result.exit_code == 0, result.output
    return "cli-ds-001"


# ---------------------------------------------------------------------------
# --version
# ---------------------------------------------------------------------------


def test_cli_version(runner: CliRunner) -> None:
    """Version flag must report 0.1.0."""
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


# ---------------------------------------------------------------------------
# list command
# ---------------------------------------------------------------------------


def test_list_no_crash(runner: CliRunner) -> None:
    """List command must not crash regardless of catalog state."""
    result = runner.invoke(main, ["list"])
    assert result.exit_code == 0


def test_list_shows_registered(
    runner: CliRunner, registered_catalog: str, tmp_path: Path
) -> None:
    result = runner.invoke(main, ["list"])
    assert result.exit_code == 0
    assert "cli-ds-001" in result.output or "CLI Test Dataset" in result.output


def test_list_limit_option(runner: CliRunner) -> None:
    result = runner.invoke(main, ["list", "--limit", "5"])
    assert result.exit_code == 0


def test_list_offset_option(runner: CliRunner) -> None:
    result = runner.invoke(main, ["list", "--offset", "0"])
    assert result.exit_code == 0


# ---------------------------------------------------------------------------
# register command
# ---------------------------------------------------------------------------


def test_register_valid_config(runner: CliRunner, tmp_path: Path) -> None:
    config = {
        "dataset_id": "reg-ds-999",
        "name": "Registration Test",
        "description": "Testing registration endpoint.",
        "format": "csv",
        "size_bytes": 100,
        "num_records": 50,
        "license": "Apache-2.0",
    }
    config_file = tmp_path / "reg.json"
    config_file.write_text(json.dumps(config), encoding="utf-8")
    result = runner.invoke(main, ["register", "--config", str(config_file)])
    assert result.exit_code == 0
    assert "reg-ds-999" in result.output


def test_register_invalid_json(runner: CliRunner, tmp_path: Path) -> None:
    bad_file = tmp_path / "bad.json"
    bad_file.write_text("{not valid json}", encoding="utf-8")
    result = runner.invoke(main, ["register", "--config", str(bad_file)])
    assert result.exit_code != 0


def test_register_non_object_json(runner: CliRunner, tmp_path: Path) -> None:
    bad_file = tmp_path / "array.json"
    bad_file.write_text("[1, 2, 3]", encoding="utf-8")
    result = runner.invoke(main, ["register", "--config", str(bad_file)])
    assert result.exit_code != 0


def test_register_missing_required_field(runner: CliRunner, tmp_path: Path) -> None:
    # Missing 'license'
    config = {
        "dataset_id": "bad-ds",
        "name": "Bad",
        "description": "Missing license",
        "format": "csv",
        "size_bytes": 0,
        "num_records": 0,
    }
    config_file = tmp_path / "bad.json"
    config_file.write_text(json.dumps(config), encoding="utf-8")
    result = runner.invoke(main, ["register", "--config", str(config_file)])
    assert result.exit_code != 0


def test_register_config_not_found(runner: CliRunner) -> None:
    result = runner.invoke(main, ["register", "--config", "/nonexistent/path.json"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# search command
# ---------------------------------------------------------------------------


def test_search_no_results(runner: CliRunner) -> None:
    result = runner.invoke(main, ["search", "--query", "zzznothingmatchesthis"])
    assert result.exit_code == 0
    assert "No datasets found" in result.output


def test_search_returns_registered(
    runner: CliRunner, registered_catalog: str
) -> None:
    result = runner.invoke(main, ["search", "--query", "cli"])
    assert result.exit_code == 0
    assert "cli-ds-001" in result.output or "CLI Test Dataset" in result.output


def test_search_with_format_filter(
    runner: CliRunner, registered_catalog: str
) -> None:
    result = runner.invoke(
        main, ["search", "--query", "cli", "--format", "jsonl"]
    )
    assert result.exit_code == 0


def test_search_with_tag_filter(
    runner: CliRunner, registered_catalog: str
) -> None:
    result = runner.invoke(
        main, ["search", "--query", "cli", "--tag", "test"]
    )
    assert result.exit_code == 0
    assert "cli-ds-001" in result.output or "CLI Test Dataset" in result.output


def test_search_missing_query_fails(runner: CliRunner) -> None:
    result = runner.invoke(main, ["search"])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# validate command
# ---------------------------------------------------------------------------


def test_validate_valid_dataset(
    runner: CliRunner, tmp_path: Path
) -> None:
    jsonl_file = tmp_path / "data.jsonl"
    jsonl_file.write_text(
        '{"name": "alice", "age": 30}\n{"name": "bob", "age": 25}\n',
        encoding="utf-8",
    )
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps({"name": "str", "age": "int"}), encoding="utf-8")

    result = runner.invoke(
        main,
        [
            "validate",
            "--dataset",
            str(jsonl_file),
            "--schema",
            str(schema_file),
        ],
    )
    assert result.exit_code == 0
    assert "Validation passed" in result.output


def test_validate_schema_errors(runner: CliRunner, tmp_path: Path) -> None:
    jsonl_file = tmp_path / "data.jsonl"
    jsonl_file.write_text('{"name": 123}\n', encoding="utf-8")  # name should be str
    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps({"name": "str"}), encoding="utf-8")

    result = runner.invoke(
        main,
        ["validate", "--dataset", str(jsonl_file), "--schema", str(schema_file)],
    )
    assert result.exit_code != 0
    assert "Validation failed" in result.output


def test_validate_invalid_schema_json(runner: CliRunner, tmp_path: Path) -> None:
    jsonl_file = tmp_path / "data.jsonl"
    jsonl_file.write_text("{}\n", encoding="utf-8")
    bad_schema = tmp_path / "bad_schema.json"
    bad_schema.write_text("{bad json}", encoding="utf-8")

    result = runner.invoke(
        main,
        ["validate", "--dataset", str(jsonl_file), "--schema", str(bad_schema)],
    )
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# stats command
# ---------------------------------------------------------------------------


def test_stats_jsonl(runner: CliRunner, tmp_path: Path) -> None:
    jsonl_file = tmp_path / "data.jsonl"
    jsonl_file.write_text(
        '{"x": 1, "y": "a"}\n{"x": 2, "y": "b"}\n', encoding="utf-8"
    )
    result = runner.invoke(main, ["stats", "--dataset", str(jsonl_file)])
    assert result.exit_code == 0
    data = _extract_json(result.output)
    assert data["row_count"] == 2


def test_stats_csv(runner: CliRunner, tmp_path: Path) -> None:
    csv_file = tmp_path / "data.csv"
    csv_file.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
    result = runner.invoke(main, ["stats", "--dataset", str(csv_file)])
    assert result.exit_code == 0
    data = _extract_json(result.output)
    assert data["row_count"] == 2


def test_stats_nonexistent_file(runner: CliRunner, tmp_path: Path) -> None:
    nonexistent = str(tmp_path / "does_not_exist.jsonl")
    result = runner.invoke(main, ["stats", "--dataset", nonexistent])
    assert result.exit_code == 0  # CLI exits 0 but prints error JSON
    assert "error" in result.output


# ---------------------------------------------------------------------------
# get command
# ---------------------------------------------------------------------------


def test_get_existing_dataset(
    runner: CliRunner, registered_catalog: str
) -> None:
    result = runner.invoke(main, ["get", registered_catalog])
    assert result.exit_code == 0
    data = _extract_json(result.output)
    assert data["dataset_id"] == registered_catalog


def test_get_nonexistent_dataset(runner: CliRunner) -> None:
    result = runner.invoke(main, ["get", "totally-nonexistent-id-xyz"])
    assert result.exit_code != 0
