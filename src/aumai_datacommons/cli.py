"""CLI entry point for aumai-datacommons."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import click

from aumai_datacommons.core import (
    DatasetCatalog,
    DatasetNotFoundError,
    DatasetValidator,
    DatasetVersionManager,
)
from aumai_datacommons.models import DatasetFormat, DatasetMetadata

# Module-level singletons used by CLI commands so they share state within a
# single process invocation (e.g. during tests).
_catalog = DatasetCatalog()
_validator = DatasetValidator()
_version_manager = DatasetVersionManager()


@click.group()
@click.version_option()
def main() -> None:
    """AumAI Datacommons — open datasets for agent development."""


@main.command("search")
@click.option("--query", required=True, help="Search query string.")
@click.option(
    "--format",
    "dataset_format",
    type=click.Choice([f.value for f in DatasetFormat]),
    default=None,
    help="Filter by dataset format.",
)
@click.option(
    "--tag",
    "tags",
    multiple=True,
    help="Filter by tag (repeatable).",
)
def search_command(
    query: str,
    dataset_format: str | None,
    tags: tuple[str, ...],
) -> None:
    """Search the dataset catalog.

    Example: aumai-datacommons search --query "agent traces"
    """
    fmt = DatasetFormat(dataset_format) if dataset_format else None
    tag_list: list[str] = list(tags) if tags else []
    results = _catalog.search(query=query, dataset_format=fmt, tags=tag_list or None)

    if not results:
        click.echo("No datasets found matching the given criteria.")
        return

    for metadata in results:
        click.echo(
            f"[{metadata.dataset_id}] {metadata.name} v{metadata.version}"
            f" ({metadata.format.value}, {metadata.num_records} records)"
        )
        click.echo(f"  {metadata.description}")
        if metadata.tags:
            click.echo(f"  Tags: {', '.join(metadata.tags)}")
        click.echo()


@main.command("register")
@click.option(
    "--config",
    "config_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to a JSON or YAML config file with DatasetMetadata fields.",
)
def register_command(config_path: str) -> None:
    """Register a dataset from a config file.

    The config file must be a JSON file whose keys match the
    DatasetMetadata schema fields.

    Example: aumai-datacommons register --config dataset.json
    """
    path = Path(config_path)
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        click.echo(f"Error reading config: {exc}", err=True)
        sys.exit(1)

    try:
        data: object = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        click.echo(f"Invalid JSON in config: {exc}", err=True)
        sys.exit(1)

    if not isinstance(data, dict):
        click.echo("Config file must contain a JSON object.", err=True)
        sys.exit(1)

    try:
        metadata = DatasetMetadata.model_validate(data)
    except Exception as exc:
        click.echo(f"Validation error: {exc}", err=True)
        sys.exit(1)

    _catalog.register(metadata)
    _version_manager.create_version(metadata.dataset_id, "Initial registration.")
    click.echo(
        f"Dataset '{metadata.dataset_id}' registered successfully (v{metadata.version})."
    )


@main.command("validate")
@click.option(
    "--dataset",
    "dataset_path",
    required=True,
    type=click.Path(),
    help="Path to a JSONL dataset file.",
)
@click.option(
    "--schema",
    "schema_path",
    required=True,
    type=click.Path(exists=True),
    help="Path to a JSON schema file (field-name → type-name map).",
)
def validate_command(dataset_path: str, schema_path: str) -> None:
    """Validate a dataset file against a schema.

    Example: aumai-datacommons validate --dataset data.jsonl --schema schema.json
    """
    schema_text = Path(schema_path).read_text(encoding="utf-8")
    try:
        schema_raw: object = json.loads(schema_text)
    except json.JSONDecodeError as exc:
        click.echo(f"Invalid schema JSON: {exc}", err=True)
        sys.exit(1)

    if not isinstance(schema_raw, dict):
        click.echo("Schema must be a JSON object.", err=True)
        sys.exit(1)

    schema: dict[str, object] = schema_raw
    errors = _validator.validate_schema(dataset_path, schema)

    if errors:
        click.echo(f"Validation failed with {len(errors)} error(s):")
        for error in errors:
            click.echo(f"  - {error}")
        sys.exit(1)
    else:
        click.echo("Validation passed — no errors found.")


@main.command("stats")
@click.option(
    "--dataset",
    "dataset_path",
    required=True,
    type=click.Path(),
    help="Path to a JSONL or CSV dataset file.",
)
def stats_command(dataset_path: str) -> None:
    """Print basic statistics for a dataset file.

    Example: aumai-datacommons stats --dataset data.jsonl
    """
    stats = _validator.compute_statistics(dataset_path)
    click.echo(json.dumps(stats, indent=2))


@main.command("list")
@click.option("--limit", default=20, show_default=True, help="Max results to show.")
@click.option("--offset", default=0, show_default=True, help="Skip first N results.")
def list_command(limit: int, offset: int) -> None:
    """List all registered datasets.

    Example: aumai-datacommons list --limit 10
    """
    items = _catalog.list_all(limit=limit, offset=offset)
    if not items:
        click.echo("No datasets registered.")
        return
    for metadata in items:
        click.echo(
            f"[{metadata.dataset_id}] {metadata.name} v{metadata.version}"
            f" — {metadata.format.value}"
        )


@main.command("get")
@click.argument("dataset_id")
def get_command(dataset_id: str) -> None:
    """Show full metadata for a dataset by ID.

    Example: aumai-datacommons get my-dataset-001
    """
    try:
        metadata = _catalog.get(dataset_id)
    except DatasetNotFoundError as exc:
        click.echo(str(exc), err=True)
        sys.exit(1)

    click.echo(json.dumps(metadata.model_dump(mode="json"), indent=2))


if __name__ == "__main__":
    main()
