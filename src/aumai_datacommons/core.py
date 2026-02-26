"""Core logic for aumai-datacommons."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from aumai_datacommons.models import (
    DatasetFormat,
    DatasetMetadata,
    DatasetVersion,
)


class DatasetNotFoundError(KeyError):
    """Raised when a dataset_id is not present in the catalog."""


class DatasetCatalog:
    """In-memory catalog of registered datasets.

    Designed to be backed by a persistent store; the interface
    intentionally avoids I/O so callers can wrap it with any storage layer.
    """

    def __init__(self) -> None:
        self._store: dict[str, DatasetMetadata] = {}

    def register(self, metadata: DatasetMetadata) -> None:
        """Register or overwrite a dataset entry.

        Args:
            metadata: Complete metadata for the dataset.
        """
        self._store[metadata.dataset_id] = metadata

    def search(
        self,
        query: str,
        format: DatasetFormat | None = None,
        tags: list[str] | None = None,
    ) -> list[DatasetMetadata]:
        """Search the catalog with optional filters.

        Performs case-insensitive substring matching on ``name`` and
        ``description``. Additional filters narrow the result set.

        Args:
            query: Substring to match against name and description.
            format: If given, only datasets of this format are returned.
            tags: If given, datasets must carry *all* of these tags.

        Returns:
            List of matching ``DatasetMetadata`` objects.
        """
        query_lower = query.lower()
        results: list[DatasetMetadata] = []

        for metadata in self._store.values():
            text_match = query_lower in metadata.name.lower() or query_lower in metadata.description.lower()
            if not text_match:
                continue
            if format is not None and metadata.format != format:
                continue
            if tags is not None:
                metadata_tag_set = set(metadata.tags)
                if not all(tag in metadata_tag_set for tag in tags):
                    continue
            results.append(metadata)

        return results

    def get(self, dataset_id: str) -> DatasetMetadata:
        """Retrieve a dataset by its ID.

        Args:
            dataset_id: The unique identifier of the dataset.

        Returns:
            The ``DatasetMetadata`` for that ID.

        Raises:
            DatasetNotFoundError: If no dataset with that ID exists.
        """
        try:
            return self._store[dataset_id]
        except KeyError:
            raise DatasetNotFoundError(f"Dataset '{dataset_id}' not found.") from None

    def list_all(self, limit: int = 100, offset: int = 0) -> list[DatasetMetadata]:
        """Return a paginated list of all registered datasets.

        Args:
            limit: Maximum number of results to return.
            offset: Number of records to skip from the beginning.

        Returns:
            Slice of all registered ``DatasetMetadata`` objects.
        """
        all_items = list(self._store.values())
        return all_items[offset : offset + limit]


class DatasetValidator:
    """Validates dataset files against declared schemas and computes statistics."""

    def validate_schema(self, data_path: str, schema: dict[str, object]) -> list[str]:
        """Validate each record in a JSONL file against a field-type schema.

        The schema format is ``{"field_name": "type_name", ...}`` where
        type names are Python primitives: ``str``, ``int``, ``float``,
        ``bool``, ``list``, ``dict``.

        Args:
            data_path: Filesystem path to a ``.jsonl`` file.
            schema: Mapping from field name to expected type name string.

        Returns:
            List of validation error messages.  Empty means valid.
        """
        type_map: dict[str, type[object]] = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            "list": list,
            "dict": dict,
        }
        errors: list[str] = []
        path = Path(data_path)

        if not path.exists():
            return [f"File not found: {data_path}"]

        with path.open(encoding="utf-8") as file_handle:
            for line_number, raw_line in enumerate(file_handle, start=1):
                raw_line = raw_line.strip()
                if not raw_line:
                    continue
                try:
                    record: object = json.loads(raw_line)
                except json.JSONDecodeError as exc:
                    errors.append(f"Line {line_number}: JSON decode error — {exc}")
                    continue

                if not isinstance(record, dict):
                    errors.append(f"Line {line_number}: record is not a JSON object.")
                    continue

                record_dict: dict[str, object] = record
                for field_name, expected_type_name in schema.items():
                    if not isinstance(expected_type_name, str):
                        continue
                    if field_name not in record_dict:
                        errors.append(
                            f"Line {line_number}: missing required field '{field_name}'."
                        )
                        continue
                    python_type = type_map.get(expected_type_name)
                    if python_type is None:
                        continue
                    value = record_dict[field_name]
                    if not isinstance(value, python_type):
                        errors.append(
                            f"Line {line_number}: field '{field_name}' expected"
                            f" {expected_type_name}, got {type(value).__name__}."
                        )

        return errors

    def compute_statistics(self, data_path: str) -> dict[str, object]:
        """Compute basic statistics for a JSONL or CSV file.

        Args:
            data_path: Filesystem path to the data file.

        Returns:
            Dictionary containing ``row_count``, ``null_counts`` per field,
            and ``type_distribution`` per field.
        """
        path = Path(data_path)
        if not path.exists():
            return {"error": f"File not found: {data_path}"}

        suffix = path.suffix.lower()

        rows: list[dict[str, object]] = []

        if suffix == ".csv":
            with path.open(encoding="utf-8", newline="") as csv_handle:
                reader = csv.DictReader(csv_handle)
                for row in reader:
                    rows.append(dict(row))
        else:
            # Treat everything else as JSONL.
            with path.open(encoding="utf-8") as jsonl_handle:
                for raw_line in jsonl_handle:
                    raw_line = raw_line.strip()
                    if not raw_line:
                        continue
                    try:
                        parsed: object = json.loads(raw_line)
                        if isinstance(parsed, dict):
                            rows.append(parsed)
                    except json.JSONDecodeError:
                        continue

        row_count = len(rows)
        null_counts: dict[str, int] = defaultdict(int)
        type_dist: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for row in rows:
            for field, value in row.items():
                if value is None or value == "":
                    null_counts[field] += 1
                type_name = type(value).__name__
                type_dist[field][type_name] += 1

        return {
            "row_count": row_count,
            "null_counts": dict(null_counts),
            "type_distribution": {k: dict(v) for k, v in type_dist.items()},
        }


class DatasetVersionManager:
    """Tracks version history for datasets."""

    def __init__(self) -> None:
        self._versions: dict[str, list[DatasetVersion]] = defaultdict(list)

    def create_version(self, dataset_id: str, changes: str) -> DatasetVersion:
        """Create a new version entry for the given dataset.

        The version number is auto-incremented as a minor bump from the
        last recorded version (or starts at ``1.0.0``).

        Args:
            dataset_id: The dataset to version.
            changes: Human-readable change description.

        Returns:
            The newly created ``DatasetVersion``.
        """
        existing = self._versions[dataset_id]
        if existing:
            last_version_str = existing[-1].version
            parts = last_version_str.split(".")
            try:
                minor = int(parts[1]) + 1
                new_version = f"{parts[0]}.{minor}.0"
            except (IndexError, ValueError):
                new_version = "1.1.0"
        else:
            new_version = "1.0.0"

        version_entry = DatasetVersion(
            version=new_version,
            changes=changes,
            created_at=datetime.utcnow(),
        )
        self._versions[dataset_id].append(version_entry)
        return version_entry

    def list_versions(self, dataset_id: str) -> list[DatasetVersion]:
        """Return all version entries for a dataset, oldest first.

        Args:
            dataset_id: The dataset whose history is requested.

        Returns:
            Ordered list of ``DatasetVersion`` objects.
        """
        return list(self._versions[dataset_id])


def compute_sha256(file_path: str) -> str:
    """Compute the SHA-256 hex digest of a file.

    Args:
        file_path: Absolute or relative path to the file.

    Returns:
        Lowercase hex string of the SHA-256 digest.
    """
    hasher = hashlib.sha256()
    path = Path(file_path)
    with path.open("rb") as binary_handle:
        for chunk in iter(lambda: binary_handle.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


__all__ = [
    "DatasetCatalog",
    "DatasetNotFoundError",
    "DatasetValidator",
    "DatasetVersionManager",
    "compute_sha256",
]
