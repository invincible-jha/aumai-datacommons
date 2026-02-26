"""Pydantic models for aumai-datacommons."""

from __future__ import annotations

import enum
from datetime import datetime

from pydantic import BaseModel, Field


class DatasetFormat(str, enum.Enum):
    """Supported dataset file formats."""

    jsonl = "jsonl"
    csv = "csv"
    parquet = "parquet"
    arrow = "arrow"


class DatasetMetadata(BaseModel):
    """Metadata describing a registered dataset."""

    dataset_id: str = Field(..., description="Unique identifier for the dataset.")
    name: str = Field(..., description="Human-readable name.")
    description: str = Field(..., description="Detailed description of the dataset.")
    format: DatasetFormat = Field(..., description="File format of the dataset.")
    size_bytes: int = Field(..., ge=0, description="Size of the dataset in bytes.")
    num_records: int = Field(..., ge=0, description="Number of records in the dataset.")
    schema: dict[str, object] = Field(
        default_factory=dict,
        description="JSON Schema or field type map for the dataset.",
    )
    license: str = Field(..., description="SPDX license identifier or name.")
    tags: list[str] = Field(default_factory=list, description="Free-form search tags.")
    version: str = Field(default="1.0.0", description="Semantic version string.")
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when the record was created.",
    )


class DatasetVersion(BaseModel):
    """A single version entry for a dataset."""

    version: str = Field(..., description="Semantic version string.")
    changes: str = Field(..., description="Human-readable change description.")
    created_at: datetime = Field(
        default_factory=datetime.utcnow,
        description="UTC timestamp when the version was created.",
    )


class DownloadResult(BaseModel):
    """Result of a dataset download operation."""

    dataset_id: str = Field(..., description="ID of the downloaded dataset.")
    path: str = Field(..., description="Local filesystem path to the downloaded file.")
    verified: bool = Field(
        ..., description="Whether the downloaded file passed integrity verification."
    )
    sha256: str = Field(..., description="SHA-256 hex digest of the downloaded file.")


__all__ = [
    "DatasetFormat",
    "DatasetMetadata",
    "DatasetVersion",
    "DownloadResult",
]
