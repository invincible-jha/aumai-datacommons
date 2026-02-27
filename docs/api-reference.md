# API Reference — aumai-datacommons

Complete reference for all public classes, functions, and Pydantic models.

---

## Module: `aumai_datacommons.core`

### `DatasetNotFoundError`

```python
class DatasetNotFoundError(KeyError)
```

Raised when a `dataset_id` is not present in the catalog. Inherits from `KeyError` so it can be caught with either `DatasetNotFoundError` or `KeyError`.

**Example:**

```python
from aumai_datacommons import DatasetCatalog, DatasetNotFoundError

catalog = DatasetCatalog()
try:
    catalog.get("nonexistent-id")
except DatasetNotFoundError as exc:
    print(str(exc))  # "Dataset 'nonexistent-id' not found."
```

---

### `DatasetCatalog`

```python
class DatasetCatalog
```

In-memory catalog of registered datasets. Designed to be wrapped by a persistent storage layer. All operations on this class are synchronous and have no I/O side effects.

**Constructor:**

```python
DatasetCatalog() -> None
```

Initializes an empty catalog.

---

#### `DatasetCatalog.register`

```python
def register(self, metadata: DatasetMetadata) -> None
```

Register or overwrite a dataset entry.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `metadata` | `DatasetMetadata` | Complete metadata for the dataset. The `dataset_id` field is used as the key. |

**Returns:** `None`

**Notes:** If a dataset with the same `dataset_id` already exists, it is silently overwritten. This is an upsert operation.

**Example:**

```python
from aumai_datacommons import DatasetCatalog, DatasetMetadata, DatasetFormat

catalog = DatasetCatalog()
metadata = DatasetMetadata(
    dataset_id="my-dataset",
    name="My Dataset",
    description="A fine-tuning dataset.",
    format=DatasetFormat.jsonl,
    size_bytes=102400,
    num_records=1000,
    license="Apache-2.0",
)
catalog.register(metadata)
```

---

#### `DatasetCatalog.search`

```python
def search(
    self,
    query: str,
    dataset_format: DatasetFormat | None = None,
    tags: list[str] | None = None,
) -> list[DatasetMetadata]
```

Search the catalog with optional filters.

Performs case-insensitive substring matching on `name` and `description`. Additional filters narrow the result set. All active filters must match for a dataset to be included.

**Parameters:**

| Name | Type | Default | Description |
|---|---|---|---|
| `query` | `str` | required | Substring to match against `name` and `description` (case-insensitive). |
| `dataset_format` | `DatasetFormat \| None` | `None` | If given, only datasets of this format are returned. |
| `tags` | `list[str] \| None` | `None` | If given, datasets must carry **all** of these tags. |

**Returns:** `list[DatasetMetadata]` — matching datasets in insertion order.

**Example:**

```python
# Search with all filters
results = catalog.search(
    "evaluation",
    dataset_format=DatasetFormat.jsonl,
    tags=["benchmark", "en"],
)
for item in results:
    print(item.dataset_id, item.name)
```

---

#### `DatasetCatalog.get`

```python
def get(self, dataset_id: str) -> DatasetMetadata
```

Retrieve a dataset by its exact ID.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `dataset_id` | `str` | The unique identifier of the dataset. Case-sensitive. |

**Returns:** `DatasetMetadata`

**Raises:** `DatasetNotFoundError` — if no dataset with that ID exists.

**Example:**

```python
metadata = catalog.get("my-dataset")
print(metadata.name, metadata.num_records)
```

---

#### `DatasetCatalog.list_all`

```python
def list_all(self, limit: int = 100, offset: int = 0) -> list[DatasetMetadata]
```

Return a paginated slice of all registered datasets in insertion order.

**Parameters:**

| Name | Type | Default | Description |
|---|---|---|---|
| `limit` | `int` | `100` | Maximum number of results to return. |
| `offset` | `int` | `0` | Number of records to skip from the beginning. |

**Returns:** `list[DatasetMetadata]`

**Example:**

```python
# Page 1: records 0–19
page_1 = catalog.list_all(limit=20, offset=0)
# Page 2: records 20–39
page_2 = catalog.list_all(limit=20, offset=20)
```

---

### `DatasetValidator`

```python
class DatasetValidator
```

Validates dataset files against declared schemas and computes basic statistics. Reads from the local filesystem. Does not interact with `DatasetCatalog`.

---

#### `DatasetValidator.validate_schema`

```python
def validate_schema(
    self,
    data_path: str,
    schema: dict[str, object],
) -> list[str]
```

Validate each record in a JSONL file against a field-type schema.

Reads the file line by line in streaming fashion. Each non-empty line is parsed as JSON and checked against the schema. Returns a list of error message strings — an empty list means the file is valid.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `data_path` | `str` | Filesystem path to a `.jsonl` file. |
| `schema` | `dict[str, object]` | Mapping from field name to expected type name string. Valid type names: `"str"`, `"int"`, `"float"`, `"bool"`, `"list"`, `"dict"`. Fields with unrecognized type names are silently skipped. |

**Returns:** `list[str]` — validation error messages. Empty list means valid.

**Error message formats:**
- `"File not found: <path>"` — file does not exist
- `"Line N: JSON decode error — <detail>"` — invalid JSON on line N
- `"Line N: record is not a JSON object."` — JSON value is not a dict
- `"Line N: missing required field '<field>'."` — field absent from record
- `"Line N: field '<field>' expected <type>, got <actual_type>."` — type mismatch

**Example:**

```python
from aumai_datacommons import DatasetValidator

validator = DatasetValidator()
errors = validator.validate_schema("data.jsonl", {
    "prompt": "str",
    "label": "str",
    "score": "float",
    "tags": "list",
})
if errors:
    for msg in errors:
        print(msg)
```

---

#### `DatasetValidator.compute_statistics`

```python
def compute_statistics(self, data_path: str) -> dict[str, object]
```

Compute basic statistics for a JSONL or CSV file.

Detects file type by extension: `.csv` uses `csv.DictReader`; everything else is treated as JSONL. Loads all rows into memory, so very large files (> 1 GB) should be processed in chunks externally.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `data_path` | `str` | Filesystem path to a JSONL or CSV file. |

**Returns:** `dict[str, object]` with keys:

| Key | Type | Description |
|---|---|---|
| `"row_count"` | `int` | Total number of valid rows parsed. |
| `"null_counts"` | `dict[str, int]` | Per-field count of `None` or empty-string values. |
| `"type_distribution"` | `dict[str, dict[str, int]]` | Per-field distribution of Python type names (e.g. `{"str": 950, "NoneType": 50}`). |
| `"error"` | `str` | Present only if the file was not found. |

**Example:**

```python
stats = validator.compute_statistics("data.jsonl")
print("Total rows:", stats["row_count"])
print("Null counts:", stats["null_counts"])
print("Types:", stats["type_distribution"])
```

---

### `DatasetVersionManager`

```python
class DatasetVersionManager
```

Tracks semantic version history for datasets. Each `DatasetVersionManager` instance maintains its own isolated history store.

---

#### `DatasetVersionManager.create_version`

```python
def create_version(self, dataset_id: str, changes: str) -> DatasetVersion
```

Create a new version entry for the given dataset.

The version number is auto-incremented as a minor bump from the last recorded version, or starts at `1.0.0` if no history exists. The format is `MAJOR.MINOR.PATCH`.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `dataset_id` | `str` | The dataset to version. Does not need to be registered in a catalog. |
| `changes` | `str` | Human-readable change description for this version. |

**Returns:** `DatasetVersion` — the newly created version entry.

**Versioning rules:**
- No history → `1.0.0`
- Previous `1.0.0` → `1.1.0`
- Previous `1.4.0` → `1.5.0`
- Malformed previous version → `1.1.0`

**Example:**

```python
from aumai_datacommons import DatasetVersionManager

manager = DatasetVersionManager()
v1 = manager.create_version("ds-001", "Initial release. 5,000 examples.")
v2 = manager.create_version("ds-001", "Removed 200 duplicates. Added 500 new examples.")
print(v1.version)  # "1.0.0"
print(v2.version)  # "1.1.0"
```

---

#### `DatasetVersionManager.list_versions`

```python
def list_versions(self, dataset_id: str) -> list[DatasetVersion]
```

Return all version entries for a dataset, in chronological order (oldest first).

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `dataset_id` | `str` | The dataset whose history is requested. |

**Returns:** `list[DatasetVersion]` — empty list if no versions have been recorded.

**Example:**

```python
history = manager.list_versions("ds-001")
for entry in history:
    print(f"v{entry.version}  {entry.created_at.date()}  {entry.changes}")
```

---

### `compute_sha256`

```python
def compute_sha256(file_path: str) -> str
```

Compute the SHA-256 hex digest of a file using streaming reads.

Reads the file in 65,536-byte (64 KB) chunks to maintain flat memory usage regardless of file size.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `file_path` | `str` | Absolute or relative path to the file. |

**Returns:** `str` — lowercase hexadecimal SHA-256 digest (64 characters).

**Raises:** `FileNotFoundError` — if the file does not exist. `PermissionError` — if the file cannot be read.

**Example:**

```python
from aumai_datacommons import compute_sha256

digest = compute_sha256("/data/traces.jsonl")
print(digest)
# e.g. "3b4c9a8d2f1e0b5c7a9d4f2e8b1c3d6e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4"
```

---

## Module: `aumai_datacommons.models`

### `DatasetFormat`

```python
class DatasetFormat(str, enum.Enum)
```

Supported dataset file formats. Inherits from `str` so values can be used directly as strings.

| Member | Value | Description |
|---|---|---|
| `DatasetFormat.jsonl` | `"jsonl"` | JSON Lines — one JSON object per line |
| `DatasetFormat.csv` | `"csv"` | Comma-separated values |
| `DatasetFormat.parquet` | `"parquet"` | Apache Parquet columnar format |
| `DatasetFormat.arrow` | `"arrow"` | Apache Arrow IPC format |

---

### `DatasetMetadata`

```python
class DatasetMetadata(BaseModel)
```

Metadata describing a registered dataset. All instances are validated by Pydantic at construction time.

**Fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `dataset_id` | `str` | Yes | — | Unique identifier. Used as the catalog key. |
| `name` | `str` | Yes | — | Human-readable name. |
| `description` | `str` | Yes | — | Detailed description of the dataset. |
| `format` | `DatasetFormat` | Yes | — | File format of the dataset. |
| `size_bytes` | `int` | Yes | — | Size in bytes. Must be `>= 0`. |
| `num_records` | `int` | Yes | — | Number of records. Must be `>= 0`. |
| `schema` | `dict[str, object]` | No | `{}` | Field-type map or JSON Schema for the dataset. |
| `license` | `str` | Yes | — | SPDX license identifier (e.g. `"Apache-2.0"`, `"CC-BY-4.0"`). |
| `tags` | `list[str]` | No | `[]` | Free-form search tags. |
| `version` | `str` | No | `"1.0.0"` | Semantic version string. |
| `created_at` | `datetime` | No | `datetime.now(UTC)` | UTC timestamp of record creation. |

**Example:**

```python
from aumai_datacommons.models import DatasetMetadata, DatasetFormat
from datetime import datetime, timezone

metadata = DatasetMetadata(
    dataset_id="rag-eval-001",
    name="RAG Evaluation Benchmark",
    description="500 question-answer pairs for retrieval-augmented generation evaluation.",
    format=DatasetFormat.jsonl,
    size_bytes=51200,
    num_records=500,
    schema={"question": "str", "context": "str", "answer": "str"},
    license="CC-BY-4.0",
    tags=["rag", "evaluation", "qa"],
    version="2.1.0",
)

# Serialize to dict
record = metadata.model_dump(mode="json")

# Deserialize from dict
restored = DatasetMetadata.model_validate(record)
```

---

### `DatasetVersion`

```python
class DatasetVersion(BaseModel)
```

A single version entry in a dataset's history.

**Fields:**

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `version` | `str` | Yes | — | Semantic version string (e.g. `"1.2.0"`). |
| `changes` | `str` | Yes | — | Human-readable change description for this version. |
| `created_at` | `datetime` | No | `datetime.now(UTC)` | UTC timestamp when this version was created. |

**Example:**

```python
from aumai_datacommons.models import DatasetVersion

version = DatasetVersion(
    version="1.3.0",
    changes="Added multilingual examples. Fixed label encoding bug.",
)
print(version.version)     # "1.3.0"
print(version.created_at)  # UTC timestamp
```

---

### `DownloadResult`

```python
class DownloadResult(BaseModel)
```

Result of a dataset download operation, including integrity verification status.

**Fields:**

| Field | Type | Required | Description |
|---|---|---|---|
| `dataset_id` | `str` | Yes | ID of the downloaded dataset. |
| `path` | `str` | Yes | Local filesystem path to the downloaded file. |
| `verified` | `bool` | Yes | Whether the downloaded file passed integrity verification (SHA-256 comparison). |
| `sha256` | `str` | Yes | SHA-256 hex digest of the downloaded file. |

**Example:**

```python
from aumai_datacommons import compute_sha256
from aumai_datacommons.models import DownloadResult

expected_hash = "abc123..."
actual_hash = compute_sha256("/tmp/download/dataset.jsonl")

result = DownloadResult(
    dataset_id="rag-eval-001",
    path="/tmp/download/dataset.jsonl",
    verified=(actual_hash == expected_hash),
    sha256=actual_hash,
)
print("Verified:", result.verified)
```

---

## Module: `aumai_datacommons.cli`

The CLI is a Click group registered as the `aumai-datacommons` entry point.

### Commands summary

| Command | Description |
|---|---|
| `search --query TEXT [--format FORMAT] [--tag TAG ...]` | Search the catalog by text with optional filters |
| `register --config PATH` | Register a dataset from a JSON metadata file |
| `validate --dataset PATH --schema PATH` | Validate a JSONL file against a field-type schema |
| `stats --dataset PATH` | Print row count, null counts, and type distribution |
| `list [--limit N] [--offset N]` | List all registered datasets with pagination |
| `get DATASET_ID` | Show full JSON metadata for a dataset by ID |

All commands use module-level singleton instances of `DatasetCatalog`, `DatasetValidator`, and `DatasetVersionManager`. The catalog state persists only within a single process invocation.

---

## Package exports (`aumai_datacommons.__init__`)

The following names are importable directly from `aumai_datacommons`:

```python
from aumai_datacommons import (
    DatasetCatalog,
    DatasetFormat,
    DatasetMetadata,
    DatasetNotFoundError,
    DatasetValidator,
    DatasetVersion,
    DatasetVersionManager,
    DownloadResult,
    compute_sha256,
)
```

Package version:

```python
import aumai_datacommons
print(aumai_datacommons.__version__)  # "0.1.0"
```
