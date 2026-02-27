# Getting Started with aumai-datacommons

This guide walks you from installation to your first working dataset pipeline in under 15 minutes.

---

## Prerequisites

- Python 3.11 or later
- pip (or your preferred package manager)
- Basic familiarity with JSONL or CSV files

No database, no cloud account, no external service is required to get started. Everything runs locally.

---

## Installation

### From PyPI (recommended)

```bash
pip install aumai-datacommons
```

### From source

```bash
git clone https://github.com/aumai/aumai-datacommons.git
cd aumai-datacommons
pip install -e ".[dev]"
```

### Verify the installation

```bash
aumai-datacommons --version
python -c "import aumai_datacommons; print(aumai_datacommons.__version__)"
```

---

## Core Concepts

Before diving in, three concepts to internalize:

**DatasetMetadata** is the identity card for a dataset. It holds the `dataset_id` (unique key), `name`, `description`, `format`, `size_bytes`, `num_records`, `schema`, `license`, `tags`, and `version`. Nothing in the library does meaningful work until you have a `DatasetMetadata` object.

**DatasetCatalog** is the registry. It is a plain in-memory dictionary wrapped with search and retrieval methods. You register metadata into it and query it back. It has no storage of its own — persistence is your responsibility.

**DatasetValidator** is the quality gate. It reads actual data files from disk and checks them against declared schemas, and computes statistics. The catalog and validator are independent by design: you can validate without registering, and register without validating.

---

## Step-by-Step Tutorial

### Step 1: Create a sample JSONL dataset

Create a file called `agent-traces.jsonl` with a few records:

```jsonl
{"prompt": "What is the capital of France?", "thoughts": ["I know this."], "tool_calls": [], "final_answer": "Paris"}
{"prompt": "Search for the latest AI papers.", "thoughts": ["Need web search."], "tool_calls": [{"tool": "web_search", "query": "AI papers 2025"}], "final_answer": "Found 12 results."}
{"prompt": "Summarize the attached PDF.", "thoughts": ["Parse first."], "tool_calls": [{"tool": "pdf_reader", "file": "paper.pdf"}], "final_answer": "The paper presents..."}
```

### Step 2: Define a schema

Create `schema.json`:

```json
{
  "prompt": "str",
  "thoughts": "list",
  "tool_calls": "list",
  "final_answer": "str"
}
```

Valid type names are: `str`, `int`, `float`, `bool`, `list`, `dict`.

### Step 3: Validate the dataset

```bash
aumai-datacommons validate \
  --dataset agent-traces.jsonl \
  --schema schema.json
```

Expected output:

```
Validation passed — no errors found.
```

If a record is malformed, you will see output like:

```
Validation failed with 1 error(s):
  - Line 2: field 'final_answer' expected str, got NoneType.
```

### Step 4: Inspect statistics

```bash
aumai-datacommons stats --dataset agent-traces.jsonl
```

```json
{
  "row_count": 3,
  "null_counts": {},
  "type_distribution": {
    "prompt": { "str": 3 },
    "thoughts": { "list": 3 },
    "tool_calls": { "list": 3 },
    "final_answer": { "str": 3 }
  }
}
```

### Step 5: Register the dataset

Create `metadata.json`:

```json
{
  "dataset_id": "agent-traces-001",
  "name": "Agent Execution Traces",
  "description": "ReAct-style agent traces for tool-use fine-tuning. Covers factual Q&A, web search, and document parsing tasks.",
  "format": "jsonl",
  "size_bytes": 512,
  "num_records": 3,
  "schema": {
    "prompt": "str",
    "thoughts": "list",
    "tool_calls": "list",
    "final_answer": "str"
  },
  "license": "CC-BY-4.0",
  "tags": ["agents", "tool-use", "react", "fine-tuning"],
  "version": "1.0.0"
}
```

```bash
aumai-datacommons register --config metadata.json
# Dataset 'agent-traces-001' registered successfully (v1.0.0).
```

### Step 6: Search and retrieve

```bash
# Text search
aumai-datacommons search --query "agent traces"

# With filters
aumai-datacommons search --query "tool-use" --format jsonl --tag react

# Full metadata as JSON
aumai-datacommons get agent-traces-001
```

### Step 7: List all registered datasets

```bash
aumai-datacommons list
aumai-datacommons list --limit 5 --offset 0
```

---

## Common Patterns

### Pattern 1: Validate before register

Always validate before registering to catch schema drift early:

```python
from aumai_datacommons import DatasetCatalog, DatasetMetadata, DatasetValidator, DatasetFormat

validator = DatasetValidator()
catalog = DatasetCatalog()

errors = validator.validate_schema("my-dataset.jsonl", {
    "input": "str",
    "output": "str",
    "label": "str",
})

if errors:
    raise ValueError(f"Dataset failed validation: {errors}")

# Only register if valid
metadata = DatasetMetadata(
    dataset_id="my-dataset-001",
    name="My Dataset",
    description="A curated instruction dataset.",
    format=DatasetFormat.jsonl,
    size_bytes=102400,
    num_records=1000,
    license="Apache-2.0",
)
catalog.register(metadata)
```

### Pattern 2: Tag-based discovery

Use tags to organize datasets by task, language, domain, and quality tier:

```python
# Find all English evaluation datasets
eval_datasets = catalog.search(
    "evaluation",
    tags=["en", "evaluation", "benchmark"],
)

# Find all fine-tuning datasets for a specific domain
finance_ft = catalog.search(
    "fine-tuning",
    tags=["finance", "fine-tuning"],
    dataset_format=DatasetFormat.jsonl,
)
```

### Pattern 3: Track version history

Record every dataset update with a meaningful change description:

```python
from aumai_datacommons import DatasetVersionManager

manager = DatasetVersionManager()

# First release
v1 = manager.create_version("agent-traces-001", "Initial release. 3,000 examples.")
print(v1.version)  # 1.0.0

# After adding more data
v2 = manager.create_version(
    "agent-traces-001",
    "Added 2,000 multi-hop tool-chain examples. Removed 50 duplicates."
)
print(v2.version)  # 1.1.0

# Print full history
for entry in manager.list_versions("agent-traces-001"):
    print(f"v{entry.version}  {entry.created_at.date()}  {entry.changes}")
```

### Pattern 4: Integrity verification

Compute and store SHA-256 digests to verify downloads have not been corrupted:

```python
from aumai_datacommons import compute_sha256, DownloadResult

# Before upload: record the expected hash
expected_hash = compute_sha256("agent-traces.jsonl")
print(f"Expected: {expected_hash}")

# After download: verify
actual_hash = compute_sha256("downloaded-traces.jsonl")
result = DownloadResult(
    dataset_id="agent-traces-001",
    path="downloaded-traces.jsonl",
    verified=(actual_hash == expected_hash),
    sha256=actual_hash,
)
print("Verified:", result.verified)
```

### Pattern 5: Persist the catalog to disk

`DatasetCatalog` is in-memory. For a simple single-file persistence solution:

```python
import json
from aumai_datacommons import DatasetCatalog, DatasetMetadata

CATALOG_PATH = "catalog.json"

def save_catalog(catalog: DatasetCatalog) -> None:
    records = [m.model_dump(mode="json") for m in catalog.list_all(limit=100_000)]
    with open(CATALOG_PATH, "w", encoding="utf-8") as file_handle:
        json.dump(records, file_handle, indent=2, default=str)

def load_catalog() -> DatasetCatalog:
    catalog = DatasetCatalog()
    try:
        with open(CATALOG_PATH, encoding="utf-8") as file_handle:
            records = json.load(file_handle)
        for record in records:
            catalog.register(DatasetMetadata.model_validate(record))
    except FileNotFoundError:
        pass  # Start with empty catalog on first run
    return catalog
```

---

## Troubleshooting FAQ

**Q: `aumai-datacommons` command not found after pip install.**

Make sure your Python scripts directory is on your `PATH`. With virtual environments this is usually automatic. Try `python -m aumai_datacommons.cli --help` as a fallback.

---

**Q: `DatasetNotFoundError: Dataset 'my-id' not found.`**

The catalog is in-memory. If you registered a dataset in a previous process, that state is gone unless you persisted and reloaded the catalog. See Pattern 5 above.

---

**Q: Validation reports errors on a file I know is correct.**

Check that your schema JSON uses Python type name strings exactly: `"str"`, `"int"`, `"float"`, `"bool"`, `"list"`, `"dict"`. Type names like `"string"`, `"integer"`, or `"array"` are silently skipped (no check performed). Also verify the file encoding is UTF-8.

---

**Q: `compute_statistics` returns an error dict instead of stats.**

The file path does not exist or is not readable. The function returns `{"error": "File not found: ..."}` rather than raising an exception. Check the path and permissions.

---

**Q: How do I handle Parquet or Arrow files?**

`DatasetFormat.parquet` and `DatasetFormat.arrow` are supported as metadata values, but `DatasetValidator.compute_statistics` and `validate_schema` only parse JSONL and CSV. For Parquet/Arrow, compute your statistics externally (e.g., with `pyarrow` or `pandas`) and pass the results to `DatasetMetadata` directly.

---

**Q: `DatasetMetadata` validation fails with a Pydantic error.**

The most common causes are: `size_bytes` or `num_records` set to a negative value (both have `ge=0`), `format` set to a string not in `["jsonl", "csv", "parquet", "arrow"]`, or `dataset_id` / `name` / `description` missing entirely. Check the error message — Pydantic's output is precise about which field failed and why.

---

## Next Steps

- [API Reference](api-reference.md) — complete documentation of every class, method, and model field
- [Examples](../examples/quickstart.py) — runnable quickstart script
- [Contributing](../CONTRIBUTING.md) — how to submit improvements
- [Discord community](https://discord.gg/aumai) — questions and discussion
