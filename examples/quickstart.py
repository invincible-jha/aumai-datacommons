"""Quickstart examples for aumai-datacommons.

Demonstrates the core dataset catalog, validation, versioning, and SHA-256
integrity verification APIs using in-memory data (no external files required).

Run this file directly to verify your installation:

    python examples/quickstart.py

Each demo function is self-contained and prints its own output.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

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


# ---------------------------------------------------------------------------
# Demo 1: Register datasets and search the catalog
# ---------------------------------------------------------------------------


def demo_catalog_search() -> None:
    """Show how to register datasets and search by text, format, and tags."""
    print("=" * 60)
    print("DEMO 1: Dataset Catalog — Register and Search")
    print("=" * 60)

    catalog = DatasetCatalog()

    # Register three datasets with different formats and tags
    datasets = [
        DatasetMetadata(
            dataset_id="agent-traces-001",
            name="Agent Execution Traces",
            description="ReAct-style agent traces for tool-use fine-tuning.",
            format=DatasetFormat.jsonl,
            size_bytes=1_048_576,
            num_records=5_000,
            license="CC-BY-4.0",
            tags=["agents", "tool-use", "react", "fine-tuning"],
            version="1.0.0",
        ),
        DatasetMetadata(
            dataset_id="rag-eval-bench-001",
            name="RAG Evaluation Benchmark",
            description="500 curated question-answer pairs for RAG pipeline evaluation.",
            format=DatasetFormat.jsonl,
            size_bytes=204_800,
            num_records=500,
            license="Apache-2.0",
            tags=["rag", "evaluation", "benchmark", "qa"],
            version="2.1.0",
        ),
        DatasetMetadata(
            dataset_id="instruction-csv-001",
            name="Instruction Following CSV",
            description="Instruction-output pairs in CSV format for SFT experiments.",
            format=DatasetFormat.csv,
            size_bytes=512_000,
            num_records=10_000,
            license="MIT",
            tags=["instruction", "sft", "fine-tuning"],
            version="1.0.0",
        ),
    ]

    for dataset in datasets:
        catalog.register(dataset)
        print(f"  Registered: [{dataset.dataset_id}] {dataset.name}")

    print()

    # Text search across name and description
    results = catalog.search("evaluation")
    print(f"  Search 'evaluation' → {len(results)} result(s):")
    for item in results:
        print(f"    [{item.dataset_id}] {item.name}")

    # Filter by format
    jsonl_results = catalog.search("", dataset_format=DatasetFormat.jsonl)
    print(f"\n  Filter format=jsonl → {len(jsonl_results)} result(s)")

    # Filter by tags (all must match)
    tagged = catalog.search("", tags=["fine-tuning"])
    print(f"  Filter tag='fine-tuning' → {len(tagged)} result(s):")
    for item in tagged:
        print(f"    [{item.dataset_id}]")

    # Paginated listing
    page = catalog.list_all(limit=2, offset=0)
    print(f"\n  list_all(limit=2, offset=0) → {len(page)} items")

    # Direct get
    fetched = catalog.get("rag-eval-bench-001")
    print(f"\n  get('rag-eval-bench-001') → {fetched.name} v{fetched.version}")

    # Expect DatasetNotFoundError for missing dataset
    try:
        catalog.get("does-not-exist")
    except DatasetNotFoundError as exc:
        print(f"\n  DatasetNotFoundError caught: {exc}")

    print()


# ---------------------------------------------------------------------------
# Demo 2: Validate a JSONL file and compute statistics
# ---------------------------------------------------------------------------


def demo_validation_and_stats() -> None:
    """Show schema validation and statistics computation on a JSONL file."""
    print("=" * 60)
    print("DEMO 2: Dataset Validation and Statistics")
    print("=" * 60)

    validator = DatasetValidator()

    # Create a temporary JSONL file with valid and invalid records
    records = [
        {"prompt": "What is the capital of France?", "label": "Paris", "score": 1.0},
        {"prompt": "Name a prime number.", "label": "7", "score": 0.9},
        # Intentionally broken: score is a string instead of float
        {"prompt": "List AI companies.", "label": "OpenAI, Anthropic", "score": "high"},
        {"prompt": "Translate 'hello' to Spanish.", "label": "hola", "score": 1.0},
    ]

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name
        for record in records:
            temp_file.write(json.dumps(record) + "\n")

    schema = {"prompt": "str", "label": "str", "score": "float"}

    # Validate
    errors = validator.validate_schema(temp_path, schema)
    if errors:
        print(f"  Validation errors found ({len(errors)}):")
        for error in errors:
            print(f"    - {error}")
    else:
        print("  Validation passed — no errors.")

    # Statistics
    stats = validator.compute_statistics(temp_path)
    print(f"\n  Statistics for {Path(temp_path).name}:")
    print(f"    row_count: {stats['row_count']}")
    print(f"    null_counts: {stats['null_counts']}")
    print(f"    type_distribution:")
    for field, types in stats["type_distribution"].items():  # type: ignore[union-attr]
        print(f"      {field}: {types}")

    # Clean up
    Path(temp_path).unlink()
    print()


# ---------------------------------------------------------------------------
# Demo 3: Version tracking
# ---------------------------------------------------------------------------


def demo_version_tracking() -> None:
    """Show automatic semantic version increment and history retrieval."""
    print("=" * 60)
    print("DEMO 3: Dataset Version Tracking")
    print("=" * 60)

    manager = DatasetVersionManager()
    dataset_id = "agent-traces-001"

    change_log = [
        "Initial release. 5,000 examples covering factual Q&A and tool-use tasks.",
        "Added 2,000 multi-hop tool-chain examples. Removed 50 exact duplicates.",
        "Fixed label encoding bug in 'final_answer' field. Added language tag metadata.",
        "Expanded to 8,500 examples. Added multilingual subset (French, Spanish, Hindi).",
    ]

    print(f"  Creating version history for dataset '{dataset_id}':")
    for change_description in change_log:
        version_entry: DatasetVersion = manager.create_version(dataset_id, change_description)
        print(f"    v{version_entry.version}: {change_description[:55]}...")

    print(f"\n  Full version history ({len(manager.list_versions(dataset_id))} entries):")
    for entry in manager.list_versions(dataset_id):
        print(f"    [{entry.version}] {entry.created_at.strftime('%Y-%m-%d %H:%M')} UTC")
        print(f"           {entry.changes[:70]}")

    print()


# ---------------------------------------------------------------------------
# Demo 4: SHA-256 integrity verification
# ---------------------------------------------------------------------------


def demo_integrity_verification() -> None:
    """Show SHA-256 file hashing and DownloadResult construction."""
    print("=" * 60)
    print("DEMO 4: File Integrity Verification (SHA-256)")
    print("=" * 60)

    # Write a sample file to hash
    sample_content = "\n".join(
        json.dumps({"prompt": f"Example {i}", "label": f"Answer {i}"})
        for i in range(100)
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name
        temp_file.write(sample_content)

    # Compute hash of the "original" file
    original_digest = compute_sha256(temp_path)
    print(f"  SHA-256 of original file: {original_digest[:32]}...")

    # Simulate a verified download
    download_result = DownloadResult(
        dataset_id="agent-traces-001",
        path=temp_path,
        verified=(compute_sha256(temp_path) == original_digest),
        sha256=original_digest,
    )
    print(f"  Download verified: {download_result.verified}")
    print(f"  Stored path: {Path(download_result.path).name}")

    # Simulate a tampered download
    with open(temp_path, "a", encoding="utf-8") as file_handle:
        file_handle.write('\n{"prompt": "injected", "label": "tampered"}\n')

    tampered_digest = compute_sha256(temp_path)
    tampered_result = DownloadResult(
        dataset_id="agent-traces-001",
        path=temp_path,
        verified=(tampered_digest == original_digest),
        sha256=tampered_digest,
    )
    print(f"\n  After tampering — verified: {tampered_result.verified}")
    print(f"  Original digest: {original_digest[:32]}...")
    print(f"  Tampered digest: {tampered_digest[:32]}...")

    Path(temp_path).unlink()
    print()


# ---------------------------------------------------------------------------
# Demo 5: Full workflow — validate, register, version, persist
# ---------------------------------------------------------------------------


def demo_full_workflow() -> None:
    """End-to-end workflow: validate a dataset, register it, create a version."""
    print("=" * 60)
    print("DEMO 5: Full Workflow — Validate, Register, Version")
    print("=" * 60)

    # Write sample JSONL
    records = [
        {"instruction": f"Task {i}", "response": f"Answer {i}", "quality": 5}
        for i in range(20)
    ]
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    ) as temp_file:
        temp_path = temp_file.name
        for rec in records:
            temp_file.write(json.dumps(rec) + "\n")

    schema = {"instruction": "str", "response": "str", "quality": "int"}

    validator = DatasetValidator()
    catalog = DatasetCatalog()
    manager = DatasetVersionManager()

    # Step 1: Validate
    errors = validator.validate_schema(temp_path, schema)
    if errors:
        print(f"  Validation failed: {errors}")
        Path(temp_path).unlink()
        return
    print("  Step 1: Validation passed.")

    # Step 2: Compute stats and hash
    stats = validator.compute_statistics(temp_path)
    file_hash = compute_sha256(temp_path)
    print(f"  Step 2: Stats computed. Rows={stats['row_count']}, SHA256={file_hash[:16]}...")

    # Step 3: Register in catalog
    metadata = DatasetMetadata(
        dataset_id="instruct-quality-001",
        name="Instruction Quality Dataset",
        description="High-quality instruction-response pairs with human quality scores.",
        format=DatasetFormat.jsonl,
        size_bytes=Path(temp_path).stat().st_size,
        num_records=stats["row_count"],  # type: ignore[arg-type]
        schema=schema,
        license="Apache-2.0",
        tags=["instruction", "quality", "sft"],
    )
    catalog.register(metadata)
    print(f"  Step 3: Registered '{metadata.dataset_id}' in catalog.")

    # Step 4: Create initial version
    version = manager.create_version(
        metadata.dataset_id,
        "Initial release. 20 examples with human quality scores.",
    )
    print(f"  Step 4: Version created: v{version.version}")

    # Step 5: Verify round-trip via JSON serialization
    serialized = metadata.model_dump(mode="json")
    restored = DatasetMetadata.model_validate(serialized)
    assert restored.dataset_id == metadata.dataset_id
    print("  Step 5: JSON round-trip serialization verified.")

    Path(temp_path).unlink()
    print("\n  Full workflow complete.")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run all quickstart demos."""
    print("\naumai-datacommons Quickstart Examples")
    print("Version:", end=" ")
    import aumai_datacommons
    print(aumai_datacommons.__version__)
    print()

    demo_catalog_search()
    demo_validation_and_stats()
    demo_version_tracking()
    demo_integrity_verification()
    demo_full_workflow()

    print("All demos completed successfully.")


if __name__ == "__main__":
    main()
