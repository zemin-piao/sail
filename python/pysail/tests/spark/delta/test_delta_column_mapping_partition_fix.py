"""
Regression tests for the metadata-loss bug in ``promote_field_types``.

Root cause
----------
``DeltaTypeConverter::promote_field_types`` in ``conversion/type_promotion.rs``
constructed the merged ``Field`` with ``Field::new(name, type, nullable)`` in
*both* return paths.  ``Field::new`` does NOT carry over Arrow field metadata, so
``delta.columnMapping.physicalName`` (and every other field-level annotation) was
silently dropped for every field that already existed in the table schema whenever
``mergeSchema`` was used.

Effect (demonstrable before/after the fix)
------------------------------------------
Arrow ``Field`` equality includes metadata.  When ``mergeSchema=true`` is used and
the schema has *not actually changed* (same columns, same types), the merged Arrow
schema contained fields with **empty** metadata while the table's Arrow schema
contained fields **with** physicalName metadata.  The inequality caused the code in
``handle_schema_evolution`` to take the "schema changed" branch and write a
**spurious ``metaData`` action** to the Delta log even though nothing logically
changed.

Fix
---
Both return sites in ``promote_field_types`` now call
``.with_metadata(table_field.metadata().clone())`` so that the existing table
field metadata is always preserved in the result.

Tests in this file
------------------
*Tests that directly prove the before/after difference*

1. ``test_merge_schema_append_same_schema_no_spurious_metadata``
   FAILS before the fix (a spurious ``metaData`` action is written to commit 2),
   PASSES after the fix (commit 2 contains only ``add`` actions).

*Regression / correctness tests (should PASS both before and after the fix)*

2. ``test_column_mapping_partition_physical_names_preserved_after_merge_schema``
   After a ``mergeSchema`` append that ADDS a new column the existing partition
   column must still use its *original* physical name in both Add.partitionValues
   and the filesystem partition directory path.

3. ``test_column_mapping_partition_physical_names_after_plain_append``
   A plain ``mode("append")`` to a column-mapped partitioned table uses physical
   names – unaffected by this specific fix, included as a regression baseline.

4. ``test_column_mapping_partition_physical_name_stable_after_merge_schema_new_col``
   After a ``mergeSchema`` that introduces a new column, the existing partition
   column must keep its *original* physical name (not receive a fresh uuid).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from pyspark.sql import Row

from pysail.testing.spark.utils.common import is_jvm_spark

if TYPE_CHECKING:
    pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _read_delta_log(log_dir: Path) -> list[list[dict]]:
    """Return a list of commits; each commit is a list of parsed JSON objects."""
    commits = []
    for log_file in sorted(log_dir.glob("*.json")):
        actions = []
        with log_file.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    actions.append(json.loads(line))
        commits.append(actions)
    return commits


def _physical_name_for_column(log_dir: Path, column: str) -> str | None:
    """Return the physicalName for *column* from the first commit's metaData."""
    commits = _read_delta_log(log_dir)
    for action in commits[0]:
        if "metaData" in action:
            schema = json.loads(action["metaData"]["schemaString"])
            for field in schema.get("fields", []):
                if field["name"] == column:
                    return field.get("metadata", {}).get("delta.columnMapping.physicalName")
    return None


def _has_metadata_action(commit_actions: list[dict]) -> bool:
    return any("metaData" in a for a in commit_actions)


def _add_partition_values(commit_actions: list[dict]) -> list[dict]:
    """Collect all partitionValues dicts from Add actions in a commit."""
    return [
        a["add"]["partitionValues"]
        for a in commit_actions
        if "add" in a and isinstance(a["add"].get("partitionValues"), dict)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.skipif(is_jvm_spark(), reason="Delta log structure assertions are Sail-only")
class TestColumnMappingPartitionFix:
    """
    Regression tests for metadata-loss in promote_field_types and its effect on
    column-mapped partitioned tables.
    """

    def test_merge_schema_append_same_schema_no_spurious_metadata(
        self,
        spark,
        tmp_path: Path,
    ) -> None:
        """
        BUG: before the fix, a mergeSchema append with *no actual schema change*
        triggered a spurious ``metaData`` action because ``promote_field_types``
        dropped Arrow field metadata (incl. ``delta.columnMapping.physicalName``),
        making the merged schema appear different from the table schema.

        AFTER FIX: the second commit should contain only ``add`` actions and no
        ``metaData`` action.
        """
        base = tmp_path / "cm_no_spurious_meta"

        # Initial write: create table with column mapping mode=name
        df = spark.createDataFrame([Row(id=1, region="us"), Row(id=2, region="eu")])
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.columnMapping.mode", "name")
            .partitionBy("region")
            .save(str(base))
        )

        # Append with mergeSchema=true using the SAME schema (no new columns).
        df2 = spark.createDataFrame([Row(id=3, region="us")])
        (
            df2.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(str(base))
        )

        log_dir = base / "_delta_log"
        commits = _read_delta_log(log_dir)
        assert len(commits) >= 2, "expected at least 2 commits"  # noqa: PLR2004

        # The second commit (index 1) should NOT contain a metaData action
        # because the schema did not change.
        second_commit = commits[1]
        assert not _has_metadata_action(second_commit), (
            "second commit must not contain a spurious metaData action when "
            "mergeSchema is used with no actual schema change; "
            f"commit actions: {second_commit}"
        )

        # Verify data is still readable and correct
        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [
            {"id": 1, "region": "us"},
            {"id": 2, "region": "eu"},
            {"id": 3, "region": "us"},
        ]

    def test_column_mapping_partition_physical_names_preserved_after_merge_schema(
        self,
        spark,
        tmp_path: Path,
    ) -> None:
        """
        After a mergeSchema append that ADDS a new column, the existing partition
        column must still use its original physical name (col-<uuid>) in both the
        partition directory path and in Add.partitionValues.
        """
        base = tmp_path / "cm_part_phys_merge"

        # Create initial table
        df = spark.createDataFrame([Row(id=1, region="us"), Row(id=2, region="eu")])
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.columnMapping.mode", "name")
            .partitionBy("region")
            .save(str(base))
        )

        log_dir = base / "_delta_log"
        phys_region = _physical_name_for_column(log_dir, "region")
        assert phys_region is not None, "expected delta.columnMapping.physicalName for 'region'"
        assert phys_region != "region", "physicalName must differ from logical name"

        # Append with a NEW column via mergeSchema
        df2 = spark.createDataFrame([Row(id=3, region="us", score=99)])
        (
            df2.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(str(base))
        )

        commits = _read_delta_log(log_dir)

        # Collect partitionValues from all Add actions across all commits
        all_pv: list[dict] = []
        for commit in commits:
            all_pv.extend(_add_partition_values(commit))

        assert all_pv, "expected at least one Add action with partitionValues"

        for pv in all_pv:
            assert "region" not in pv, (
                f"Add.partitionValues must not use logical key 'region'; got {pv}"
            )
            assert phys_region in pv, (
                f"Add.partitionValues must use physical key {phys_region!r}; got {pv}"
            )

        # Verify partition directories use physical names
        parquet_files = list(base.glob("**/*.parquet"))
        assert parquet_files, "expected parquet files to be written"
        for pf in parquet_files:
            parts = pf.relative_to(base).parts
            # Each partition segment looks like "col-uuid=value"
            for part in parts[:-1]:  # exclude the parquet filename itself
                col_part, _, _ = part.partition("=")
                assert col_part != "region", (
                    f"partition directory segment {part!r} must not use logical name "
                    f"'region'; expected physical name {phys_region!r}"
                )
                assert col_part == phys_region, (
                    f"partition directory segment {part!r} must use physical name "
                    f"{phys_region!r}"
                )

        # Data should be fully readable
        out = (
            spark.read.format("delta")
            .load(str(base))
            .orderBy("id")
            .collect()
        )
        assert len(out) == 3  # noqa: PLR2004
        assert {r.region for r in out} == {"us", "eu"}

    def test_column_mapping_partition_physical_names_after_plain_append(
        self,
        spark,
        tmp_path: Path,
    ) -> None:
        """
        Sanity check: a plain append (no mergeSchema) must also use physical names
        for partition directories and partitionValues.
        """
        base = tmp_path / "cm_part_phys_plain"

        df = spark.createDataFrame([Row(id=1, region="us")])
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.columnMapping.mode", "name")
            .partitionBy("region")
            .save(str(base))
        )

        log_dir = base / "_delta_log"
        phys_region = _physical_name_for_column(log_dir, "region")
        assert phys_region is not None
        assert phys_region != "region"

        df2 = spark.createDataFrame([Row(id=2, region="eu")])
        df2.write.format("delta").mode("append").save(str(base))

        commits = _read_delta_log(log_dir)
        all_pv: list[dict] = []
        for commit in commits:
            all_pv.extend(_add_partition_values(commit))

        for pv in all_pv:
            assert "region" not in pv, f"must not use logical name 'region'; got {pv}"
            assert phys_region in pv, f"must use physical name {phys_region!r}; got {pv}"

        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert [r.asDict() for r in out] == [{"id": 1, "region": "us"}, {"id": 2, "region": "eu"}]

    def test_column_mapping_partition_physical_name_stable_after_merge_schema_new_col(
        self,
        spark,
        tmp_path: Path,
    ) -> None:
        """
        When mergeSchema adds a new column the *existing* partition column must
        keep its ORIGINAL physical name (not be re-annotated with a fresh uuid),
        and subsequent appends must write to the same physical partition directory.
        """
        base = tmp_path / "cm_part_stable_phys"

        df = spark.createDataFrame([Row(id=1, region="us")])
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("delta.columnMapping.mode", "name")
            .partitionBy("region")
            .save(str(base))
        )

        log_dir = base / "_delta_log"
        phys_region_before = _physical_name_for_column(log_dir, "region")
        assert phys_region_before is not None

        # mergeSchema with a new column
        df2 = spark.createDataFrame([Row(id=2, region="us", extra="x")])
        (
            df2.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(str(base))
        )

        # The physicalName for region must not have changed after schema evolution
        commits = _read_delta_log(log_dir)
        phys_region_after: str | None = None
        for commit in reversed(commits):
            for action in commit:
                if "metaData" in action:
                    schema = json.loads(action["metaData"]["schemaString"])
                    for field in schema.get("fields", []):
                        if field["name"] == "region":
                            phys_region_after = field.get("metadata", {}).get(
                                "delta.columnMapping.physicalName"
                            )
                    if phys_region_after is not None:
                        break
            if phys_region_after is not None:
                break

        if phys_region_after is not None:
            assert phys_region_after == phys_region_before, (
                f"physicalName for 'region' changed from {phys_region_before!r} "
                f"to {phys_region_after!r} after mergeSchema — physical names must be stable"
            )

        # All Add actions (both commits) should use the same physical partition key
        all_pv: list[dict] = []
        for commit in commits:
            all_pv.extend(_add_partition_values(commit))

        for pv in all_pv:
            assert phys_region_before in pv, (
                f"expected physical key {phys_region_before!r} in partitionValues; got {pv}"
            )

        # Verify data integrity
        out = spark.read.format("delta").load(str(base)).orderBy("id").collect()
        assert len(out) == 2  # noqa: PLR2004
        assert out[0].id == 1
        assert out[1].id == 2  # noqa: PLR2004
