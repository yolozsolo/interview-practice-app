"""PySpark and Delta Lake traceability challenge.

Title: Traceability Lineage, Auditability, and Compliance Risk in PySpark
Duration: 2-3 hours stated, 4-7 hours realistic senior practice
Difficulty: Senior / Lead data engineering
Topics tested:
- pyspark.dataframes
- pyspark.window_functions
- pyspark.joins
- pyspark.data_quality
- lineage_modeling
- compliance_risk_propagation
- auditability
- delta_lake.merge
- retry_safety
- idempotency

Scenario:
A supply-chain traceability platform ingests origin, processing, shipment, and
regulatory records from multiple source systems. The company needs a small
Spark pipeline that normalizes source data, rejects invalid rows, deduplicates
corrections, builds lineage edges, propagates compliance risk downstream, and
produces an explainable audit report for finished product batches.

Tasks:
1. Load small in-memory starter data into explicitly typed PySpark DataFrames.
2. Normalize and validate origin, batch-event, shipment, and declaration data.
3. Deduplicate records deterministically using source version and ingest time.
4. Build transformation and shipment lineage edges.
5. Detect attribute conflicts across source systems.
6. Propagate high-risk origin flags to downstream product batches.
7. Build an audit-friendly report explaining risk, conflicts, and declarations.
8. Practice small local Delta Lake writes, append-only lineage, and retry-safe
   silver upserts with Delta MERGE.

Evaluation focus:
Correct PySpark API usage, deterministic behavior, rejection handling, lineage
thinking, audit explainability, Delta idempotency, and practical senior-level
written reasoning about production tradeoffs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F


# =========================
# CHALLENGE DESCRIPTION
# =========================

# Implement the TODOs in the IMPLEMENTATION AREA and the optional DELTA LAKE
# IMPLEMENTATION AREA. Use PySpark DataFrame APIs for the main transformations.
# Avoid pandas, plain-Python row loops for business logic, GraphFrames, cloud
# dependencies, Databricks-only APIs, and streaming.
#
# Important business concepts:
# - source_record_id identifies a source-system record.
# - source_version identifies source corrections.
# - ingest_ts is the platform ingestion time and may arrive out of event order.
# - origin_id, farm_id, plot_id, event entity ids, shipment ids, and declaration
#   entity ids are business identifiers.
# - A deterministic pipeline must produce the same output even if input order
#   changes.
# - High-risk origin compliance issues should be visible downstream in product
#   audit summaries.
# - Missing critical fields should be rejected into quarantine with reasons.
# - Missing non-critical fields should survive normalization with explicit nulls.
#
# Suggested normalized entity ids:
# - Origins: ORIGIN:<origin_id>
# - Farm lots, processing batches, shipments, and products may already arrive as
#   stable input_entity_id / output_entity_id values in event data.
#
# Bounded lineage guidance:
# - Do not use recursive graph libraries.
# - The starter data is intentionally small enough for iterative Spark joins or
#   bounded-depth joins.
# - A practical solution can propagate origin risk across a few downstream hops.


# =========================
# GIVEN / STARTER DATA
# =========================

RAW_ORIGIN_SCHEMA = T.StructType(
    [
        T.StructField("source_system", T.StringType(), True),
        T.StructField("source_record_id", T.StringType(), True),
        T.StructField("source_version", T.IntegerType(), True),
        T.StructField("ingest_ts", T.StringType(), True),
        T.StructField("origin_id", T.StringType(), True),
        T.StructField("farm_id", T.StringType(), True),
        T.StructField("plot_id", T.StringType(), True),
        T.StructField("country", T.StringType(), True),
        T.StructField("geo_risk_level", T.StringType(), True),
        T.StructField("certification_status", T.StringType(), True),
    ]
)

RAW_BATCH_EVENT_SCHEMA = T.StructType(
    [
        T.StructField("source_system", T.StringType(), True),
        T.StructField("source_record_id", T.StringType(), True),
        T.StructField("source_version", T.IntegerType(), True),
        T.StructField("ingest_ts", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("input_entity_id", T.StringType(), True),
        T.StructField("output_entity_id", T.StringType(), True),
        T.StructField("quantity_kg", T.DoubleType(), True),
        T.StructField("event_ts", T.StringType(), True),
    ]
)

RAW_SHIPMENT_SCHEMA = T.StructType(
    [
        T.StructField("source_system", T.StringType(), True),
        T.StructField("source_record_id", T.StringType(), True),
        T.StructField("source_version", T.IntegerType(), True),
        T.StructField("ingest_ts", T.StringType(), True),
        T.StructField("shipment_id", T.StringType(), True),
        T.StructField("from_entity_id", T.StringType(), True),
        T.StructField("to_entity_id", T.StringType(), True),
        T.StructField("carrier", T.StringType(), True),
        T.StructField("shipment_ts", T.StringType(), True),
    ]
)

RAW_DECLARATION_SCHEMA = T.StructType(
    [
        T.StructField("source_system", T.StringType(), True),
        T.StructField("source_record_id", T.StringType(), True),
        T.StructField("source_version", T.IntegerType(), True),
        T.StructField("ingest_ts", T.StringType(), True),
        T.StructField("entity_id", T.StringType(), True),
        T.StructField("declaration_type", T.StringType(), True),
        T.StructField("declaration_status", T.StringType(), True),
        T.StructField("declared_by", T.StringType(), True),
        T.StructField("declaration_ts", T.StringType(), True),
    ]
)

RAW_ORIGIN_RECORDS: list[dict[str, Any]] = [
    {
        "source_system": "geo_registry",
        "source_record_id": "origin-001-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-30T08:00:00Z",
        "origin_id": "origin-001",
        "farm_id": "farm-001",
        "plot_id": "plot-a1",
        "country": "GH",
        "geo_risk_level": "none",
        "certification_status": "certified",
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "origin-001-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-30T08:00:00Z",
        "origin_id": "origin-001",
        "farm_id": "farm-001",
        "plot_id": "plot-a1",
        "country": "GH",
        "geo_risk_level": "none",
        "certification_status": "certified",
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "origin-002-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-30T08:03:00Z",
        "origin_id": "origin-002",
        "farm_id": "farm-001",
        "plot_id": "plot-a2",
        "country": "GH",
        "geo_risk_level": "review",
        "certification_status": "certified",
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "origin-002-geo",
        "source_version": 2,
        "ingest_ts": "2026-05-30T09:15:00Z",
        "origin_id": "origin-002",
        "farm_id": "farm-001",
        "plot_id": "plot-a2",
        "country": "GH",
        "geo_risk_level": "high",
        "certification_status": "suspended",
    },
    {
        "source_system": "certification_portal",
        "source_record_id": "origin-002-cert",
        "source_version": 1,
        "ingest_ts": "2026-05-30T09:20:00Z",
        "origin_id": "origin-002",
        "farm_id": "farm-001",
        "plot_id": "plot-a2",
        "country": "CI",
        "geo_risk_level": "none",
        "certification_status": "certified",
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "origin-003-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-30T08:10:00Z",
        "origin_id": "origin-003",
        "farm_id": "farm-009",
        "plot_id": None,
        "country": "EC",
        "geo_risk_level": "none",
        "certification_status": "certified",
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "origin-invalid-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-30T08:11:00Z",
        "origin_id": None,
        "farm_id": "farm-404",
        "plot_id": "plot-z9",
        "country": "GH",
        "geo_risk_level": "none",
        "certification_status": "certified",
    },
]

RAW_BATCH_EVENTS: list[dict[str, Any]] = [
    {
        "source_system": "factory_mes",
        "source_record_id": "evt-100",
        "source_version": 1,
        "ingest_ts": "2026-05-30T10:30:00Z",
        "event_type": "roast",
        "input_entity_id": "ORIGIN:ORIGIN-001",
        "output_entity_id": "LOT:LOT-100",
        "quantity_kg": 120.0,
        "event_ts": "2026-05-29T10:00:00Z",
    },
    {
        "source_system": "factory_mes",
        "source_record_id": "evt-101",
        "source_version": 1,
        "ingest_ts": "2026-05-30T10:20:00Z",
        "event_type": "roast",
        "input_entity_id": "ORIGIN:ORIGIN-002",
        "output_entity_id": "LOT:LOT-100",
        "quantity_kg": 80.0,
        "event_ts": "2026-05-29T10:05:00Z",
    },
    {
        "source_system": "factory_mes",
        "source_record_id": "evt-102",
        "source_version": 1,
        "ingest_ts": "2026-05-30T11:40:00Z",
        "event_type": "blend",
        "input_entity_id": "LOT:LOT-100",
        "output_entity_id": "BATCH:BATCH-55",
        "quantity_kg": 190.0,
        "event_ts": "2026-05-29T11:00:00Z",
    },
    {
        "source_system": "factory_mes",
        "source_record_id": "evt-103",
        "source_version": 1,
        "ingest_ts": "2026-05-30T09:50:00Z",
        "event_type": "pack",
        "input_entity_id": "BATCH:BATCH-55",
        "output_entity_id": "PRODUCT:CHOC-2026-05-A",
        "quantity_kg": 185.0,
        "event_ts": "2026-05-29T12:00:00Z",
    },
    {
        "source_system": "factory_mes",
        "source_record_id": "evt-104",
        "source_version": 1,
        "ingest_ts": "2026-05-30T12:00:00Z",
        "event_type": "pack",
        "input_entity_id": "BATCH:BATCH-55",
        "output_entity_id": "PRODUCT:CHOC-2026-05-B",
        "quantity_kg": 40.0,
        "event_ts": "2026-05-29T12:05:00Z",
    },
    {
        "source_system": "factory_mes",
        "source_record_id": "evt-invalid",
        "source_version": 1,
        "ingest_ts": "2026-05-30T12:10:00Z",
        "event_type": "blend",
        "input_entity_id": "LOT:LOT-404",
        "output_entity_id": None,
        "quantity_kg": 10.0,
        "event_ts": "2026-05-29T12:10:00Z",
    },
]

RAW_SHIPMENTS: list[dict[str, Any]] = [
    {
        "source_system": "shipment_tms",
        "source_record_id": "ship-900",
        "source_version": 1,
        "ingest_ts": "2026-05-30T13:10:00Z",
        "shipment_id": "shipment-900",
        "from_entity_id": "PRODUCT:CHOC-2026-05-A",
        "to_entity_id": "DC:EU-1",
        "carrier": "TransCocoa",
        "shipment_ts": "2026-05-30T13:00:00Z",
    },
    {
        "source_system": "shipment_tms",
        "source_record_id": "ship-900",
        "source_version": 2,
        "ingest_ts": "2026-05-30T14:10:00Z",
        "shipment_id": "shipment-900",
        "from_entity_id": "PRODUCT:CHOC-2026-05-A",
        "to_entity_id": "DC:EU-2",
        "carrier": "TransCocoa",
        "shipment_ts": "2026-05-30T13:00:00Z",
    },
    {
        "source_system": "shipment_tms",
        "source_record_id": "ship-901",
        "source_version": 1,
        "ingest_ts": "2026-05-30T13:20:00Z",
        "shipment_id": "shipment-901",
        "from_entity_id": "PRODUCT:CHOC-2026-05-B",
        "to_entity_id": "DC:EU-1",
        "carrier": None,
        "shipment_ts": "2026-05-30T13:05:00Z",
    },
]

RAW_REGULATORY_DECLARATIONS: list[dict[str, Any]] = [
    {
        "source_system": "regulatory_portal",
        "source_record_id": "decl-001",
        "source_version": 1,
        "ingest_ts": "2026-05-30T15:00:00Z",
        "entity_id": "PRODUCT:CHOC-2026-05-A",
        "declaration_type": "EUDR",
        "declaration_status": "approved",
        "declared_by": "compliance-team",
        "declaration_ts": "2026-05-30T14:45:00Z",
    },
    {
        "source_system": "regulatory_portal",
        "source_record_id": "decl-002",
        "source_version": 1,
        "ingest_ts": "2026-05-30T15:05:00Z",
        "entity_id": "PRODUCT:CHOC-2026-05-B",
        "declaration_type": "EUDR",
        "declaration_status": "approved",
        "declared_by": "compliance-team",
        "declaration_ts": "2026-05-30T14:50:00Z",
    },
    {
        "source_system": "regulatory_portal",
        "source_record_id": "decl-003",
        "source_version": 1,
        "ingest_ts": "2026-05-30T15:10:00Z",
        "entity_id": "ORIGIN:ORIGIN-002",
        "declaration_type": "origin_attestation",
        "declaration_status": "rejected",
        "declared_by": "geo-risk-team",
        "declaration_ts": "2026-05-30T14:55:00Z",
    },
    {
        "source_system": "regulatory_portal",
        "source_record_id": "decl-invalid",
        "source_version": 1,
        "ingest_ts": "2026-05-30T15:11:00Z",
        "entity_id": None,
        "declaration_type": "EUDR",
        "declaration_status": "approved",
        "declared_by": "compliance-team",
        "declaration_ts": "2026-05-30T15:00:00Z",
    },
]


def create_spark_session() -> SparkSession:
    """Create a small local Spark session for the challenge.

    The Delta section may need additional Delta configuration depending on your
    local `delta-spark` version. Keep this helper small and local-only.
    """
    return (
        SparkSession.builder.master("local[2]")
        .appName("traceability-lineage-pyspark-delta-practice")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def load_starter_data(spark: SparkSession) -> dict[str, DataFrame]:
    """Load raw starter data into explicitly typed Spark DataFrames."""
    return {
        "raw_origin_records": spark.createDataFrame(
            RAW_ORIGIN_RECORDS, schema=RAW_ORIGIN_SCHEMA
        ),
        "raw_batch_events": spark.createDataFrame(
            RAW_BATCH_EVENTS, schema=RAW_BATCH_EVENT_SCHEMA
        ),
        "raw_shipments": spark.createDataFrame(RAW_SHIPMENTS, schema=RAW_SHIPMENT_SCHEMA),
        "raw_regulatory_declarations": spark.createDataFrame(
            RAW_REGULATORY_DECLARATIONS, schema=RAW_DECLARATION_SCHEMA
        ),
    }


# =========================
# IMPLEMENTATION AREA
# =========================

def is_missing(column_name: str) -> Column:
    return F.col(column_name).isNull() | (F.trim(F.col(column_name)) == "")

def reject_invalid_records(
    records: DataFrame, required_columns: list[str]
) -> tuple[DataFrame, DataFrame]:
    """Split a DataFrame into valid rows and rejected rows.

    - Treat null or blank required columns as invalid.
    - Return `(valid_records, rejected_records)`.
    - Include `rejection_reason` on rejected records.
    - Preserve enough original columns for quarantine investigation.
    - Use DataFrame APIs, not collection to Python.
    """
    df = records
    
    reason_columns = [
        F.when(is_missing(column), F.lit(f"missing_{column}"))
        for column in required_columns
    ]

    validated_df = df.withColumn(
        "rejection_reasons",
        F.filter(
            F.array(*reason_columns), 
            lambda reason: reason.isNotNull()
        )
    ).withColumn(
        "is_valid",
        F.size(F.col("rejection_reasons")) == 0
    )

    valid_records = validated_df.filter(F.col("is_valid"))
    rejected_records = validated_df.filter(~F.col("is_valid"))
        
    return (valid_records, rejected_records)

def deduplicate_latest(records: DataFrame, business_keys: list[str]) -> DataFrame:
    """Keep the deterministic latest row per business key.

    TODO:
    - Use a window with `row_number`.
    - Partition by `business_keys`.
    - Order by source_version descending, ingest timestamp descending, and a
      deterministic tie-breaker.
    - Drop exact duplicate source rows before ranking if useful.
    """
    raise NotImplementedError


def normalize_origins(raw_origins: DataFrame) -> DataFrame:
    """Normalize origin records into silver-ready origin entities.

    TODO:
    - Reject rows missing critical fields such as source_system,
      source_record_id, source_version, ingest_ts, and origin_id.
    - Add `entity_id` as `ORIGIN:<upper origin_id>`.
    - Normalize case and whitespace.
    - Parse `ingest_ts` to a timestamp column.
    - Preserve missing non-critical fields such as plot_id.
    - Deduplicate latest corrections per source record.
    - Return valid normalized rows. Rejection handling may be exposed through a
      companion function or audit output.
    """
    raise NotImplementedError


def normalize_batch_events(raw_events: DataFrame) -> DataFrame:
    """Normalize processing/transformation events.

    TODO:
    - Reject missing critical event fields.
    - Normalize event type and entity id strings.
    - Parse ingest_ts and event_ts.
    - Deduplicate source corrections.
    - Preserve out-of-order event timestamps; do not rely on input ordering.
    """
    raise NotImplementedError


def normalize_shipments(raw_shipments: DataFrame) -> DataFrame:
    """Normalize shipment movement records.

    TODO:
    - Reject missing shipment_id, from_entity_id, or to_entity_id.
    - Deduplicate latest shipment corrections.
    - Preserve missing non-critical carrier values.
    - Parse timestamps.
    """
    raise NotImplementedError


def normalize_regulatory_declarations(raw_declarations: DataFrame) -> DataFrame:
    """Normalize regulatory declarations.

    TODO:
    - Reject missing entity_id, declaration_type, or declaration_status.
    - Normalize status values.
    - Deduplicate latest declaration source records.
    - Keep enough fields to explain who declared what and when.
    """
    raise NotImplementedError


def build_lineage_edges(batch_events: DataFrame, shipments: DataFrame) -> DataFrame:
    """Build directed lineage edges from transformations and shipments.

    TODO:
    - Batch events produce edges from input_entity_id to output_entity_id.
    - Shipments produce edges from from_entity_id to to_entity_id.
    - Include edge_type, source_system, source_record_id, event/departure
      timestamp, quantity_kg when relevant, and an explanation field.
    - Deduplicate deterministic duplicate edges.
    """
    raise NotImplementedError


def detect_attribute_conflicts(normalized_origins: DataFrame) -> DataFrame:
    """Detect origin attribute conflicts across source systems.

    TODO:
    - Compare attributes such as country, geo_risk_level, and
      certification_status per entity_id across source systems.
    - Return one row per conflicting entity/attribute.
    - Include conflicting values, source systems, source_record_ids, and
      severity.
    """
    raise NotImplementedError


def propagate_compliance_risk(origins: DataFrame, lineage_edges: DataFrame) -> DataFrame:
    """Propagate high/review origin risk to downstream entities.

    TODO:
    - Start from origin entities and their geo/certification risk.
    - Use bounded-depth iterative joins or explicit hop joins.
    - Return downstream entity risk flags and upstream origin contributors.
    - High-risk origin OR rejected origin attestation should make downstream
      product batches visible as high risk.
    - Keep output deterministic.
    """
    raise NotImplementedError


def build_entity_audit_summary(
    origins: DataFrame,
    lineage_edges: DataFrame,
    conflicts: DataFrame,
    declarations: DataFrame,
    risk_propagation: DataFrame,
) -> DataFrame:
    """Build one explainable audit summary row per audited entity.

    TODO:
    - Include upstream origins.
    - Include source systems involved.
    - Include risk flags and highest propagated risk.
    - Include conflict flags and conflict details.
    - Include declaration status and declaration mismatches.
    - Include human-readable explanation fields suitable for audit review.
    """
    raise NotImplementedError


def produce_traceability_report(
    entity_id: str,
    origins: DataFrame,
    lineage_edges: DataFrame,
    conflicts: DataFrame,
    declarations: DataFrame,
    risk_propagation: DataFrame,
) -> DataFrame:
    """Return a compact report for a selected downstream entity.

    TODO:
    - Build or reuse `build_entity_audit_summary`.
    - Filter to the requested `entity_id`.
    - Include upstream origins, source systems, risk flags, conflicts,
      declaration status, and audit explanations.
    """
    raise NotImplementedError


# =========================
# DELTA LAKE IMPLEMENTATION AREA
# =========================


def prepare_delta_paths(base_path: str) -> dict[str, str]:
    """Return local Delta paths for bronze, silver, and lineage tables.

    TODO:
    - Keep all paths under `base_path`.
    - Use names such as bronze_origins, silver_origins, and lineage_edges.
    """
    raise NotImplementedError


def write_bronze_delta(df: DataFrame, path: str) -> None:
    """Write normalized bronze data as a local Delta table.

    TODO:
    - Use `format("delta")`.
    - Choose an appropriate mode for repeatable local practice.
    """
    raise NotImplementedError


def write_silver_delta_overwrite(df: DataFrame, path: str) -> None:
    """Write deduplicated silver data as a local Delta table.

    TODO:
    - Use an overwrite pattern suitable for this small practice table.
    - Keep the operation local-filesystem only.
    """
    raise NotImplementedError


def merge_silver_origins_delta(
    spark: SparkSession, updates: DataFrame, path: str
) -> None:
    """Retry-safe upsert into silver origins using DeltaTable.merge.

    TODO:
    - Import DeltaTable inside the function so non-Delta tests can import this
      file even if Delta is unavailable.
    - Match on the stable silver origin key.
    - Update only when the incoming source_version/ingest_ts wins.
    - Insert new keys.
    - Running the same merge twice must not create duplicates.
    """
    raise NotImplementedError


def append_lineage_edges_delta(edges: DataFrame, path: str) -> None:
    """Append lineage edges to an append-only local Delta table.

    TODO:
    - Append lineage edges.
    - Include deterministic edge ids in your implementation so retries can be
      handled by a later compaction/dedup process or by merge if you choose.
    """
    raise NotImplementedError


def read_delta_table(spark: SparkSession, path: str) -> DataFrame:
    """Read the current local Delta table."""
    raise NotImplementedError


def read_delta_version(spark: SparkSession, path: str, version: int) -> DataFrame:
    """Read a previous local Delta table version if available."""
    raise NotImplementedError


# =========================
# WRITTEN ANSWERS
# =========================

# Answer these after implementation:
#
# 1. Why is deterministic deduplication important in Spark jobs?
# ANSWER:
#
# 2. Why is idempotency important when using Delta MERGE?
# ANSWER:
#
# 3. Which columns would you partition by if this were stored in Delta Lake?
# ANSWER:
#
# 4. Which tables would you design for bronze, silver, and gold layers?
# ANSWER:
#
# 5. Which tables should be append-only and which should be upserted?
# ANSWER:
#
# 6. How would you handle late-arriving lineage events?
# ANSWER:
#
# 7. How would you detect and monitor broken lineage chains?
# ANSWER:
#
# 8. How would you explain auditability to a non-technical compliance
# stakeholder?
# ANSWER:
#
# 9. What Spark performance risks exist in this task?
# ANSWER:
#
# 10. Where could data skew appear?
# ANSWER:
#
# 11. How would schema evolution affect the bronze/silver/gold design?
# ANSWER:
#
# 12. What would change if this ran on Databricks with Unity Catalog?
# ANSWER:


# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    session = create_spark_session()
    yield session
    session.stop()


def test_spark_session_can_be_created_locally(spark: SparkSession) -> None:
    assert spark.range(1).count() == 1


def test_invalid_records_are_rejected_with_reasons(spark: SparkSession) -> None:
    data = load_starter_data(spark)

    valid, rejected = reject_invalid_records(
        data["raw_origin_records"],
        ["source_system", "source_record_id", "source_version", "ingest_ts", "origin_id"],
    )

    assert valid.count() == 6
    assert rejected.count() == 1
    assert "rejection_reasons" in rejected.columns


def test_deduplication_keeps_latest_deterministic_source_record(
    spark: SparkSession,
) -> None:
    data = load_starter_data(spark)
    valid, _ = reject_invalid_records(
        data["raw_origin_records"],
        ["source_system", "source_record_id", "source_version", "ingest_ts", "origin_id"],
    )

    deduped = deduplicate_latest(valid, ["source_system", "source_record_id"])
    latest = (
        deduped.filter(deduped.source_record_id == "origin-002-geo")
        .select("source_version", "geo_risk_level", "certification_status")
        .collect()
    )

    assert len(latest) == 1
    assert latest[0]["source_version"] == 2
    assert latest[0]["geo_risk_level"] == "high"
    assert latest[0]["certification_status"] == "suspended"


def test_out_of_order_events_do_not_break_lineage(spark: SparkSession) -> None:
    data = load_starter_data(spark)

    events = normalize_batch_events(data["raw_batch_events"])
    shipments = normalize_shipments(data["raw_shipments"])
    edges = build_lineage_edges(events, shipments)

    product_inputs = {
        row["parent_entity_id"]
        for row in edges.filter(edges.child_entity_id == "PRODUCT:CHOC-2026-05-A")
        .select("parent_entity_id")
        .collect()
    }

    assert product_inputs == {"BATCH:BATCH-55"}


def test_lineage_edges_are_built_from_events_and_shipments(
    spark: SparkSession,
) -> None:
    data = load_starter_data(spark)

    events = normalize_batch_events(data["raw_batch_events"])
    shipments = normalize_shipments(data["raw_shipments"])
    edges = build_lineage_edges(events, shipments)

    edge_pairs = {
        (row["parent_entity_id"], row["child_entity_id"])
        for row in edges.select("parent_entity_id", "child_entity_id").collect()
    }

    assert ("ORIGIN:ORIGIN-001", "LOT:LOT-100") in edge_pairs
    assert ("ORIGIN:ORIGIN-002", "LOT:LOT-100") in edge_pairs
    assert ("LOT:LOT-100", "BATCH:BATCH-55") in edge_pairs
    assert ("PRODUCT:CHOC-2026-05-A", "DC:EU-2") in edge_pairs


def test_high_risk_origin_propagates_to_downstream_products(
    spark: SparkSession,
) -> None:
    data = load_starter_data(spark)

    origins = normalize_origins(data["raw_origin_records"])
    events = normalize_batch_events(data["raw_batch_events"])
    shipments = normalize_shipments(data["raw_shipments"])
    edges = build_lineage_edges(events, shipments)
    risk = propagate_compliance_risk(origins, edges)

    products = {
        row["entity_id"]: row["highest_risk_level"]
        for row in risk.filter(risk.entity_id.startswith("PRODUCT:"))
        .select("entity_id", "highest_risk_level")
        .collect()
    }

    assert products["PRODUCT:CHOC-2026-05-A"] == "high"
    assert products["PRODUCT:CHOC-2026-05-B"] == "high"


def test_conflicting_origin_attributes_are_detected(spark: SparkSession) -> None:
    data = load_starter_data(spark)

    origins = normalize_origins(data["raw_origin_records"])
    conflicts = detect_attribute_conflicts(origins)
    conflict_rows = {
        (row["entity_id"], row["attribute_name"])
        for row in conflicts.select("entity_id", "attribute_name").collect()
    }

    assert ("ORIGIN:ORIGIN-002", "country") in conflict_rows
    assert ("ORIGIN:ORIGIN-002", "certification_status") in conflict_rows


def test_missing_or_invalid_declarations_are_visible_in_audit_summary(
    spark: SparkSession,
) -> None:
    data = load_starter_data(spark)

    origins = normalize_origins(data["raw_origin_records"])
    events = normalize_batch_events(data["raw_batch_events"])
    shipments = normalize_shipments(data["raw_shipments"])
    declarations = normalize_regulatory_declarations(
        data["raw_regulatory_declarations"]
    )
    edges = build_lineage_edges(events, shipments)
    conflicts = detect_attribute_conflicts(origins)
    risk = propagate_compliance_risk(origins, edges)
    audit = build_entity_audit_summary(origins, edges, conflicts, declarations, risk)

    rows = {
        row["entity_id"]: row
        for row in audit.filter(audit.entity_id.startswith("PRODUCT:")).collect()
    }

    assert rows["PRODUCT:CHOC-2026-05-A"]["declaration_status"] == "mismatch"
    assert rows["PRODUCT:CHOC-2026-05-B"]["declaration_status"] == "mismatch"


def test_final_traceability_report_contains_audit_fields(
    spark: SparkSession,
) -> None:
    data = load_starter_data(spark)

    origins = normalize_origins(data["raw_origin_records"])
    events = normalize_batch_events(data["raw_batch_events"])
    shipments = normalize_shipments(data["raw_shipments"])
    declarations = normalize_regulatory_declarations(
        data["raw_regulatory_declarations"]
    )
    edges = build_lineage_edges(events, shipments)
    conflicts = detect_attribute_conflicts(origins)
    risk = propagate_compliance_risk(origins, edges)

    report = produce_traceability_report(
        "PRODUCT:CHOC-2026-05-A",
        origins,
        edges,
        conflicts,
        declarations,
        risk,
    )

    row = report.collect()[0].asDict()

    assert "ORIGIN:ORIGIN-001" in row["upstream_origins"]
    assert "ORIGIN:ORIGIN-002" in row["upstream_origins"]
    assert "geo_registry" in row["source_systems"]
    assert row["highest_risk_level"] == "high"
    assert row["has_conflict"] is True
    assert row["declaration_status"] == "mismatch"
    assert row["audit_explanations"]


def test_result_is_deterministic_when_input_order_is_shuffled(
    spark: SparkSession,
) -> None:
    data = load_starter_data(spark)
    shuffled_origins = data["raw_origin_records"].orderBy("source_record_id")

    first = normalize_origins(data["raw_origin_records"]).orderBy(
        "entity_id", "source_system", "source_record_id"
    )
    second = normalize_origins(shuffled_origins).orderBy(
        "entity_id", "source_system", "source_record_id"
    )

    assert first.collect() == second.collect()


def _delta_available() -> bool:
    try:
        import delta  # noqa: F401
    except Exception:
        return False
    return True


@pytest.mark.skipif(not _delta_available(), reason="delta-spark is unavailable")
class TestOptionalDeltaLake:
    def test_bronze_normalized_origins_can_be_written_and_read(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        data = load_starter_data(spark)
        origins = normalize_origins(data["raw_origin_records"])
        paths = prepare_delta_paths(str(tmp_path))

        write_bronze_delta(origins, paths["bronze_origins"])
        read_back = read_delta_table(spark, paths["bronze_origins"])

        assert read_back.count() == origins.count()

    def test_silver_origins_can_be_written_as_delta(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        data = load_starter_data(spark)
        origins = normalize_origins(data["raw_origin_records"])
        paths = prepare_delta_paths(str(tmp_path))

        write_silver_delta_overwrite(origins, paths["silver_origins"])
        read_back = read_delta_table(spark, paths["silver_origins"])

        assert read_back.count() == origins.count()

    def test_merge_is_retry_safe_for_source_corrections(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        data = load_starter_data(spark)
        origins = normalize_origins(data["raw_origin_records"])
        paths = prepare_delta_paths(str(tmp_path))

        initial = origins.filter(origins.source_record_id != "origin-002-geo")
        update = origins.filter(origins.source_record_id == "origin-002-geo")

        write_silver_delta_overwrite(initial, paths["silver_origins"])
        merge_silver_origins_delta(spark, update, paths["silver_origins"])
        merge_silver_origins_delta(spark, update, paths["silver_origins"])

        read_back = read_delta_table(spark, paths["silver_origins"])
        origin_002_rows = read_back.filter(
            read_back.source_record_id == "origin-002-geo"
        ).count()

        assert origin_002_rows == 1

    def test_lineage_edges_can_be_appended_to_delta(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        data = load_starter_data(spark)
        events = normalize_batch_events(data["raw_batch_events"])
        shipments = normalize_shipments(data["raw_shipments"])
        edges = build_lineage_edges(events, shipments)
        paths = prepare_delta_paths(str(tmp_path))

        append_lineage_edges_delta(edges, paths["lineage_edges"])
        read_back = read_delta_table(spark, paths["lineage_edges"])

        assert read_back.count() == edges.count()

    def test_optional_previous_delta_version_can_be_read(
        self, spark: SparkSession, tmp_path: Path
    ) -> None:
        data = load_starter_data(spark)
        origins = normalize_origins(data["raw_origin_records"])
        paths = prepare_delta_paths(str(tmp_path))

        write_silver_delta_overwrite(origins.limit(1), paths["silver_origins"])
        write_silver_delta_overwrite(origins, paths["silver_origins"])
        version_zero = read_delta_version(spark, paths["silver_origins"], 0)

        assert version_zero.count() == 1


if __name__ == "__main__":
    spark_session = create_spark_session()
    try:
        frames = load_starter_data(spark_session)
        normalized_origins = normalize_origins(frames["raw_origin_records"])
        normalized_events = normalize_batch_events(frames["raw_batch_events"])
        normalized_shipments = normalize_shipments(frames["raw_shipments"])
        normalized_declarations = normalize_regulatory_declarations(
            frames["raw_regulatory_declarations"]
        )
        lineage = build_lineage_edges(normalized_events, normalized_shipments)
        origin_conflicts = detect_attribute_conflicts(normalized_origins)
        propagated_risk = propagate_compliance_risk(normalized_origins, lineage)
        final_report = produce_traceability_report(
            "PRODUCT:CHOC-2026-05-A",
            normalized_origins,
            lineage,
            origin_conflicts,
            normalized_declarations,
            propagated_risk,
        )
        final_report.show(truncate=False)
    finally:
        spark_session.stop()


# When finished, submit this whole file for grading.
