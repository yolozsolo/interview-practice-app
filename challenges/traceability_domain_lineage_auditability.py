from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal, TypedDict, get_args
from datetime import datetime

"""
Title: Traceability Domain Model, Lineage, and Auditability
Duration: 60-120 minutes
Difficulty: Senior / Lead
Topics tested:
- Traceability domain modeling
- Business entity identity vs source record identity
- Lineage graph construction and traversal
- Audit events and explainability
- Deterministic ingestion and idempotency
- Deduplication and source-version corrections
- Schema validation and conflict detection
- Confidence / completeness scoring
- Regulatory and sustainability risk propagation
- Written communication for a technical leadership interview

Scenario:
You are designing the in-memory core of an end-to-end supply chain
traceability platform for cocoa-derived product batches. The platform ingests
heterogeneous records from farm management, geospatial, shipment, factory,
regulatory, and product systems.

The business needs to answer:
1. Which upstream entities contributed to a downstream product batch?
2. Which downstream products are impacted by a problematic farm, plot, or lot?
3. Which source records contributed to a resolved business entity?
4. What changed between two ingestions of the same business entity?
5. Can an auditor understand why a traceability decision was made?

Tasks:
1. Normalize and validate raw source records.
2. Resolve source records into business entities.
3. Build lineage edges between entities.
4. Detect conflicting attributes from different systems.
5. Traverse upstream and downstream lineage deterministically.
6. Build an audit trail and calculate a traceability score.
7. Produce a compact report for one entity.
8. Answer the written design questions.

Evaluation focus:
- Clear domain modeling choices
- Correctness under duplicates, corrections, and out-of-order events
- Practical handling of source identity vs business identity
- Deterministic output suitable for tests and audit
- Senior-level reasoning about production scale, Delta Lake, and governance
"""

# =========================
# CHALLENGE DESCRIPTION
# =========================

# Build a small in-memory traceability model. Do not add a database or external
# libraries. The data structures can be simple lists, dictionaries, dataclasses,
# or TypedDicts.
#
# Important identity concepts:
# - source_record_id identifies one raw source-system record.
# - source_version identifies corrections from the same source system.
# - business_id identifies the real-world thing being tracked, such as a farm,
#   plot, harvest lot, shipment, ingredient batch, or downstream product batch.
# - entity_id should be a normalized stable key, for example "PLOT:PLOT-A1".
#
# Suggested lineage direction:
# - parent_entity_id -> child_entity_id
# - Example: "LOT:COCOA-LOT-77" -> "BATCH:INGREDIENT-BATCH-55"
#
# Suggested resolution rule:
# - Drop invalid records but keep validation errors for audit/reporting.
# - Deduplicate exact same source record/version records.
# - For the same source_system + source_record_id, keep the highest
#   source_version. If tied, keep the newest ingest_ts. If still tied, choose
#   deterministically.
# - Do not silently overwrite conflicting business attributes from different
#   systems. Detect and report them.
#
# Suggested score:
# - Return a float from 0.0 to 1.0.
# - Penalize missing critical fields, unresolved conflicts, compliance risks,
#   and weak lineage completeness.
# - Make the score deterministic and explainable from the audit trail.

# =========================
# GIVEN / STARTER DATA
# =========================

EntityType = Literal["farm", "plot", "lot", "shipment", "batch", "product"]
SourceSystem = Literal[
    "farm_mdm",
    "geo_registry",
    "shipment_tms",
    "factory_mes",
    "regulatory_portal",
    "sustainability_platform",
    "erp",
]
RiskLevel = Literal["none", "review", "high"]


class RawSourceRecord(TypedDict, total=False):
    source_system: SourceSystem
    source_record_id: str
    source_version: int
    ingest_ts: str
    event_ts: str
    entity_type: EntityType
    business_id: str
    attributes: dict[str, object]
    parents: list[dict[str, str]]


@dataclass(frozen=True)
class NormalizedRecord:
    """A validated source record ready for entity resolution."""

    source_system: str
    source_record_id: str
    source_version: int
    ingest_ts: str
    event_ts: str
    entity_type: str
    business_id: str
    entity_id: str
    attributes: dict[str, object]
    parent_entity_ids: tuple[str, ...]
    record_fingerprint: str


@dataclass(frozen=True)
class LineageEdge:
    """A directed contribution relationship between two business entities."""

    parent_entity_id: str
    child_entity_id: str
    source_record_id: str
    confidence: float
    reason: str


@dataclass(frozen=True)
class Conflict:
    """A disagreement between source records for one business entity."""

    entity_id: str
    attribute: str
    values_by_source: dict[str, object]
    source_record_ids: tuple[str, ...]
    severity: Literal["low", "medium", "high"]


@dataclass(frozen=True)
class AuditEvent:
    """One explainability event suitable for a regulator or internal auditor."""

    entity_id: str
    event_type: str
    message: str
    source_record_ids: tuple[str, ...]
    risk_level: RiskLevel


Graph = dict[str, set[str]]
BusinessEntities = dict[str, dict[str, object]]


RAW_SOURCE_RECORDS: list[RawSourceRecord] = [
    # Farm and plot records.
    {
        "source_system": "farm_mdm",
        "source_record_id": "farm-001-mdm",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:01:00",
        "event_ts": "2026-05-18T09:00:00",
        "entity_type": "farm",
        "business_id": "farm-001",
        "attributes": {
            "producer_name": "Ama Mensah",
            "country": "GH",
            "cooperative_id": "COOP-9",
        },
        "parents": [],
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "plot-a1-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:03:00",
        "event_ts": "2026-05-18T09:05:00",
        "entity_type": "plot",
        "business_id": "plot-a1",
        "attributes": {
            "farm_id": "farm-001",
            "country": "GH",
            "geo_hash": "7zzzz-example-a1",
            "hectares": 2.4,
            "deforestation_risk": "none",
        },
        "parents": [{"entity_type": "farm", "business_id": "farm-001"}],
    },
    {
        # Correction: same source record with newer source_version.
        "source_system": "geo_registry",
        "source_record_id": "plot-a1-geo",
        "source_version": 2,
        "ingest_ts": "2026-05-20T09:10:00",
        "event_ts": "2026-05-18T09:05:00",
        "entity_type": "plot",
        "business_id": "plot-a1",
        "attributes": {
            "farm_id": "farm-001",
            "country": "GH",
            "geo_hash": "7zzzz-corrected-a1",
            "hectares": 2.5,
            "deforestation_risk": "review",
        },
        "parents": [{"entity_type": "farm", "business_id": "farm-001"}],
    },
    {
        # Conflicts with geo_registry country for the same plot.
        "source_system": "sustainability_platform",
        "source_record_id": "plot-a1-sus",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:04:00",
        "event_ts": "2026-05-18T10:00:00",
        "entity_type": "plot",
        "business_id": "plot-a1",
        "attributes": {
            "country": "CI",
            "certification": "RA",
            "deforestation_risk": "high",
        },
        "parents": [{"entity_type": "farm", "business_id": "farm-001"}],
    },
    {
        "source_system": "geo_registry",
        "source_record_id": "plot-b9-geo",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:06:00",
        "event_ts": "2026-05-18T09:15:00",
        "entity_type": "plot",
        "business_id": "plot-b9",
        "attributes": {
            "farm_id": "farm-002",
            "country": "GH",
            "geo_hash": "7zzzz-example-b9",
            "hectares": 1.1,
            "deforestation_risk": "high",
        },
        "parents": [{"entity_type": "farm", "business_id": "farm-002"}],
    },
    # Lot records, including an exact duplicate and missing optional fields.
    {
        "source_system": "factory_mes",
        "source_record_id": "lot-77-harvest",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:10:00",
        "event_ts": "2026-05-17T16:00:00",
        "entity_type": "lot",
        "business_id": "cocoa-lot-77",
        "attributes": {
            "material": "cocoa_beans",
            "weight_kg": 1200,
            "moisture_pct": 7.2,
        },
        "parents": [{"entity_type": "plot", "business_id": "plot-a1"}],
    },
    {
        # Intentional duplicate of the previous record.
        "source_system": "factory_mes",
        "source_record_id": "lot-77-harvest",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:10:00",
        "event_ts": "2026-05-17T16:00:00",
        "entity_type": "lot",
        "business_id": "cocoa-lot-77",
        "attributes": {
            "material": "cocoa_beans",
            "weight_kg": 1200,
            "moisture_pct": 7.2,
        },
        "parents": [{"entity_type": "plot", "business_id": "plot-a1"}],
    },
    {
        # Missing optional moisture_pct is acceptable.
        "source_system": "factory_mes",
        "source_record_id": "lot-88-harvest",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:12:00",
        "event_ts": "2026-05-17T17:30:00",
        "entity_type": "lot",
        "business_id": "cocoa-lot-88",
        "attributes": {
            "material": "cocoa_beans",
            "weight_kg": 800,
        },
        "parents": [{"entity_type": "plot", "business_id": "plot-b9"}],
    },
    # Shipment and transformation records intentionally arrive out of order.
    {
        "source_system": "factory_mes",
        "source_record_id": "batch-55-roast",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:00:00",
        "event_ts": "2026-05-19T06:00:00",
        "entity_type": "batch",
        "business_id": "ingredient-batch-55",
        "attributes": {
            "material": "roasted_cocoa",
            "weight_kg": 1850,
            "processing_site": "PLANT-AMS",
        },
        "parents": [
            {"entity_type": "lot", "business_id": "cocoa-lot-77"},
            {"entity_type": "lot", "business_id": "cocoa-lot-88"},
        ],
    },
    {
        "source_system": "shipment_tms",
        "source_record_id": "ship-900",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:20:00",
        "event_ts": "2026-05-18T22:00:00",
        "entity_type": "shipment",
        "business_id": "ship-900",
        "attributes": {
            "carrier": "ACME Logistics",
            "origin_country": "GH",
            "destination_site": "PLANT-AMS",
            "temperature_controlled": False,
        },
        "parents": [{"entity_type": "lot", "business_id": "cocoa-lot-77"}],
    },
    # Regulatory and compliance records.
    {
        "source_system": "regulatory_portal",
        "source_record_id": "decl-900",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:25:00",
        "event_ts": "2026-05-18T23:00:00",
        "entity_type": "shipment",
        "business_id": "ship-900",
        "attributes": {
            "declaration_id": "EUDR-2026-900",
            "customs_status": "cleared",
            "deforestation_risk": "review",
            "regulatory_flag": "requires_geo_review",
        },
        "parents": [{"entity_type": "lot", "business_id": "cocoa-lot-77"}],
    },
    {
        # Missing critical business_id must be rejected.
        "source_system": "regulatory_portal",
        "source_record_id": "decl-missing-business-id",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:26:00",
        "event_ts": "2026-05-18T23:05:00",
        "entity_type": "plot",
        "attributes": {
            "declaration_id": "EUDR-BAD-001",
            "deforestation_risk": "high",
        },
        "parents": [],
    },
    # Downstream product batches. One upstream batch contributes to two products.
    {
        "source_system": "erp",
        "source_record_id": "prod-bar-a",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:30:00",
        "event_ts": "2026-05-19T12:00:00",
        "entity_type": "product",
        "business_id": "choc-bar-2026-05-a",
        "attributes": {
            "sku": "CHOC-BAR-70G",
            "market": "EU",
            "units": 24000,
        },
        "parents": [{"entity_type": "batch", "business_id": "ingredient-batch-55"}],
    },
    {
        "source_system": "erp",
        "source_record_id": "prod-drink-b",
        "source_version": 1,
        "ingest_ts": "2026-05-20T08:31:00",
        "event_ts": "2026-05-19T13:00:00",
        "entity_type": "product",
        "business_id": "choc-drink-2026-05-b",
        "attributes": {
            "sku": "CHOC-DRINK-250ML",
            "market": "EU",
            "units": 11000,
        },
        "parents": [{"entity_type": "batch", "business_id": "ingredient-batch-55"}],
    },
]


EXPECTED_PRODUCT_A_UPSTREAM = [
    "BATCH:INGREDIENT-BATCH-55",
    "FARM:FARM-001",
    "FARM:FARM-002",
    "LOT:COCOA-LOT-77",
    "LOT:COCOA-LOT-88",
    "PLOT:PLOT-A1",
    "PLOT:PLOT-B9",
]

EXPECTED_PLOT_A1_IMPACT = [
    "BATCH:INGREDIENT-BATCH-55",
    "LOT:COCOA-LOT-77",
    "PRODUCT:CHOC-BAR-2026-05-A",
    "PRODUCT:CHOC-DRINK-2026-05-B",
    "SHIPMENT:SHIP-900",
]


# =========================
# IMPLEMENTATION AREA
# =========================


def make_entity_id(entity_type: str, business_id: str) -> str:
    """Return a stable normalized business entity identifier."""
    # TODO:
    # - Strip whitespace.
    # - Uppercase entity_type and business_id.
    # - Preserve hyphens.
    # - Return "<ENTITY_TYPE>:<BUSINESS_ID>".
    norm_entity_type = (entity_type.strip().upper() if entity_type is not None else "")
    if not norm_entity_type:
        raise ValueError("invalid_entity_type")
    
    norm_business_id = business_id.strip().upper() if business_id is not None else ""
    if not norm_business_id:
        raise ValueError("invalid_business_id")

    return ":".join([norm_entity_type, norm_business_id])

def normalize_required_string(value: object, field_name:str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"invalid_{field_name}")
    
    normalized_value = value.strip()

    if not normalized_value:
        raise ValueError(f"missing_{field_name}")
    
    return normalized_value

def normalize_required_int(value: object, field_name: str) -> int:
    if not isinstance(value, int):
        raise ValueError(f"invalid_{field_name}")
    
    return int(value)

def normalize_literal(value: object, allowed_values: set[str], field_name:str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"invalid_{field_name}")
    
    normalized_value = value.strip()
    
    if normalized_value not in allowed_values:
        raise ValueError(f"invalid_{field_name}_literal")
    
    return normalized_value
    
def normalize_parents(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValueError("invalide_parent")
    
    result: list[str] = []

    for item in value:
        if not isinstance(item, dict):
            raise ValueError("invalide_parent")
        
        for key, n_value in item.items():
            if key:
                normalize_required_string(n_value, key)
                entity_id = make_entity_id(key, n_value)

        result.append(entity_id)

    return tuple(result)

def normalize_timestamp(value: object, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"invalid_{field_name}")
    
    try:
        datetime.fromisoformat(value)
    except (ValueError, TypeError):
        raise ValueError(f"invalid_{field_name}")
    
    return value

def normalize_source_record(raw: RawSourceRecord) -> tuple[NormalizedRecord, dict[str, object]]:
    if not raw:
        raise ValueError("empty_record")
    
    try:
        normalized_source_system = normalize_literal(
            value=raw["source_system"], 
            allowed_values=set(get_args(SourceSystem)), 
            field_name="source_system"
        )

        normalized_source_record_id = normalize_required_string(
            value=raw["source_record_id"], 
            field_name="source_record_id"
        )

        normalized_source_version = normalize_required_int(
            value=raw["source_version"], 
            field_name="source_version"
        )

        normalized_ingest_ts = normalize_timestamp(
            value=raw["ingest_ts"],
            field_name="ingest_ts"
        )

        normalized_event_ts = normalize_timestamp(
            value=raw["event_ts"],
            field_name="eve_ts"
        )

        normalized_entity_type = normalize_literal(
            value=raw["entity_type"],
            allowed_values=set(get_args(EntityType)),
            field_name="entity_type"
        )

        normalized_business_id = normalize_required_string(
            value=raw["business_id"],
            field_name="business_id"
        )

        entity_id = make_entity_id(normalized_entity_type, normalized_business_id)

        attributes = raw["attributes"]

        normalized_parents = normalize_parents(
            value=raw["parents"]
        )

        record_figerprint = hash(
            normalized_source_system
            + normalized_source_record_id
            + str(normalized_source_version)
            + normalized_ingest_ts
            + normalized_event_ts
            + normalized_entity_type
            + normalized_business_id
            + entity_id
        )
    except ValueError as ve:
        return None, {
            "reason": str(ve),
            "record": raw
        }
    
    return NormalizedRecord(
        source_system=normalized_source_system,
        source_record_id=normalized_source_record_id,
        source_version=normalized_source_version,
        ingest_ts=normalized_ingest_ts,
        event_ts=normalized_event_ts,
        entity_type=normalized_entity_type,
        business_id=normalized_business_id,
        entity_id=entity_id,
        attributes=attributes,
        parent_entity_ids=normalized_parents,
        record_fingerprint=record_figerprint
    ),None

def deduplicate(normalized_set) -> dict[tuple[str,str], NormalizedRecord]:
    group: dict[tuple[str,str], NormalizedRecord] = {}
    for item in normalized_set:
        key = (item.source_system, item.source_record_id)

        if key not in group:
            group[key] = item
            continue

        current = group[key]

        if item.source_version > current.source_version:
            group[key] = item
    return group

def normalize_source_records(
    raw_records: list[RawSourceRecord],
) -> tuple[list[NormalizedRecord], list[dict[str, object]]]:
    """Validate, deduplicate, and normalize raw records.

    Return:
    - normalized records that are safe for graph/entity processing
    - rejected records with source_record_id and reason

    TODO:
    - Validate critical fields: source_system, source_record_id,
      source_version, ingest_ts, event_ts, entity_type, business_id,
      attributes, and parents.
    - Treat missing optional attributes as acceptable.
    - Convert business_id and parent ids into stable entity ids.
    - Deduplicate exact duplicate source records.
    - Keep only the latest correction for a given
      (source_system, source_record_id).
    - Sort output deterministically by entity_id, source_system,
      source_record_id, and source_version.
    - Include rejected records for missing critical fields.
    """

    required_fields = (
        "source_system", "source_record_id", "source_version",
        "ingest_ts", "event_ts", "entity_type", "business_id",
        "attributes", "parents")
    
    
    results: list[tuple[NormalizedRecord, dict[str, object]]] = []
    for record in raw_records:

        #validate required fields
        field_errors = required_fields - record.keys()
        if field_errors:
            results.append((None, {
                "reason": f"missing fields: {field_errors}",
                "record": record
            }))
            continue

        results.append(normalize_source_record(raw=record))
    
    normalized_records= [n for n,_ in results if n]
    rejected_records = [r for _,r in results if r]

    #dedup
    group = deduplicate(normalized_records)

    rank_keys = lambda r: (r.entity_id, r.source_system, r.source_record_id, r.source_version)
    results = [
        item for item in sorted(group.values(), key=rank_keys)
    ]
        
    return (results, rejected_records)


def resolve_business_entities(
    records: list[NormalizedRecord],
) -> BusinessEntities:
    """Resolve normalized source records into business entities.

    TODO:
    - Group records by entity_id.
    - Preserve a list of contributing source_record_ids per entity.
    - Merge non-conflicting attributes.
    - Keep enough source attribution to support conflict and audit reporting.
    - Include useful fields such as entity_type, business_id, attributes,
      source_record_ids, and latest_ingest_ts.
    """
    groups: BusinessEntities = {}
    for record in records:
        key = record.entity_id
        if key not in groups:
            groups.setdefault(key,{
                "entity_id": "",
                "entity_type": "",
                "business_id": "",
                "attributes": {},
                "source_record_ids": [],
                "latest_ingest_ts": ""
            })

            entity = groups[key]
            entity["entity_id"] = key
            entity["entity_type"] = record.entity_type
            entity["business_id"] = record.business_id
            entity_attributes = entity["attributes"]

            for k, v in record.attributes.items():
                entity_attributes[k] = v

            entity["latest_ingest_ts"] = record.ingest_ts
            entity["source_record_ids"].append(record.source_record_id)
            continue

        entity = groups[key]

        entity["source_record_ids"].append(record.source_record_id)
        entity_attributes = entity["attributes"]
        for k, v in record.attributes.items():
            if k in entity_attributes and v != entity_attributes[k]:
                del entity_attributes[k]
            else:
                entity_attributes[k] = v

        if datetime.fromisoformat(record.ingest_ts) > datetime.fromisoformat(entity["latest_ingest_ts"]):
            entity["latest_ingest_ts"] = record.ingest_ts

    return groups
                



def build_traceability_graph(
    records: list[NormalizedRecord],
) -> tuple[Graph, Graph, list[LineageEdge]]:
    """Build upstream and downstream lineage graphs.

    Return:
    - upstream_graph: child_entity_id -> parent_entity_ids
    - downstream_graph: parent_entity_id -> child_entity_ids
    - edges: detailed edge records with source attribution

    TODO:
    - Use parent_entity_ids from normalized records.
    - Make graph traversal deterministic by storing sorted sets or sorting at
      traversal time.
    - Deduplicate repeated edges caused by duplicate/corrected source records.
    """
    raise NotImplementedError


def detect_conflicts(records: list[NormalizedRecord]) -> list[Conflict]:
    """Detect conflicting attributes for the same business entity.
    TODO:
    - Compare attributes reported by different source systems for the same
      entity_id.
    - Report conflicts where multiple non-null values exist for a key.
    - Severity suggestion:
      - high: country, regulatory_flag, deforestation_risk
      - medium: weight_kg, hectares, material
      - low: descriptive fields
    - Sort conflicts deterministically.
    """

    #group by entityId
    groups: dict[str, list[NormalizedRecord]] = {}
    for record in records:
        key = record.entity_id
        if key not in groups:
            groups.setdefault(key, []).append(record)
            continue

        groups[key].append(record)

    high_sev = ("country", "regulatory_flag", "deforestation_risk")
    med_sev = ("weight_kg", "hectares", "material")
    conflicts: list[Conflict] = []

    #loop through entities
    for entity_id, records in groups.items():
        if len(records) < 2:
            continue

        all_attributes: dict[str, list[dict[str, object]]] = {}
        severity = ""

        #loop through the records which has multiple records for the same entity_id
        for record in records:
            for attribute_name, attribute_value in record.attributes.items():
                #ignore nulls
                if attribute_value is None:
                    continue
                # we initialize the key and value if its not in already
                if attribute_name not in all_attributes:
                    all_attributes[attribute_name]= [{
                        "source_system": record.source_system,
                        "source_record_id": record.source_record_id,
                        "value": attribute_value 
                    }]
                    continue
                
                #if the name is already in
                values = all_attributes[attribute_name]
                #we check if the value happens to be added already
                pairs = [(v["source_system"],v["value"]) for v in values]
                #if not in
                if (record.source_system, attribute_value) not in pairs:
                    #we add it (because how entity_id works it should be a separate source_system)
                    all_attributes[attribute_name].append({
                        "source_system": record.source_system,
                        "source_record_id": record.source_record_id,
                        "value": attribute_value 
                })

        #we loop through the findings
        for k, v in all_attributes.items():
            unique_values = set(value["value"] for value in v)
            if len(unique_values) > 1:
                #create severity   
                if k in (high_sev):
                    severity = "high"
                elif k in (med_sev):
                    severity = "medium"
                else:
                    severity = "low"

                #Create conflict
                conflicts.append(Conflict(
                    entity_id = entity_id,
                    attribute = k,
                    values_by_source = {r["source_system"]: r["value"] for r in v},
                    source_record_ids = tuple(r["source_record_id"] for r in v),
                    severity = severity
                ))

    return sorted(conflicts, key=lambda c: (c.entity_id, c.attribute))


def get_upstream_lineage(entity_id: str, upstream_graph: Graph) -> list[str]:
    """Return all upstream contributors for entity_id in deterministic order."""
    # TODO:
    # - Traverse recursively or iteratively.
    # - Avoid infinite loops if bad data creates a cycle.
    # - Exclude entity_id itself from the result.
    # - Return a sorted list for deterministic audit/report output.
    raise NotImplementedError


def get_downstream_impact(entity_id: str, downstream_graph: Graph) -> list[str]:
    """Return all downstream entities impacted by entity_id."""
    # TODO:
    # - Traverse recursively or iteratively.
    # - Avoid infinite loops if bad data creates a cycle.
    # - Exclude entity_id itself from the result.
    # - Return a sorted list for deterministic audit/report output.
    raise NotImplementedError


def build_audit_trail(
    entity_id: str,
    records: list[NormalizedRecord],
    upstream_graph: Graph,
    downstream_graph: Graph,
    conflicts: list[Conflict],
    rejected_records: list[dict[str, object]],
) -> list[AuditEvent]:
    """Build explainability events for one entity.

    TODO:
    - Include source records that contributed to the entity.
    - Include upstream and downstream lineage decisions.
    - Include conflicts affecting the entity or its upstream contributors.
    - Include compliance/regulatory risk flags.
    - Include rejected source records when they are relevant to the entity.
    - Sort events deterministically.
    """
    raise NotImplementedError


def calculate_traceability_score(
    entity_id: str,
    audit_trail: list[AuditEvent],
) -> float:
    """Return a deterministic score from 0.0 to 1.0."""
    # TODO:
    # - Start from 1.0.
    # - Penalize high-risk audit events more than review-risk events.
    # - Penalize conflicts and rejected critical source records.
    # - Clamp the final value to [0.0, 1.0].
    # - Round consistently, for example to 3 decimals.
    raise NotImplementedError


def produce_traceability_report(
    entity_id: str,
    raw_records: list[RawSourceRecord],
) -> dict[str, object]:
    """Produce a compact audit-friendly report for one business entity.

    TODO:
    - Normalize source records.
    - Resolve business entities.
    - Build lineage graphs.
    - Detect conflicts.
    - Build audit trail.
    - Calculate score.
    - Return a dictionary with at least:
      - entity_id
      - upstream_entities
      - downstream_entities
      - source_record_ids
      - conflicts
      - audit_trail
      - traceability_score
      - compliance_risk
      - rejected_records
    """
    raise NotImplementedError


# =========================
# WRITTEN ANSWERS
# =========================

"""
Answer these as comments or replace this docstring with your notes.

1. How would this model change if the data volume became petabyte scale?

2. How would you represent business entities, source records, lineage edges,
   conflicts, and audit events in Delta Lake / Databricks tables?

3. How would you store lineage edges and audit events so they are queryable,
   replayable, and useful for regulatory audits?

4. How would you make ingestion idempotent across retries, backfills, and
   corrections from source systems?

5. How would you handle schema evolution when a new source system adds new
   compliance attributes or changes field names?

6. How would you explain auditability to a non-technical sustainability
   stakeholder?

7. What monitoring would you add for a production traceability pipeline?
"""

# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


def _prepared_model() -> tuple[
    list[NormalizedRecord],
    list[dict[str, object]],
    BusinessEntities,
    Graph,
    Graph,
    list[LineageEdge],
    list[Conflict],
]:
    normalized, rejected = normalize_source_records(RAW_SOURCE_RECORDS)
    entities = resolve_business_entities(normalized)
    upstream_graph, downstream_graph, edges = build_traceability_graph(normalized)
    conflicts = detect_conflicts(normalized)
    return normalized, rejected, entities, upstream_graph, downstream_graph, edges, conflicts


def test_make_entity_id_normalizes_business_identity() -> None:
    assert make_entity_id(" plot ", " plot-a1 ") == "PLOT:PLOT-A1"


def test_normalization_is_deterministic_and_retry_safe() -> None:
    first, first_rejected = normalize_source_records(RAW_SOURCE_RECORDS)
    second, second_rejected = normalize_source_records(list(reversed(RAW_SOURCE_RECORDS)))

    assert first == second
    assert first_rejected == second_rejected
    assert len(first) == 11
    assert len(first_rejected) == 1
    assert first_rejected[0]["reason"] == "missing fields: {'business_id'}"

    kept_plot_versions = [
        record.source_version
        for record in first
        if record.source_system == "geo_registry"
        and record.source_record_id == "plot-a1-geo"
    ]
    assert kept_plot_versions == [2]


def test_resolved_entities_keep_source_record_attribution() -> None:
    normalized, _, entities, _, _, _, _ = _prepared_model()

    assert len(normalized) == 12
    assert "PRODUCT:CHOC-BAR-2026-05-A" in entities
    assert "PLOT:PLOT-A1" in entities

    plot_sources = entities["PLOT:PLOT-A1"]["source_record_ids"]
    assert plot_sources == ("plot-a1-geo", "plot-a1-sus")


def test_conflict_detection_finds_country_and_risk_conflicts() -> None:
    normalized, *_ = _prepared_model()
    conflicts = detect_conflicts(normalized)

    conflict_keys = {(conflict.entity_id, conflict.attribute) for conflict in conflicts}
    assert ("PLOT:PLOT-A1", "country") in conflict_keys
    assert ("PLOT:PLOT-A1", "deforestation_risk") in conflict_keys

    country_conflict = next(
        conflict
        for conflict in conflicts
        if conflict.entity_id == "PLOT:PLOT-A1" and conflict.attribute == "country"
    )
    assert country_conflict.severity == "high"
    assert country_conflict.values_by_source == {
        "geo_registry": "GH",
        "sustainability_platform": "CI",
    }


def test_upstream_lineage_for_product_batch() -> None:
    _, _, _, upstream_graph, _, _, _ = _prepared_model()

    upstream = get_upstream_lineage("PRODUCT:CHOC-BAR-2026-05-A", upstream_graph)
    assert upstream == EXPECTED_PRODUCT_A_UPSTREAM


def test_downstream_impact_from_problematic_plot() -> None:
    _, _, _, _, downstream_graph, _, _ = _prepared_model()

    downstream = get_downstream_impact("PLOT:PLOT-A1", downstream_graph)
    assert downstream == EXPECTED_PLOT_A1_IMPACT


def test_audit_trail_contains_lineage_conflict_and_risk_events() -> None:
    normalized, rejected, _, upstream_graph, downstream_graph, _, conflicts = _prepared_model()

    audit_trail = build_audit_trail(
        "PRODUCT:CHOC-BAR-2026-05-A",
        normalized,
        upstream_graph,
        downstream_graph,
        conflicts,
        rejected,
    )

    event_types = {event.event_type for event in audit_trail}
    assert "source_attribution" in event_types
    assert "upstream_lineage" in event_types
    assert "conflict_detected" in event_types
    assert "compliance_risk" in event_types
    assert any(event.risk_level == "high" for event in audit_trail)


def test_traceability_score_penalizes_risk_and_conflicts() -> None:
    normalized, rejected, _, upstream_graph, downstream_graph, _, conflicts = _prepared_model()

    product_audit = build_audit_trail(
        "PRODUCT:CHOC-BAR-2026-05-A",
        normalized,
        upstream_graph,
        downstream_graph,
        conflicts,
        rejected,
    )
    farm_audit = build_audit_trail(
        "FARM:FARM-001",
        normalized,
        upstream_graph,
        downstream_graph,
        conflicts,
        rejected,
    )

    product_score = calculate_traceability_score(
        "PRODUCT:CHOC-BAR-2026-05-A",
        product_audit,
    )
    farm_score = calculate_traceability_score("FARM:FARM-001", farm_audit)

    assert 0.0 <= product_score <= 1.0
    assert 0.0 <= farm_score <= 1.0
    assert product_score < farm_score
    assert product_score < 0.85


def test_report_propagates_compliance_risk_to_downstream_product() -> None:
    report = produce_traceability_report(
        "PRODUCT:CHOC-BAR-2026-05-A",
        RAW_SOURCE_RECORDS,
    )

    assert report["entity_id"] == "PRODUCT:CHOC-BAR-2026-05-A"
    assert report["upstream_entities"] == EXPECTED_PRODUCT_A_UPSTREAM
    assert report["compliance_risk"] == "high"
    assert report["traceability_score"] < 0.85
    assert "plot-a1-sus" in report["source_record_ids"]
    assert any(
        conflict["entity_id"] == "PLOT:PLOT-A1"
        for conflict in report["conflicts"]
    )


if __name__ == "__main__":
    print(
        "Traceability challenge loaded. Implement the TODOs, then run:\n"
        "  uv run pytest challenges/traceability_domain_lineage_auditability.py"
    )


# When finished, submit this whole file for grading.
