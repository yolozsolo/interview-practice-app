from __future__ import annotations
from datetime import datetime

"""
Title: Inventory Snapshot Reconciliation With Retry-Safe Updates
Duration: 60 minutes
Difficulty: Senior
Topics tested:
- Python data normalization and validation
- Deduplication by business key and event version
- Retry-safe incremental ingestion
- Tombstone/delete handling
- Reconciliation reporting and production observability

Scenario:
You own an inventory pipeline that receives hourly product-location snapshots
from several warehouse systems. The same batch can be retried, source systems
can send duplicate records, and late corrections can arrive with a higher
source version. Some records are tombstones that should remove a product from
the active serving table.

Tasks:
1. Normalize and validate the incoming records.
2. Deduplicate the batch deterministically by business key.
3. Apply the batch to an existing target table in a retry-safe way.
4. Produce a compact reconciliation report.
5. Answer the written design questions.

Evaluation focus:
- Correctness under retries and out-of-order records
- Clear business-key reasoning
- Defensive handling of bad source data
- Simple, readable Python 3.12 code
- Practical production thinking, especially around metrics and backfills
"""

# =========================
# CHALLENGE DESCRIPTION
# =========================

# Implement a small in-memory version of an inventory ingestion pipeline.
#
# Business key:
# - (sku, warehouse_id)
#
# Important source fields:
# - sku: product identifier; normalize by stripping whitespace and uppercasing
# - warehouse_id: warehouse identifier; normalize by stripping whitespace and uppercasing
# - quantity_on_hand: integer >= 0 for active records
# - event_ts: ISO timestamp string; used only for reporting/debugging
# - source_version: monotonically increasing integer per business key
# - is_deleted: tombstone flag; if true, remove the key from active target state
# - ingest_id: source batch attempt identifier; retries may reuse or change this
#
# Senior-level constraints:
# - The same raw batch may be applied more than once.
# - A retried batch must not double-count inserts, updates, or deletes.
# - A lower source_version must never overwrite a higher source_version already
#   present in the target.
# - Within one batch, keep exactly one record per business key:
#   highest source_version wins; if tied, newest event_ts wins; if still tied,
#   choose deterministically.
# - Invalid records should be excluded from target mutation and included in the
#   reconciliation report.

# =========================
# GIVEN / STARTER DATA
# =========================

RawRecord = dict[str, object]
TargetRecord = dict[str, object]


EXISTING_TARGET: list[TargetRecord] = [
    {
        "sku": "SKU-100",
        "warehouse_id": "AMS-1",
        "quantity_on_hand": 12,
        "source_version": 3,
        "last_event_ts": "2026-05-18T08:00:00",
    },
    {
        "sku": "SKU-200",
        "warehouse_id": "BUD-1",
        "quantity_on_hand": 5,
        "source_version": 8,
        "last_event_ts": "2026-05-18T08:05:00",
    },
    {
        "sku": "SKU-300",
        "warehouse_id": "AMS-1",
        "quantity_on_hand": 1,
        "source_version": 4,
        "last_event_ts": "2026-05-18T08:10:00",
    },
]


RAW_BATCH: list[RawRecord] = [
    {
        "sku": " sku-100 ",
        "warehouse_id": "ams-1",
        "quantity_on_hand": "14",
        "event_ts": "2026-05-18T09:00:00",
        "source_version": 4,
        "is_deleted": False,
        "ingest_id": "attempt-001",
    },
    {
        "sku": "SKU-100",
        "warehouse_id": "AMS-1",
        "quantity_on_hand": 13,
        "event_ts": "2026-05-18T08:59:59",
        "source_version": 4,
        "is_deleted": False,
        "ingest_id": "attempt-001",
    },
    {
        "sku": "SKU-200",
        "warehouse_id": "BUD-1",
        "quantity_on_hand": 99,
        "event_ts": "2026-05-18T07:00:00",
        "source_version": 7,
        "is_deleted": False,
        "ingest_id": "attempt-001",
    },
    {
        "sku": "SKU-300",
        "warehouse_id": "AMS-1",
        "quantity_on_hand": None,
        "event_ts": "2026-05-18T09:02:00",
        "source_version": 5,
        "is_deleted": True,
        "ingest_id": "attempt-001",
    },
    {
        "sku": "sku-400",
        "warehouse_id": "bud-1",
        "quantity_on_hand": "21",
        "event_ts": "2026-05-18T09:03:00",
        "source_version": 1,
        "is_deleted": False,
        "ingest_id": "attempt-001",
    },
    {
        "sku": "",
        "warehouse_id": "BUD-1",
        "quantity_on_hand": 10,
        "event_ts": "2026-05-18T09:04:00",
        "source_version": 1,
        "is_deleted": False,
        "ingest_id": "attempt-001",
    },
    {
        "sku": "SKU-500",
        "warehouse_id": "WAW-1",
        "quantity_on_hand": -2,
        "event_ts": "2026-05-18T09:05:00",
        "source_version": 1,
        "is_deleted": False,
        "ingest_id": "attempt-001",
    },
]


# This intentionally weak implementation is here to make the file runnable.
# Replace it with a senior-quality implementation in the TODO area below.
def broken_apply_batch(
    existing_target: list[TargetRecord],
    raw_batch: list[RawRecord],
) -> tuple[list[TargetRecord], dict[str, object]]:
    report: dict[str, object] = {
        "received_records": len(raw_batch),
        "valid_records": len(raw_batch),
        "invalid_records": 0,
        "deduplicated_records": len(raw_batch),
        "inserted": 0,
        "updated": 0,
        "deleted": 0,
        "ignored_stale": 0,
        "errors": [],
    }
    return list(existing_target), report


# =========================
# IMPLEMENTATION AREA
# =========================


def normalize_record(raw: RawRecord) -> tuple[TargetRecord | None, str | None]:
    """Return a normalized record or a short validation error string."""
    # TODO:
    # - Normalize sku and warehouse_id.
    # - Parse quantity_on_hand to int for active records.
    # - Allow quantity_on_hand to be missing/None for tombstone records.
    # - Validate non-empty business key, non-negative quantity, valid integer
    #   source_version, event_ts presence, and boolean is_deleted.
    # - Return (record, None) when valid, otherwise (None, "reason").
    
    #Check for all required keys
    required = {"sku", "warehouse_id", "event_ts", "is_deleted", "ingest_id", "source_version", "quantity_on_hand"}
    missing = required - raw.keys()
    if missing:
       return None, f"missing_fields: {",".join(missing)}"
    
    #normalize SKU
    raw_sku = raw["sku"]
    sku = str(raw_sku).strip().upper() if raw_sku is not None else ""
    if not sku:
        return None, "invalid_sku"
    
    #normalize warehouse_id
    raw_warehouse_id = raw["warehouse_id"]
    warehouse_id = str(raw_warehouse_id).strip().upper() if raw_warehouse_id is not None else ""
    if not warehouse_id:
        return None, "invalid_warehouse_id"
    
    #normalize is_deleted
    raw_is_deleted = raw["is_deleted"]
    if not isinstance(raw_is_deleted, bool):
        return None, "invalid_is_deleted"
    is_deleted = raw_is_deleted
    
    #normalize quantity_on_hand
    raw_quantity_on_hand = raw["quantity_on_hand"]
    if not is_deleted:
        try:
            quantity_on_hand = int(raw_quantity_on_hand)
        except (TypeError, ValueError):
            return  None, "invalid_quantity_on_hand"
        
        if quantity_on_hand < 0:
            return  None, "invalid_quantity_on_hand"
    else:
        quantity_on_hand = None

    #normalize source_version
    raw_source_version = raw["source_version"]
    str_source_version = str(raw_source_version).strip() if raw_source_version is not None else ""
    if not str_source_version:
        return None, "invalid_source_version"
    
    try:
        source_version = int(str_source_version)
    except (TypeError, ValueError):
        return None, "invalid_source_version"
    
    #normalize event_ts
    raw_event_ts = raw["event_ts"]
    event_ts = str(raw_event_ts).strip() if raw_event_ts is not None else ""
    
    if not event_ts:
        return None, "invalid_event_ts"
    else:      
        try:
            datetime.fromisoformat(event_ts)
        except (ValueError, TypeError):
            return None, "invalid_event_ts"
        
    #normalize ingest_id
    raw_ingest_id = raw["ingest_id"]
    ingest_id = str(raw_ingest_id).strip() if raw_ingest_id is not None else ""
    if not ingest_id:
        return None, "invalid_ingest_id"

    #return normalized record
    return {
        "sku": sku,
        "warehouse_id": warehouse_id,
        "is_deleted": is_deleted,
        "quantity_on_hand": quantity_on_hand,
        "source_version": source_version,
        "event_ts": event_ts,
        "ingest_id": ingest_id
    }, None


def deduplicate_records(records: list[TargetRecord]) -> list[TargetRecord]:
    """Keep one record per (sku, warehouse_id)."""
    # TODO:
    # - Group by business key.
    # - Choose highest source_version.
    # - Break ties with newest event_ts.
    # - Make the final tie-break deterministic.

    def rank_key(record: TargetRecord) -> tuple[int, datetime, str]:
            return(
                record["source_version"],
                datetime.fromisoformat(record["event_ts"]),
                record["ingest_id"]
            )

    groups: dict[tuple[str,str], TargetRecord] = {}
    for r in records:
        key = (r["sku"], r["warehouse_id"])

        if key not in groups:
            groups[key] = r
            continue
        
        current = groups.get(key)
        if current is None or rank_key(r) > rank_key(current):
                groups[key] = r

    return list(groups.values())


def apply_inventory_batch(
    existing_target: list[TargetRecord],
    raw_batch: list[RawRecord],
) -> tuple[list[TargetRecord], dict[str, object]]:
    """Apply an incremental inventory batch and return new target plus report."""
    # TODO:
    # - Normalize and validate every raw record.
    # - Deduplicate only valid records.
    # - Build target state keyed by (sku, warehouse_id).
    # - Insert new active records.
    # - Update existing active records only when source_version is newer.
    # - Delete existing records only when the tombstone source_version is newer.
    # - Ignore stale or equal-version records so retries are idempotent.
    # - Return target rows sorted by (sku, warehouse_id).
    # - Return a report with at least the keys used by broken_apply_batch.

    norm_result = [normalize_record(record) for record in raw_batch]
    
    norm_batch = [record for record, _ in norm_result if record is not None]

    errors = [error for _, error in norm_result  if error is not None]

    source = deduplicate_records(norm_batch)

    def business_key(tr: TargetRecord):
        return(
            tr["sku"],
            tr["warehouse_id"]
        )
    
    def raw_to_target(rr: TargetRecord) -> TargetRecord:
        return {
            "sku": rr["sku"],
            "warehouse_id": rr["warehouse_id"],
            "quantity_on_hand": rr["quantity_on_hand"],
            "source_version": rr["source_version"],
            "last_event_ts": rr["event_ts"]
        }
    
    target = {business_key(record):record for record in existing_target}

    inserted = 0
    updated = 0
    deleted = 0
    ignored_stale = 0
    for r in source:
        key = business_key(r)

        if key not in target:
            if r["is_deleted"]:
                ignored_stale += 1
            else:
                target[key] = raw_to_target(r)
                inserted += 1
            continue

        if r["source_version"] > target[key]["source_version"]:
            if r["is_deleted"]:
                target.pop(key)
                deleted += 1
                continue

            target[key] = raw_to_target(r)
            updated += 1
            continue
        else:
            ignored_stale += 1

    result = sorted(target.values(), key=business_key)

    report: dict[str, object] = {
        "received_records": len(raw_batch),
        "valid_records": len(norm_batch),
        "invalid_records": len(errors),
        "deduplicated_records": len(norm_batch)-(len(norm_batch) - len(source)),
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "ignored_stale": ignored_stale,
        "errors": errors,
    }
    return (result, report)


# =========================
# WRITTEN ANSWERS
# =========================

WRITTEN_ANSWERS = {
    "idempotency": """
    TODO: Explain why this implementation is retry-safe. Be specific about
    source_version, deletes, and what happens when the same batch is applied
    twice.
    ANSWER: The implementation is idempotent because:
    - Update: When record already exists based on key in the target group we check only for newer version based on rank key. In this case source_version and if its not higher we keep the same record. So in case we apply the same record nothing will happend based on the logic.
    - Delete: On deletes scenario 1: when we check for new records we skip the ones where `is_deleted` is set to true. scenario 2: We remove during the update the ones that is set for deletion. So when we reapply scenario one will kick in so we skip.
    - The above 2 operation overs the idempotency because there is no "new" record when we reapply. We only need to take care of Update and delete.
    """,
    "spark_delta_design": """
    TODO: Describe how you would implement this in Spark/Delta Lake. Include
    the MERGE condition, how you would handle tombstones, and which columns
    must be part of the business key.
    ANSWER: I dont know I need practice in this. 
    """,
    "data_quality": """/
    TODO: List 4 metrics or alerts you would emit in production for this job.
    Explain what operational problem each metric helps detect.
    ANSWER:
    - received records: there is an expectation on how much this should be if it not hits the expected threshold we should raise an alert. The reason is that we should expect the number of records processed prior to processing. If the numbers differ there is an error in the ingestion.
    - invalid records: This should be obvious, we do not neccessarily want invalid records. This can be based on percentage or any time there is one. It can either indicate source system error or validation error in the pipeline
    - ignored_stale: we should also keep an eye on this if this is close to received records or hits a certain treshold compared to received records there is a MERGE issue within the pipeline. Ofcourse when reapplying then this is expected.
    - deduplicated_records: can suggest an issue in the pipeline where we read the same message multiple times without a known reason.
    - ofcourse any of the metrics in the report is useful information for the correct question. deleted/updated should also be watched in case we modify existing datasets without the correct intention
    """,
    "backfill_strategy": """
    TODO: A warehouse sends corrected source_version values for the last 30
    days. Explain a safe backfill strategy that avoids corrupting newer target
    records and allows rollback or audit.
    ANSWER: First we should do a snapshot on existing target. The job should emit all the necessary metrics for audition for example the ones in the task above. For rollback we could apply the snapshot.
    """,
}


# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


def test_apply_batch_expected_target_state() -> None:
    target, report = apply_inventory_batch(EXISTING_TARGET, RAW_BATCH)

    assert target == [
        {
            "sku": "SKU-100",
            "warehouse_id": "AMS-1",
            "quantity_on_hand": 14,
            "source_version": 4,
            "last_event_ts": "2026-05-18T09:00:00",
        },
        {
            "sku": "SKU-200",
            "warehouse_id": "BUD-1",
            "quantity_on_hand": 5,
            "source_version": 8,
            "last_event_ts": "2026-05-18T08:05:00",
        },
        {
            "sku": "SKU-400",
            "warehouse_id": "BUD-1",
            "quantity_on_hand": 21,
            "source_version": 1,
            "last_event_ts": "2026-05-18T09:03:00",
        },
    ]
    assert report["received_records"] == 7
    assert report["valid_records"] == 5
    assert report["invalid_records"] == 2
    assert report["deduplicated_records"] == 4
    assert report["inserted"] == 1
    assert report["updated"] == 1
    assert report["deleted"] == 1
    assert report["ignored_stale"] == 1


def test_apply_batch_is_idempotent_on_retry() -> None:
    first_target, first_report = apply_inventory_batch(EXISTING_TARGET, RAW_BATCH)
    second_target, second_report = apply_inventory_batch(first_target, RAW_BATCH)

    assert second_target == first_target
    assert first_report["inserted"] == 1
    assert first_report["updated"] == 1
    assert first_report["deleted"] == 1
    assert second_report["inserted"] == 0
    assert second_report["updated"] == 0
    assert second_report["deleted"] == 0
    assert second_report["ignored_stale"] == 4


def test_lower_version_never_overwrites_target() -> None:
    target, report = apply_inventory_batch(
        existing_target=[
            {
                "sku": "SKU-900",
                "warehouse_id": "AMS-1",
                "quantity_on_hand": 3,
                "source_version": 10,
                "last_event_ts": "2026-05-18T10:00:00",
            }
        ],
        raw_batch=[
            {
                "sku": "SKU-900",
                "warehouse_id": "AMS-1",
                "quantity_on_hand": 999,
                "event_ts": "2026-05-18T09:00:00",
                "source_version": 9,
                "is_deleted": False,
                "ingest_id": "attempt-stale",
            }
        ],
    )

    assert target[0]["quantity_on_hand"] == 3
    assert target[0]["source_version"] == 10
    assert report["ignored_stale"] == 1


if __name__ == "__main__":
    next_target, reconciliation_report = apply_inventory_batch(
        EXISTING_TARGET,
        RAW_BATCH,
    )
    print("Target rows:")
    for row in next_target:
        print(row)

    print("\nReconciliation report:")
    for key, value in reconciliation_report.items():
        print(f"{key}: {value}")


# When finished, submit this whole file for grading.
