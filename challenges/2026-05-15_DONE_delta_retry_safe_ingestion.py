"""Delta-style retry-safe ingestion challenge.

Title: Retry-Safe Customer Profile Ingestion
Duration: 60 minutes
Difficulty: Senior / Lead data engineering
Topics tested:
- python.typing
- python.data_modeling
- python.testing
- delta.merge
- engineering.idempotency
- engineering.retry_safety
- engineering.data_quality
- engineering.observability

Scenario:
You own a daily customer-profile ingestion job. The upstream system can resend
the same batch after a failed retry, send duplicate records inside one batch,
and occasionally send stale updates. Your task is to implement the pure-Python
equivalent of the business logic you would later express as a Delta Lake MERGE.

Tasks:
1. Normalize raw customer records into a consistent shape.
2. Deduplicate incoming rows by business key, keeping the newest valid update.
3. Merge the deduplicated rows into an existing target table in a retry-safe way.
4. Track rejected rows with clear reasons.
5. Explain the Delta/Spark production design in the written-answer section.

Evaluation focus:
Correctness, edge-case handling, idempotency, type clarity, practical tests,
and clear senior-level reasoning about Delta MERGE, retries, and observability.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypedDict


# =========================
# CHALLENGE DESCRIPTION
# =========================

# Implement the TODOs in the IMPLEMENTATION AREA.
#
# Important business rules:
# - customer_id is the business key.
# - email comparison should be case-insensitive after trimming whitespace.
# - updated_at is an ISO-8601 UTC timestamp ending in "Z".
# - A row is invalid if customer_id, email, or updated_at is missing/blank.
# - A row is invalid if updated_at cannot be parsed.
# - If multiple valid incoming rows exist for the same customer_id, keep the row
#   with the newest updated_at. If timestamps tie, keep the one that appears
#   latest in the source batch.
# - Merge semantics:
#   - Insert customer_ids not present in the target.
#   - Update existing customer_ids only when the incoming updated_at is newer
#     than the target updated_at.
#   - Ignore stale or equal-timestamp incoming rows.
# - Running the same batch twice must produce the same target rows.


# =========================
# GIVEN / STARTER DATA
# =========================


class CustomerRow(TypedDict):
    customer_id: str
    email: str
    status: str
    updated_at: str


class RejectedRow(TypedDict):
    row: dict[str, Any]
    reason: str


EXISTING_CUSTOMERS: list[CustomerRow] = [
    {
        "customer_id": "C001",
        "email": "alice@example.com",
        "status": "active",
        "updated_at": "2026-05-13T08:00:00Z",
    },
    {
        "customer_id": "C002",
        "email": "bob@example.com",
        "status": "active",
        "updated_at": "2026-05-14T10:00:00Z",
    },
]


RAW_BATCH: list[dict[str, Any]] = [
    {
        "customer_id": " C001 ",
        "email": "Alice.New@Example.com ",
        "status": "ACTIVE",
        "updated_at": "2026-05-15T09:30:00Z",
    },
    {
        "customer_id": "C002",
        "email": "bob.old@example.com",
        "status": "inactive",
        "updated_at": "2026-05-13T12:00:00Z",
    },
    {
        "customer_id": "C003",
        "email": " cara@example.com ",
        "status": " trial ",
        "updated_at": "2026-05-15T07:00:00Z",
    },
    {
        "customer_id": "C003",
        "email": "cara.final@example.com",
        "status": "active",
        "updated_at": "2026-05-15T11:00:00Z",
    },
    {
        "customer_id": "",
        "email": "missing-id@example.com",
        "status": "active",
        "updated_at": "2026-05-15T11:30:00Z",
    },
    {
        "customer_id": "C004",
        "email": "bad-time@example.com",
        "status": "active",
        "updated_at": "not-a-timestamp",
    },
]


# =========================
# IMPLEMENTATION AREA
# =========================


def parse_utc_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp ending with Z.

    - Return a timezone-aware datetime.
    - Raise ValueError for invalid values.
    """
    import dateutil

    try:
        #TODO  Timestamp validation does not enforce “ending in Z” strictly.
        #TODO dateutil.parser.isoparse accepts more formats than the challenge contract requires.
        return dateutil.parser.isoparse(value.strip())
    
    #except Exception in parse_utc_timestamp is too broad.
    except ValueError:
        raise ValueError("invalid_updated_at")

def normalize_row(raw: dict[str, Any]) -> CustomerRow:
    """Normalize one raw row into the canonical target shape.

    - Strip customer_id, email, status, and updated_at.
    - Lowercase email and status.
    - Validate required fields.
    - Validate updated_at by parsing it, but keep the canonical string value
      in the returned row.
    """

    for k, v in raw.items():
        if k in ("email", "updated_at", "customer_id"):
            #challenges/2026-05-15_delta_retry_safe_ingestion.py:164: validation checks raw
            #values before stripping, so "   " is accepted as present.
             if not str(v).strip():
                 raise ValueError(f"missing_{k}")
    

    #- challenges/2026-05-15_delta_retry_safe_ingestion.py:169: missing keys raise
    # KeyError, not a clean rejection reason.
    if not raw.get("updated_at"):
        raise ValueError("missing_updated_at")
    
    parse_utc_timestamp(value=raw["updated_at"])

    #Use stripped values for validation:
    return CustomerRow(
        customer_id=str(raw.get("customer_id", "")).strip(),
        email=str(raw.get("email","")).strip().lower(),
        status=str(raw.get("status","")).strip().lower(),
        updated_at=str(raw.get("updated_at","")).strip(),
    )

def normalize_batch(raw_rows: list[dict[str, Any]]) -> tuple[list[CustomerRow], list[RejectedRow]]:
    """Normalize a raw batch and collect rejected rows.

    - Return valid normalized rows and rejected rows.
    - Do not stop processing the batch after one bad row.
    - Use useful rejection reasons such as "missing_customer_id" or
      "invalid_updated_at".
    """
    c_rows=[]
    r_rows=[]
    for r in raw_rows:
        try:
            c_rows.append(normalize_row(r))
        except ValueError as e:
            r_rows.append(RejectedRow(
                row=r,
                reason=str(e)
            ))

    return (c_rows, r_rows)


def deduplicate_latest(rows: list[CustomerRow]) -> list[CustomerRow]:
    """Keep only the newest valid row per customer_id.

    - Use updated_at ordering.
    - For equal timestamps, keep the later source row.
    - Return rows in deterministic customer_id order.
    """

    latest_customer_ids: dict[str, CustomerRow] = {}

    for row in rows:
        customer_id = row["customer_id"]

        if customer_id not in latest_customer_ids:
            latest_customer_ids[customer_id] = row
            continue

        exist = latest_customer_ids[customer_id]

        if parse_utc_timestamp(exist["updated_at"]) <= parse_utc_timestamp(row["updated_at"]):
            latest_customer_ids[customer_id] = row

    #challenges/2026-05-15_delta_retry_safe_ingestion.py:223: output order depends on
    #dict insertion order, not sorted customer_id order as requested.


    return [latest_customer_ids[customer_id] for customer_id in sorted(latest_customer_ids.keys())] 




def merge_customers(
    existing_rows: list[CustomerRow],
    incoming_rows: list[CustomerRow],
) -> list[CustomerRow]:
    """Apply Delta-like merge semantics in memory.

    - Do not mutate the input lists or dictionaries.
    - Insert new customer_ids.
    - Update existing customer_ids only when incoming updated_at is newer.
    - Ignore stale or equal updates.
    - Return rows in deterministic customer_id order.
    """

    ex_dict = {row["customer_id"]:row.copy() for row in existing_rows}

    #merge_customers does not mutate the input lists, but it reuses the original row
    #dictionaries in the returned result. That is usually acceptable here, but a
    #stricter interpretation would copy rows.
    for row_in in incoming_rows:
        r_id = row_in["customer_id"]

        if r_id not in ex_dict:
            ex_dict[r_id]=row_in
        else:
            if parse_utc_timestamp(row_in["updated_at"]) > parse_utc_timestamp(ex_dict[r_id]["updated_at"]):
                ex_dict[r_id] = row_in


    return sorted(
        [r for r in ex_dict.values()],
        key=lambda r: r["customer_id"]
    ) 


def run_ingestion(
    existing_rows: list[CustomerRow],
    raw_rows: list[dict[str, Any]],
) -> tuple[list[CustomerRow], list[RejectedRow]]:
    """Run the full ingestion flow.

    TODO:
    - Normalize the batch.
    - Deduplicate valid rows.
    - Merge into the existing target.
    - Return the new target and rejected rows.
    """
    norm_rej = normalize_batch(raw_rows=raw_rows)
    dedup = deduplicate_latest(norm_rej[0])
    merged = merge_customers(existing_rows=existing_rows, incoming_rows=dedup)

    return (merged, norm_rej[1])


# =========================
# WRITTEN ANSWERS
# =========================

WRITTEN_ANSWERS = """
Answer these after your implementation:

1. Delta MERGE design:
   What would the MERGE condition and update condition be in Delta Lake?
   - I would firsst normalize and dedupliacete the incoming batch so there is only one source
   row per customer_id, keeping the newest updated_at. THen I would merge on the business key.

2. Retry safety:
   Why is this ingestion idempotent if the same batch is retried?
   - THe ingestion is idempotent because applying the same batch again does not change the final target state.
   New customers are inserted the first time, but on retry they already exist. Existing customers are only updated
   when the incoming update_at is greater than the target updated_at. On retry, the same records
   have equal timestamps, so they are ignored.

3. Spark performance:
   If this job processes 500 million incoming rows per day, what partitioning,
   shuffle, or skew concerns would you check?
   - At 500m incoming rows per day, I would check the shuffle caused by deduplicatiing on customer_id
   because grouping or windowing by customer_id can be expensive. I would look for skewed customer_ids, large shuffle spill,
   long tail tasks and uneven partition sizes.
   - I would also check target table layout. I would avoid partitioning by customer_id because
   it is high-cardinality. I would consider partitioning by a coarser ingestion date or business date
   if it matches access patterns, and use clstering or Z-ordering on customer_id if available to make merges
   more efficient

4. Observability:
   What metrics and data-quality checks would you emit for this job?
   - I would emit input row count, valid row count, rejected row count by reason,
   duplicate row count, rows inserted, rows updated, stale rows ignored, and the maximum
   updated_at processed. I would also track job duration, shuffle size, failed records,
   and Delta merge metrics if available.

   For data quality, I would check for missing customer_id, missing email, invalid updated_at,
   duplicate customer_ids after deduplication, unexpected status values, and sudden spikes
   in rejected or stale rows. I would keep the rejected rows with reasons so they can be
    audited or replazed after fixing upstream data. 

5. Backfill behavior:
   How would your design prevent an old backfill from overwriting fresher data?
   - Old backfills are safe because the merge update condition only allows a
   source row to update the target when source.updated_at is greater than
   target.updated_at. If a backfill contains older historical records, those
   rows may be inserted if the customer does no exist yet, but they will not
   overwrite a fresher current record.

   I would run backfills through the same normalzation, deduplication, and merge
   path as daily ingestion. I wold also include batch metadata such as batch_id,
   source_file, and ingestion_time for auditability, but those fields should not
   replace the business updated_at rule
"""


# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


def test_normalize_row_trims_and_standardizes_values() -> None:
    row = normalize_row(
        {
            "customer_id": " C010 ",
            "email": "Person@Example.COM ",
            "status": " ACTIVE ",
            "updated_at": "2026-05-15T12:00:00Z ",
        }
    )

    assert row == {
        "customer_id": "C010",
        "email": "person@example.com",
        "status": "active",
        "updated_at": "2026-05-15T12:00:00Z",
    }


def test_normalize_batch_collects_rejections() -> None:
    valid_rows, rejected_rows = normalize_batch(RAW_BATCH)

    assert len(valid_rows) == 4
    assert len(rejected_rows) == 2
    assert {item["reason"] for item in rejected_rows} == {
        "missing_customer_id",
        "invalid_updated_at",
    }


def test_deduplicate_latest_keeps_newest_and_tie_keeps_later_row() -> None:
    rows: list[CustomerRow] = [
        {
            "customer_id": "C100",
            "email": "old@example.com",
            "status": "active",
            "updated_at": "2026-05-15T10:00:00Z",
        },
        {
            "customer_id": "C100",
            "email": "tie-first@example.com",
            "status": "active",
            "updated_at": "2026-05-15T11:00:00Z",
        },
        {
            "customer_id": "C100",
            "email": "tie-second@example.com",
            "status": "inactive",
            "updated_at": "2026-05-15T11:00:00Z",
        },
    ]

    assert deduplicate_latest(rows) == [
        {
            "customer_id": "C100",
            "email": "tie-second@example.com",
            "status": "inactive",
            "updated_at": "2026-05-15T11:00:00Z",
        }
    ]


def test_run_ingestion_merges_newer_rows_and_rejects_bad_rows() -> None:
    target_rows, rejected_rows = run_ingestion(EXISTING_CUSTOMERS, RAW_BATCH)

    assert target_rows == [
        {
            "customer_id": "C001",
            "email": "alice.new@example.com",
            "status": "active",
            "updated_at": "2026-05-15T09:30:00Z",
        },
        {
            "customer_id": "C002",
            "email": "bob@example.com",
            "status": "active",
            "updated_at": "2026-05-14T10:00:00Z",
        },
        {
            "customer_id": "C003",
            "email": "cara.final@example.com",
            "status": "active",
            "updated_at": "2026-05-15T11:00:00Z",
        },
    ]
    assert len(rejected_rows) == 2


def test_run_ingestion_is_idempotent_for_retries() -> None:
    first_target, first_rejections = run_ingestion(EXISTING_CUSTOMERS, RAW_BATCH)
    second_target, second_rejections = run_ingestion(first_target, RAW_BATCH)

    assert second_target == first_target
    assert second_rejections == first_rejections


if __name__ == "__main__":
    print(
        "Challenge ready. Implement the TODOs, then run:\n"
        "  uv run python challenges/2026-05-15_delta_retry_safe_ingestion.py\n"
        "  uv run pytest challenges/2026-05-15_delta_retry_safe_ingestion.py"
    )


# When finished, submit this whole file for grading.
