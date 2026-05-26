from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, HTTPException, status
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field, field_validator

"""
Title: FastAPI Inventory Event Ingestion With Idempotent Retries
Duration: 60-90 minutes
Difficulty: B-level
Topics tested:
- FastAPI route design
- Pydantic request and response models
- Inventory event ingestion
- Idempotency keys and retry-safe behavior
- Source versioning
- Rejected records with reasons
- Aggregate query endpoint
- Simple observability counters

Scenario:
You own a small API used by warehouse systems to submit inventory events. Client
systems retry requests when they time out, so the API must be safe when the same
event is submitted more than once. Events can arrive out of order, and each
warehouse sends a monotonically increasing source_version per (sku, warehouse).

Tasks:
1. Implement core ingestion logic that validates business rules beyond Pydantic.
2. Make retries idempotent using event_id.
3. Maintain the latest active inventory state by (sku, warehouse_id).
4. Reject stale or invalid events with useful reasons.
5. Expose FastAPI endpoints for ingestion, inventory lookup, and metrics.
6. Answer the written design questions.

Evaluation focus:
- Clear separation between route handlers and core data logic
- Correct HTTP status behavior
- Retry safety and deterministic state updates
- Practical data-engineering reasoning around rejected records and monitoring
"""

# =========================
# CHALLENGE DESCRIPTION
# =========================

# Build a single-file FastAPI API for inventory event ingestion.
#
# Event types:
# - snapshot: set the current quantity_on_hand for one SKU at one warehouse
# - adjustment: add delta_quantity to the current quantity_on_hand
#
# Business key:
# - (sku, warehouse_id)
#
# Idempotency key:
# - event_id
#
# Version rule:
# - For each business key, only events with a strictly higher source_version
#   than the stored state may mutate inventory.
# - Retrying the exact same event_id must return the original ingestion result
#   without mutating state again.
# - A new event_id with an older or equal source_version is stale and should be
#   rejected with reason "stale_source_version".
#
# Required endpoints:
# - POST /events
# - GET /inventory/{sku}/{warehouse_id}
# - GET /metrics
#
# Keep all state in memory for this exercise. Do not add a database.

# =========================
# GIVEN / STARTER DATA
# =========================

EventType = Literal["snapshot", "adjustment"]


class InventoryEventRequest(BaseModel):
    event_id: str = Field(min_length=1)
    sku: str = Field(min_length=1)
    warehouse_id: str = Field(min_length=1)
    event_type: EventType
    quantity_on_hand: int | None = Field(default=None, ge=0)
    delta_quantity: int | None = None
    source_version: int = Field(gt=0)
    event_ts: str = Field(min_length=1)

    @field_validator("event_id", "sku", "warehouse_id", "event_ts")
    @classmethod
    def strip_required_strings(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("must not be blank")
        return cleaned


class IngestEventResponse(BaseModel):
    event_id: str
    status: Literal["accepted", "duplicate", "rejected"]
    reason: str | None = None
    sku: str
    warehouse_id: str
    source_version: int
    quantity_on_hand: int | None = None


class InventoryStateResponse(BaseModel):
    sku: str
    warehouse_id: str
    quantity_on_hand: int
    source_version: int
    last_event_id: str
    last_event_ts: str


class MetricsResponse(BaseModel):
    received_events: int
    accepted_events: int
    duplicate_events: int
    rejected_events: int
    stale_events: int
    inventory_keys: int


InventoryKey = tuple[str, str]
InventoryState = dict[str, object]
IngestResult = dict[str, object]


STARTER_INVENTORY: dict[InventoryKey, InventoryState] = {
    ("SKU-100", "AMS-1"): {
        "sku": "SKU-100",
        "warehouse_id": "AMS-1",
        "quantity_on_hand": 10,
        "source_version": 3,
        "last_event_id": "seed-001",
        "last_event_ts": "2026-05-19T08:00:00",
    }
}


def new_store() -> dict[str, object]:
    """Create isolated in-memory state for the app and tests."""
    return {
        "inventory": {key: dict(value) for key, value in STARTER_INVENTORY.items()},
        "processed_events": {},
        "metrics": {
            "received_events": 0,
            "accepted_events": 0,
            "duplicate_events": 0,
            "rejected_events": 0,
            "stale_events": 0,
        },
    }


STORE = new_store()
app = FastAPI(title="Inventory Event Ingestion Practice")


# =========================
# IMPLEMENTATION AREA
# =========================


def normalize_key(sku: str, warehouse_id: str) -> InventoryKey:
    """Return the normalized inventory business key."""
    # TODO:
    # - Strip and uppercase sku and warehouse_id.
    # - Return a tuple in the form (sku, warehouse_id).
    raise NotImplementedError


def validate_event_shape(event: InventoryEventRequest) -> str | None:
    """Return a rejection reason for business-rule validation, or None."""
    # TODO:
    # - For snapshot events, require quantity_on_hand and reject delta_quantity.
    # - For adjustment events, require delta_quantity and reject quantity_on_hand.
    # - Reject adjustment events that would produce negative inventory.
    #   You may handle the current-state check inside ingest_event instead if
    #   that keeps your design cleaner.
    raise NotImplementedError


def ingest_event(
    event: InventoryEventRequest,
    store: dict[str, object],
) -> IngestResult:
    """Apply one inventory event in a retry-safe way."""
    # TODO:
    # - Increment received_events for every request that reaches this function.
    # - Use event_id as the idempotency key.
    # - If event_id was already processed, return the stored result and increment
    #   duplicate_events without mutating inventory.
    # - Validate snapshot vs adjustment payload rules.
    # - Build the normalized business key.
    # - Reject stale events where source_version <= current source_version.
    # - Apply snapshot or adjustment updates.
    # - Store the ingestion result under processed_events for future retries.
    # - Update accepted/rejected/stale metrics.
    raise NotImplementedError


def get_inventory_state(
    sku: str,
    warehouse_id: str,
    store: dict[str, object],
) -> InventoryStateResponse:
    """Return current inventory state or raise HTTPException if missing."""
    # TODO:
    # - Normalize the path parameters.
    # - Look up the inventory row by business key.
    # - Raise HTTPException(status_code=404, detail="inventory_not_found") when
    #   no row exists.
    # - Return InventoryStateResponse.
    raise HTTPException(status_code=404, detail="inventory_not_found")


def get_metrics(store: dict[str, object]) -> MetricsResponse:
    """Return API metrics."""
    # TODO:
    # - Read counters from store["metrics"].
    # - Include inventory_keys as the number of current inventory rows.
    # - Return MetricsResponse.
    raise NotImplementedError


@app.post(
    "/events",
    response_model=IngestEventResponse,
    status_code=status.HTTP_200_OK,
)
def post_event(event: InventoryEventRequest) -> IngestEventResponse:
    """Ingest one inventory event."""
    result = ingest_event(event, STORE)
    return IngestEventResponse.model_validate(result)


@app.get(
    "/inventory/{sku}/{warehouse_id}",
    response_model=InventoryStateResponse,
)
def read_inventory(sku: str, warehouse_id: str) -> InventoryStateResponse:
    """Read current inventory state for one business key."""
    return get_inventory_state(sku, warehouse_id, STORE)


@app.get("/metrics", response_model=MetricsResponse)
def read_metrics() -> MetricsResponse:
    """Read compact API counters."""
    return get_metrics(STORE)


# =========================
# WRITTEN ANSWERS
# =========================

WRITTEN_ANSWERS = {
    "retry_safety": """
    TODO: Explain how this API should behave when a client retries the exact
    same event_id after a timeout. What should happen to inventory state and
    metrics?
    """,
    "rejected_records": """
    TODO: In production, where would you store rejected events and what fields
    would you include to make debugging possible?
    """,
    "scale_to_10k_rpm": """
    TODO: What would need to change if this endpoint received 10k requests per
    minute? Discuss API workers, durable storage, idempotency, and hot keys.
    """,
    "monitoring_and_security": """
    TODO: What metrics, alerts, and security controls would you add before
    exposing this ingestion endpoint to warehouse systems?
    """,
}


# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


def make_test_client() -> TestClient:
    STORE.clear()
    STORE.update(new_store())
    return TestClient(app)


def test_snapshot_event_updates_inventory_and_metrics() -> None:
    client = make_test_client()

    response = client.post(
        "/events",
        json={
            "event_id": "evt-100",
            "sku": " sku-100 ",
            "warehouse_id": "ams-1",
            "event_type": "snapshot",
            "quantity_on_hand": 14,
            "source_version": 4,
            "event_ts": "2026-05-19T09:00:00",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "event_id": "evt-100",
        "status": "accepted",
        "reason": None,
        "sku": "SKU-100",
        "warehouse_id": "AMS-1",
        "source_version": 4,
        "quantity_on_hand": 14,
    }

    inventory = client.get("/inventory/sku-100/ams-1")
    assert inventory.status_code == 200
    assert inventory.json()["quantity_on_hand"] == 14
    assert inventory.json()["source_version"] == 4

    metrics = client.get("/metrics")
    assert metrics.json()["received_events"] == 1
    assert metrics.json()["accepted_events"] == 1
    assert metrics.json()["inventory_keys"] == 1


def test_retry_same_event_is_duplicate_without_second_mutation() -> None:
    client = make_test_client()
    payload = {
        "event_id": "evt-retry",
        "sku": "SKU-100",
        "warehouse_id": "AMS-1",
        "event_type": "adjustment",
        "delta_quantity": 5,
        "source_version": 4,
        "event_ts": "2026-05-19T09:05:00",
    }

    first = client.post("/events", json=payload)
    second = client.post("/events", json=payload)

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["status"] == "accepted"
    assert second.json()["status"] == "duplicate"

    inventory = client.get("/inventory/SKU-100/AMS-1")
    assert inventory.json()["quantity_on_hand"] == 15

    metrics = client.get("/metrics").json()
    assert metrics["received_events"] == 2
    assert metrics["accepted_events"] == 1
    assert metrics["duplicate_events"] == 1


def test_stale_event_is_rejected_and_does_not_mutate_state() -> None:
    client = make_test_client()

    response = client.post(
        "/events",
        json={
            "event_id": "evt-stale",
            "sku": "SKU-100",
            "warehouse_id": "AMS-1",
            "event_type": "snapshot",
            "quantity_on_hand": 99,
            "source_version": 3,
            "event_ts": "2026-05-19T09:10:00",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "rejected"
    assert response.json()["reason"] == "stale_source_version"

    inventory = client.get("/inventory/SKU-100/AMS-1")
    assert inventory.json()["quantity_on_hand"] == 10

    metrics = client.get("/metrics").json()
    assert metrics["rejected_events"] == 1
    assert metrics["stale_events"] == 1


def test_invalid_business_payload_is_rejected() -> None:
    client = make_test_client()

    response = client.post(
        "/events",
        json={
            "event_id": "evt-invalid-shape",
            "sku": "SKU-200",
            "warehouse_id": "BUD-1",
            "event_type": "snapshot",
            "source_version": 1,
            "event_ts": "2026-05-19T09:15:00",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "rejected"
    assert response.json()["reason"] == "missing_quantity_on_hand"


def test_missing_inventory_returns_404() -> None:
    client = make_test_client()

    response = client.get("/inventory/sku-missing/ams-1")

    assert response.status_code == 404
    assert response.json()["detail"] == "inventory_not_found"


if __name__ == "__main__":
    print("FastAPI inventory event ingestion challenge")
    print("Run: uv run pytest challenges/2026-05-19_inventory_event_ingestion_api.py")
    print("Optional app object: challenges.2026-05-19_inventory_event_ingestion_api:app")


# When finished, submit this whole file for grading.
