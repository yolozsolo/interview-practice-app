"""Clickstream sessionization and late-event handling challenge.

Title: Retry-Safe Clickstream Sessionization
Duration: 60 minutes
Difficulty: Senior / Lead data engineering
Topics tested:
- python.typing
- python.data_normalization
- python.testing
- spark.windowing
- spark.shuffle
- streaming.watermarking
- engineering.idempotency
- engineering.observability

Scenario:
You own a clickstream pipeline that receives web events from multiple clients.
Events can arrive out of order, some fields are messy, and a small number of
users produce a very large number of events. The business wants deterministic
sessions using a 30-minute inactivity timeout.

Tasks:
1. Normalize raw clickstream events into a consistent shape.
2. Reject invalid rows with clear reasons.
3. Assign deterministic session numbers per user.
4. Produce one aggregate row per session.
5. Explain how you would implement this in Spark or Structured Streaming.

Evaluation focus:
Correctness, timestamp handling, deterministic ordering, edge-case handling,
testing mindset, and clear reasoning about Spark windows, shuffle, skew,
watermarks, and retry-safe output.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, TypedDict


# =========================
# CHALLENGE DESCRIPTION
# =========================

# Implement the TODOs in the IMPLEMENTATION AREA.
#
# Business rules:
# - user_id is required and should be stripped.
# - event_id is required and should be stripped.
# - event_type is required and should be lowercased after trimming.
# - event_time is required and must be an ISO-8601 UTC timestamp ending in "Z".
# - page_url is optional; blank or missing page_url should become None.
# - Reject invalid rows, but continue processing the rest of the batch.
# - Deduplicate exact event_id duplicates, keeping the row with the latest
#   ingest_time. If ingest_time ties, keep the later row in the source batch.
# - Within each user, order events by event_time, then ingest_time, then event_id.
# - A new session starts when the gap from the previous event for that user is
#   greater than 30 minutes.
# - Return session aggregates in deterministic order by user_id and session_id.


# =========================
# GIVEN / STARTER DATA
# =========================


class EventRow(TypedDict):
    event_id: str
    user_id: str
    event_type: str
    event_time: str
    ingest_time: str
    page_url: str | None


class RejectedRow(TypedDict):
    row: dict[str, Any]
    reason: str


class SessionRow(TypedDict):
    session_id: str
    user_id: str
    started_at: str
    ended_at: str
    event_count: int
    page_view_count: int
    event_ids: list[str]


RAW_EVENTS: list[dict[str, Any]] = [
    {
        "event_id": " e001 ",
        "user_id": " U001 ",
        "event_type": " PAGE_VIEW ",
        "event_time": "2026-05-16T09:00:00Z",
        "ingest_time": "2026-05-16T09:00:05Z",
        "page_url": " /home ",
    },
    {
        "event_id": "e002",
        "user_id": "U001",
        "event_type": "click",
        "event_time": "2026-05-16T09:20:00Z",
        "ingest_time": "2026-05-16T09:20:03Z",
        "page_url": "",
    },
    {
        "event_id": "e003",
        "user_id": "U001",
        "event_type": "page_view",
        "event_time": "2026-05-16T10:00:01Z",
        "ingest_time": "2026-05-16T10:00:03Z",
        "page_url": "/pricing",
    },
    {
        "event_id": "e004",
        "user_id": "U002",
        "event_type": "page_view",
        "event_time": "2026-05-16T11:00:00Z",
        "ingest_time": "2026-05-16T11:00:03Z",
        "page_url": "/docs",
    },
    {
        "event_id": "e004",
        "user_id": "U002",
        "event_type": "page_view",
        "event_time": "2026-05-16T11:00:00Z",
        "ingest_time": "2026-05-16T11:01:00Z",
        "page_url": "/docs?ref=retry",
    },
    {
        "event_id": "",
        "user_id": "U003",
        "event_type": "page_view",
        "event_time": "2026-05-16T12:00:00Z",
        "ingest_time": "2026-05-16T12:00:01Z",
        "page_url": "/bad",
    },
    {
        "event_id": "e006",
        "user_id": "U003",
        "event_type": "page_view",
        "event_time": "not-a-timestamp",
        "ingest_time": "2026-05-16T12:00:01Z",
        "page_url": "/bad-time",
    },
]


# =========================
# IMPLEMENTATION AREA
# =========================


def parse_utc_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp ending with Z.

    - Strip whitespace before parsing.
    - Require the value to end with "Z".
    - Return a timezone-aware datetime in UTC.
    - Raise ValueError("invalid_timestamp") for invalid values.
    """
    s_value = value.strip()
    if not s_value or s_value[-1] != 'Z':
        raise ValueError("invalid timestamp")
    return datetime.fromisoformat(s_value)


def normalize_event(raw: dict[str, Any]) -> EventRow:
    """Normalize one raw event.

    TODO:
    - Validate required fields: event_id, user_id, event_type, event_time,
      ingest_time.
    - Strip event_id, user_id, event_type, event_time, ingest_time, page_url.
    - Lowercase event_type.
    - Convert blank or missing page_url to None.
    - Validate both event_time and ingest_time with parse_utc_timestamp.
    - Raise ValueError with reasons like "missing_event_id" or
      "invalid_event_time".
    """
    required_fields = ("event_id", "user_id", "event_type", "event_time", "ingest_time")

    #Validate missing keys
    for f in required_fields:
        if f not in raw.keys():
            raise ValueError(f"missing_{f}")
        
    #Validate missing values
    for k,v in raw.items():
        if k in required_fields:
            if not str(v).strip():
                raise ValueError(f"missing_{k}")
    
    clean_event_time = str(raw.get("event_time")).strip()
    try:
        parse_utc_timestamp(clean_event_time)
    except ValueError:
        raise ValueError("invalid_event_time")
    
    clean_ingest_time = str(raw.get("ingest_time")).strip()
    try:
        parse_utc_timestamp(clean_ingest_time)
    except ValueError:
        raise ValueError("invalid_ingest_time")
    
    clean_page_url = str(raw.get("page_url")).strip()
    if not str(clean_page_url).strip():
        clean_page_url = None
        
    return EventRow(
        event_id=str(raw.get("event_id")).strip(),
        user_id=str(raw.get("user_id")).strip(),
        event_type=str(raw.get("event_type")).strip().lower(),
        event_time=clean_event_time,
        ingest_time=clean_ingest_time,
        page_url=clean_page_url
    )


def normalize_batch(raw_rows: list[dict[str, Any]]) -> tuple[list[EventRow], list[RejectedRow]]:
    """Normalize all rows and collect rejections.

    TODO:
    - Return valid normalized rows and rejected rows.
    - Do not stop after the first invalid row.
    - Preserve the original invalid row in each RejectedRow.
    """
    event_rows: list[EventRow] = []
    rejected_rows: list[RejectedRow] = []
    
    for row in raw_rows:
        try:
            event_rows.append(normalize_event(row))
        except Exception as e:
            rejected_rows.append(RejectedRow(
                row=row,
                reason=str(e)
            ))

    return (event_rows, rejected_rows)

def deduplicate_events(rows: list[EventRow]) -> list[EventRow]:
    """Deduplicate by event_id.

    TODO:
    - For duplicate event_id values, keep the row with the newest ingest_time.
    - If ingest_time ties, keep the later row in the input list.
    - Return rows sorted deterministically by user_id, event_time, ingest_time,
      and event_id.
    """

    dedup: dict[str: EventRow] = {}

    for row in rows:
        event_id = row["event_id"]
        
        if event_id not in dedup:
            dedup[event_id] =  row
            continue
        
        if parse_utc_timestamp(row["ingest_time"]) >= parse_utc_timestamp(dedup[event_id]["ingest_time"]):
            dedup[event_id] = row

    list_dedup = [dedup[event_id] for event_id in dedup.keys()]
    return sorted(list_dedup, key= lambda r: (r["user_id"], r["event_time"], r["ingest_time"], r["event_id"]))


def build_sessions(rows: list[EventRow], timeout_minutes: int = 30) -> list[SessionRow]:
    """Build deterministic per-user session aggregates.

    - Events are already normalized and deduplicated, but do not assume sorted
      input.
    - Start session 1 for each user at their first event.
    - Start a new session when the gap from the previous event is greater than
      timeout_minutes.
    - Use session_id format "{user_id}_s{number}", for example "U001_s1".
    - Count page views where event_type == "page_view".
    - Keep event_ids in the ordered event sequence for that session.
    - Return sessions sorted by user_id and session_id.
    """
    from collections import OrderedDict
    from datetime import timedelta

    #sort the list
    sorted_rows = sorted(rows, key= lambda r: (r["user_id"], r["event_time"], r["ingest_time"], r["event_id"]))

    #initialize the groupby dict
    rows_by_id: OrderedDict[str, list[EventRow]] = OrderedDict()

    #populate the group by
    for row in sorted_rows:
        user_id=row["user_id"]
        rows_by_id.setdefault(user_id, []).append(row)

    #create session counter

    #initialize sessions
    session_events: dict[str, list[EventRow]] = {}

    #create sessions
    for user_id, events in rows_by_id.items():
        session_counter = 1
        session_id = f"{user_id}_s{session_counter}"

        #here I need to further breakdown int 30m intervals
        session_start_index = 0
        for i,_ in enumerate(events):
            if i == 0:
                continue

            previous_time = parse_utc_timestamp(events[i-1]["event_time"])
            current_time = parse_utc_timestamp(events[i]["event_time"])
            gap = current_time - previous_time

            if gap > timedelta(minutes=timeout_minutes):
                session_events[session_id] = events[session_start_index:i] #save
                session_counter += 1
                session_id = f"{user_id}_s{session_counter}" #create new id
                session_start_index = i
                

        #save remaining sessions
        session_events[session_id] = events[session_start_index:]

    #flat into session list
    sessions: list[SessionRow] = []
    for session_id, events in session_events.items():
        sessions.append({
            "session_id": session_id,
            "user_id": events[0]["user_id"],
            "started_at": events[0]["event_time"],
            "ended_at": events[-1]["event_time"],
            "event_count": len(events),
            "page_view_count": len([event for event in events if event["event_type"] == "page_view"]),
            "event_ids": [user_event["event_id"] for user_event in events],
        })

    return sorted(sessions, key=lambda s: (s["user_id"], s["session_id"]))

def run_sessionization(
    raw_rows: list[dict[str, Any]],
) -> tuple[list[SessionRow], list[RejectedRow]]:
    """Run the full batch sessionization flow.

    TODO:
    - Normalize the batch.
    - Deduplicate valid events.
    - Build sessions.
    - Return sessions and rejected rows.
    """
    norm_batch = normalize_batch(raw_rows=raw_rows)
    acc_batch = norm_batch[0]
    dedup_batch = deduplicate_events(acc_batch)
    sessions_batch = build_sessions(dedup_batch)

    return (sessions_batch, norm_batch[1])


# =========================
# WRITTEN ANSWERS
# =========================

WRITTEN_ANSWERS = """
Answer these after your implementation:

1. Spark implementation:
   How would you implement the session assignment using Spark DataFrame APIs or
   SQL window functions?

2. Shuffle and skew:
   Which operations in this pipeline cause shuffles, and how would you detect
   and mitigate skewed users with very large event volumes?

3. Streaming and late data:
   If this became a Structured Streaming job, how would you use watermarks and
   output mode? What data would be dropped or updated?

4. Retry safety:
   How would you make the final session output idempotent if the same source
   files or micro-batches are retried?

5. Observability:
   What metrics and data-quality checks would you emit for this pipeline?
"""


# =========================
# SELF-CHECK / OPTIONAL TESTS
# =========================


def test_normalize_event_trims_and_standardizes_values() -> None:
    row = normalize_event(
        {
            "event_id": " e100 ",
            "user_id": " U100 ",
            "event_type": " PAGE_VIEW ",
            "event_time": "2026-05-16T13:00:00Z ",
            "ingest_time": "2026-05-16T13:00:02Z ",
            "page_url": " /account ",
        }
    )

    assert row == {
        "event_id": "e100",
        "user_id": "U100",
        "event_type": "page_view",
        "event_time": "2026-05-16T13:00:00Z",
        "ingest_time": "2026-05-16T13:00:02Z",
        "page_url": "/account",
    }


def test_normalize_batch_collects_rejections() -> None:
    valid_rows, rejected_rows = normalize_batch(RAW_EVENTS)

    assert len(valid_rows) == 5
    assert len(rejected_rows) == 2
    assert {item["reason"] for item in rejected_rows} == {
        "missing_event_id",
        "invalid_event_time",
    }


def test_deduplicate_events_keeps_latest_ingest_time() -> None:
    valid_rows, _ = normalize_batch(RAW_EVENTS)
    deduplicated = deduplicate_events(valid_rows)

    matching = [row for row in deduplicated if row["event_id"] == "e004"]

    assert len(matching) == 1
    assert matching[0]["page_url"] == "/docs?ref=retry"
    assert matching[0]["ingest_time"] == "2026-05-16T11:01:00Z"


def test_run_sessionization_builds_expected_sessions() -> None:
    sessions, rejected_rows = run_sessionization(RAW_EVENTS)

    assert sessions == [
        {
            "session_id": "U001_s1",
            "user_id": "U001",
            "started_at": "2026-05-16T09:00:00Z",
            "ended_at": "2026-05-16T09:20:00Z",
            "event_count": 2,
            "page_view_count": 1,
            "event_ids": ["e001", "e002"],
        },
        {
            "session_id": "U001_s2",
            "user_id": "U001",
            "started_at": "2026-05-16T10:00:01Z",
            "ended_at": "2026-05-16T10:00:01Z",
            "event_count": 1,
            "page_view_count": 1,
            "event_ids": ["e003"],
        },
        {
            "session_id": "U002_s1",
            "user_id": "U002",
            "started_at": "2026-05-16T11:00:00Z",
            "ended_at": "2026-05-16T11:00:00Z",
            "event_count": 1,
            "page_view_count": 1,
            "event_ids": ["e004"],
        },
    ]
    assert len(rejected_rows) == 2


def test_session_boundary_uses_greater_than_timeout() -> None:
    rows = [
        {
            "event_id": "a",
            "user_id": "U999",
            "event_type": "page_view",
            "event_time": "2026-05-16T10:00:00Z",
            "ingest_time": "2026-05-16T10:00:01Z",
            "page_url": "/a",
        },
        {
            "event_id": "b",
            "user_id": "U999",
            "event_type": "click",
            "event_time": "2026-05-16T10:30:00Z",
            "ingest_time": "2026-05-16T10:30:01Z",
            "page_url": None,
        },
        {
            "event_id": "c",
            "user_id": "U999",
            "event_type": "page_view",
            "event_time": "2026-05-16T11:00:01Z",
            "ingest_time": "2026-05-16T11:00:02Z",
            "page_url": "/c",
        },
    ]

    sessions = build_sessions(rows)

    assert [session["event_ids"] for session in sessions] == [["a", "b"], ["c"]]


if __name__ == "__main__":
    print(
        "Challenge ready. Implement the TODOs, then run:\n"
        "  uv run python challenges/2026-05-16_clickstream_sessionization_late_events.py\n"
        "  uv run pytest challenges/2026-05-16_clickstream_sessionization_late_events.py"
    )


# When finished, submit this whole file for grading.
