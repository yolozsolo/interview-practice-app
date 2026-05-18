# Grade

Score: 76 / 100
Level estimate: acceptable

# Compile / runtime concerns

- `uv run python challenges/2026-05-16_clickstream_sessionization_late_events.py` passes.
- `uv run pytest challenges/2026-05-16_clickstream_sessionization_late_events.py` passes: 5 tests.
- `uv run ruff check challenges/2026-05-16_clickstream_sessionization_late_events.py` passes.
- `uv run mypy challenges/2026-05-16_clickstream_sessionization_late_events.py` fails with 5 errors:
  - `page_url` is inferred as `str`, then assigned `None`.
  - `save_session` is annotated as returning `list[SessionRow]` but returns one dictionary.
  - `sessions.append(...)` receives the incorrectly annotated `list[SessionRow]`.
  - One test fixture is inferred as `list[object]`, not `list[EventRow]`.

# Summary

The implementation is functionally solid for the provided batch examples. It
normalizes records, collects rejections, deduplicates by latest `ingest_time`,
builds deterministic sessions, and passes the embedded pytest suite. The core
algorithm shows practical understanding of ordered sessionization.

For a senior / lead interview, the main gaps are type discipline, a few hidden
data-quality edge cases, and the written answers. The code would likely pass a
basic functional screen, but the Spark, streaming, idempotency, and production
reasoning are not yet strong enough for a confident senior signal.

# What was good

- The end-to-end flow is implemented cleanly: normalize, reject invalid rows,
  deduplicate, sessionize, and return rejected rows.
- The provided tests pass, including the important `gap > 30 minutes` boundary.
- Deduplication correctly keeps the duplicate event with the newest
  `ingest_time`, and the `>=` comparison also preserves the later source row on
  ties.
- `build_sessions` sorts internally, so it does not rely on caller-provided
  ordering.
- The session aggregate fields are correct for the included examples:
  `started_at`, `ended_at`, `event_count`, `page_view_count`, and ordered
  `event_ids`.
- The code stays self-contained and uses standard-library tools appropriately
  for this challenge.

# Missing or weak points

- Missing optional `page_url` becomes the string `"None"` instead of Python
  `None`. Blank `page_url` is handled correctly, but a truly absent `page_url`
  is not.
- Required fields with value `None` can be accepted incorrectly for string
  fields because `str(None).strip()` becomes `"None"`.
- `parse_utc_timestamp` catches broad `Exception` during parsing. This works for
  the exercise, but production validation should catch expected parse failures
  and preserve programming errors.
- The helper `save_session` has a misleading return type and a typo in the
  parameter name: `sessiond_id`. It also ignores that parameter and instead
  closes over `session_id` from the outer scope.
- Tests cover the happy path and a few edge cases, but not missing `page_url`,
  `None` required values, equal-ingest duplicate tie behavior, unsorted input, or
  more than 9 sessions for one user.

# Incorrect or risky points

- Sorting final sessions by string `session_id` can produce the wrong order once
  a user reaches `s10`, because lexical sorting places `U001_s10` before
  `U001_s2`.
- The type errors are real maintainability risks. The runtime behavior happens
  to work, but the annotations currently describe different shapes from what the
  functions return.
- The Spark implementation answer is too generic. `rank()` or `row_number()`
  alone does not assign inactivity-based sessions; you need `lag(event_time)`,
  a new-session flag, and a cumulative sum per user.
- The streaming answer says to use the 30-minute timeout as the watermark. The
  session gap and allowed lateness are separate concepts: the session timeout
  defines when a new session starts, while the watermark controls how long state
  is retained for late events.
- The retry-safety answer, "I think it is already idempotent," is not enough.
  Idempotency needs stable keys and deterministic overwrite/merge behavior in
  the target output.

# Code feedback

- In `normalize_event`, clean each required value first, then validate against
  `None` and blank strings explicitly. This avoids accidentally accepting
  `"None"` as a real value.
- Treat `page_url` separately from required fields: if the key is missing or the
  stripped value is blank, return `None`; otherwise return the stripped string.
- Change `save_session` to return `SessionRow`, not `list[SessionRow]`, and pass
  `session_id` explicitly instead of relying on the outer variable.
- Sort sessions by `(user_id, numeric_session_number)` or append sessions in
  already-correct order and avoid a final lexical sort by generated ID.
- Add focused tests for the hidden edge cases above. The practical concept here
  is that interview tests should prove business rules, not only match the sample
  data.

# Written answer feedback

- Spark implementation: mention `Window.partitionBy("user_id").orderBy(...)`,
  `lag(event_time)`, compute `gap_minutes`, create an `is_new_session` flag, and
  use cumulative `sum(is_new_session)` to derive session numbers.
- Shuffle and skew: deduplication by `event_id`, per-user windowing, grouping,
  and global ordering can all require shuffles. Detection should include Spark
  UI stage/task skew, spill, shuffle read/write, and extreme partition sizes.
- Skew mitigation needs more concrete options: identify heavy users, isolate or
  special-case them, choose partitioning intentionally, and avoid unnecessary
  global sorts.
- Streaming: discuss event-time watermarks, late-event policy, state retention,
  and append vs update mode based on when sessions are considered finalized.
- Retry safety: explain deterministic session IDs, idempotent writes keyed by
  stable identifiers, and source/micro-batch replay handling.
- Observability: expand beyond counts. Include reject reasons, duplicate rate,
  late-event rate, watermark lag, state size, skew indicators, session-count
  distribution, and output merge/update failures.

# Topics tested

- Python normalization and validation
- Optional-field handling
- Timestamp parsing and comparison
- Deduplication by business key
- Deterministic ordering
- Sessionization with inactivity timeout
- Pytest-based self-checking
- `TypedDict` and static typing clarity
- Spark window-function reasoning
- Shuffle and skew awareness
- Structured Streaming watermark reasoning
- Retry-safe output design
- Production observability
