# Grade

  Score: 72 / 100
  Level estimate: acceptable

  # Compile / runtime concerns

  - uv run python challenges/2026-05-15_delta_retry_safe_ingestion.py passes.
  - uv run pytest challenges/2026-05-15_delta_retry_safe_ingestion.py passes: 5
    tests.
  - uv run ruff check challenges/2026-05-15_delta_retry_safe_ingestion.py fails on
    challenges/2026-05-15_delta_retry_safe_ingestion.py:152: f"invalid_updated_at" is
    an f-string with no placeholders.
  - uv run mypy challenges/2026-05-15_delta_retry_safe_ingestion.py fails because
    dateutil.parser has no installed type stubs.

  # Summary

  Your implementation solves the main happy path and passes the included tests. The
  ingestion flow is correctly structured: normalize, reject invalid rows, deduplicate
  by latest timestamp, then merge only newer updates. The main issues are validation
  strictness, deterministic ordering in deduplicate_latest, style/type quality, and
  reliance on dateutil for a problem that asked for UTC timestamps ending in Z.

  Written answers were excluded from this grade as requested.

  # What was good

  - Clear functional decomposition into small functions.
  - run_ingestion wires the pipeline together correctly.
  - Duplicate handling keeps the newer row, and equal timestamps keep the later
    source row.
  - Merge logic uses strict > comparison, which is the key retry-safety rule.
  - Existing test suite passes.

  # Missing or weak points

  - challenges/2026-05-15_delta_retry_safe_ingestion.py:164: validation checks raw
    values before stripping, so "   " is accepted as present.
  - challenges/2026-05-15_delta_retry_safe_ingestion.py:169: missing keys raise
    KeyError, not a clean rejection reason.
  - challenges/2026-05-15_delta_retry_safe_ingestion.py:223: output order depends on
    dict insertion order, not sorted customer_id order as requested.
  - Timestamp validation does not enforce “ending in Z” strictly.

  # Incorrect or risky points

  - except Exception in parse_utc_timestamp is too broad.
  - dateutil.parser.isoparse accepts more formats than the challenge contract
    requires.
  - merge_customers does not mutate the input lists, but it reuses the original row
    dictionaries in the returned result. That is usually acceptable here, but a
    stricter interpretation would copy rows.

  # Code feedback

  - Use stripped values for validation:

  customer_id = str(raw.get("customer_id", "")).strip()

  - Prefer standard library timestamp parsing for this task.
  - Sort deduplicated rows by customer_id.
  - Fix spacing and style issues around assignments and indentation.
  - Replace .keys() membership checks with direct dict membership. #HUMAN_COMMENT I dont undertand this

  # Written answer feedback

  - Not graded per your request.

  # Topics tested

  - Python data normalization
  - Validation and rejection handling
  - Deduplication by business key
  - Timestamp comparison
  - Retry-safe merge behavior
  - Idempotency
  - Type hygiene
  - Test coverage

  One useful next exercise: add tests for whitespace-only required fields and
  timestamps without trailing Z. Those will expose the two most important correctness
  gaps.
