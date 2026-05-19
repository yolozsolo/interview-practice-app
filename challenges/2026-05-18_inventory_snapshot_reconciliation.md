# Grade

Score: 72 / 100
Level estimate: acceptable

# Compile / runtime concerns

- Scope of this grade: only `IMPLEMENTATION AREA` and `WRITTEN ANSWERS`.
- `uv run python challenges/2026-05-18_inventory_snapshot_reconciliation.py` passes.
- `uv run pytest challenges/2026-05-18_inventory_snapshot_reconciliation.py` passes: 3 tests.
- `uv run ruff check challenges/2026-05-18_inventory_snapshot_reconciliation.py` passes.
- `uv run mypy challenges/2026-05-18_inventory_snapshot_reconciliation.py` fails with 5 errors in the implementation around `object` values being used as `int`, `str`, tuple keys, and comparison operands.

# Summary

The implementation solves the main reconciliation behavior for the provided
scenario. It normalizes keys, validates common bad records, deduplicates by
business key, applies strict version-aware mutations, handles tombstones, and
keeps retries idempotent. That is the most important part of this challenge.

The grade is capped by type-safety problems, a few validation edge cases, and
weak written answers for Spark/Delta and backfill design. The written
idempotency answer has the right core idea, and the data-quality answer is
reasonable, but the design answers are not yet senior-level.

# What was good

- `normalize_record` correctly strips and uppercases `sku` and `warehouse_id`.
- Active records parse `quantity_on_hand` to `int` and reject negative values.
- Tombstones are represented separately from active target rows.
- `deduplicate_records` uses the right business key: `(sku, warehouse_id)`.
- Deduplication ranks by `source_version`, then `event_ts`, then `ingest_id`,
  which satisfies the deterministic winner requirement.
- `apply_inventory_batch` uses strict `>` version comparison before update or
  delete, which is the key retry-safety invariant.
- Equal-version and lower-version records are ignored, so the same batch can be
  replayed without double-counting mutations.
- Output rows are sorted by business key, making the result deterministic.
- The idempotency written answer identifies the important update/delete replay
  behavior.
- The data-quality written answer includes useful operational counters:
  received records, invalid records, stale records, deduplicated records,
  deletes, and updates.

# Missing or weak points

- `quantity_on_hand` is required for every raw record at line 202. The task says
  tombstone records should allow `quantity_on_hand` to be missing or `None`, so a
  delete without that key would be rejected before `is_deleted` is considered.
- The implementation uses broad `dict[str, object]` rows throughout. This keeps
  the code compact, but it is why mypy cannot prove that `source_version`,
  `event_ts`, `sku`, and `warehouse_id` have the expected types.
- The report stores bare error strings only. For reconciliation, reason counts
  plus record identity would be more useful.
- `deduplicated_records` is correct but indirect:
  `len(norm_batch) - (len(norm_batch) - len(source))` should just be
  `len(source)`.
- Missing-field messages are built from a set, so multiple missing fields can be
  reported in nondeterministic order.
- `source_version` validation accepts booleans and non-positive integers through
  `int(...)`; a stricter implementation would reject those.

# Incorrect or risky points

- The idempotency answer says only update and delete need attention because
  there is no new record on reapply. Insert replay also matters: the inserted row
  already exists on the second run and must be ignored because the version is
  equal.
- `datetime.fromisoformat` is a permissive timestamp check. It is acceptable for
  this exercise, but production code should match the actual source contract.
- `business_key` returns values typed as `object`, which works at runtime for the
  sample but weakens static guarantees.
- `rank_key` is typed as returning `tuple[int, datetime, str]`, but the values
  are pulled from `dict[str, object]`, so the annotation is not actually proven.
- Spark/Delta design is not answered beyond saying you need practice.
- Backfill strategy mentions snapshot and rollback, but does not explain how the
  corrected source data would be staged, deduplicated, merged safely, audited, or
  validated.

# Code feedback

- For tombstones, check `is_deleted` before requiring `quantity_on_hand`, or only
  require `quantity_on_hand` when `is_deleted` is false.
- Introduce a `TypedDict` for normalized records with concrete field types. That
  would make `rank_key`, `business_key`, and the version comparison much safer.
- Give `business_key` an explicit return type: `tuple[str, str]`.
- Use `deduplicated_records = len(source)` for readability.
- Sort missing fields before joining them:
  `",".join(sorted(missing))`.
- Consider returning structured error details like
  `{"reason": "invalid_sku", "sku": raw.get("sku"), "warehouse_id": ...}`.

# Written answer feedback

- Idempotency: directionally correct. Improve it by stating the invariant first:
  target changes only when incoming `source_version` is strictly greater than
  target `source_version`. Then cover insert replay, update replay, delete
  replay, equal versions, and stale lower versions.
- Spark/Delta: weak. The answer does not describe staging, deduplication,
  `MERGE` conditions, tombstone handling, or the business key.
- Data quality: acceptable. The chosen metrics are relevant. Make them more
  senior-level by adding thresholds, reason-level breakdowns, source batch IDs,
  target row deltas, and alert ownership.
- Backfill: weak. Snapshot and rollback are only part of the answer. A safe
  backfill also needs a staged correction table, version-guarded merge, audit
  output, validation queries, and a rollback mechanism.
- Communication: understandable, but tighten grammar and structure. In
  interviews, concise invariants are stronger than long prose.

# Topics tested

- Python data normalization and validation
- Deduplication by business key
- Deterministic tie-breaking
- Retry-safe incremental ingestion
- Tombstone/delete handling
- Version-aware merge semantics
- Reconciliation reporting
- Static typing discipline
- Spark and Delta Lake design reasoning
- Backfill safety and auditability
- Production observability

# Topic performance

- normalization.validation: okay — handles common cases, but tombstone
  `quantity_on_hand` handling and strict type validation need work.
- deduplication: strong — ranking logic matches the requested business rules.
- retry_safety: strong — strict version comparison gives the right replay
  behavior.
- tombstones: okay — existing deletes work, but missing quantity on tombstones is
  mishandled.
- reconciliation_reporting: okay — counters are present, but error detail is too
  thin.
- typing_quality: weak — mypy fails on central implementation assumptions.
- code_style: okay — ruff passes, but formatting and type clarity can improve.
- written_communication: okay — idempotency and metrics show understanding;
  Spark/Delta and backfill are weak.
- Spark.Delta.reasoning: weak — not substantively answered.
- production_readiness: okay — some observability thinking is present, but
  backfill and audit strategy are underdeveloped.

# What I should study next

- Delta Lake `MERGE` for versioned upserts and tombstones.
- Spark window deduplication with `row_number` over business keys.
- Safe backfill design: staging, version guards, audit tables, validation, and
  rollback.
- `TypedDict` for row-shaped dictionaries.
- Validation patterns that convert malformed input into rejected rows instead of
  runtime exceptions.

# Better interview answer

A stronger Spark/Delta answer would be:

I would stage the raw batch, normalize `sku` and `warehouse_id`, validate rows,
and write invalid records to a quarantine table with rejection reasons. For valid
records, I would deduplicate with `row_number` over
`partitionBy("sku", "warehouse_id")`, ordered by descending `source_version`,
descending `event_ts`, and a deterministic final tie-breaker. Then I would merge
into the Delta target on `sku` and `warehouse_id`. Matched active rows update
only when `source.source_version > target.source_version`. Matched tombstones
delete only when the tombstone version is newer. Not-matched rows insert only
when `is_deleted = false`. Equal and lower versions are ignored, which makes
batch retries safe.
