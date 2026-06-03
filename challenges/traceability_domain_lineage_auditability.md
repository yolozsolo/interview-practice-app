# Grade

Score: 82 / 100
Level estimate: strong

# Compile / runtime concerns

- Scope of this grade: implementation only. Written answers were excluded from the score as requested.
- `uv run python challenges/traceability_DONE_domain_lineage_auditability.py` passes.
- `uv run pytest challenges/traceability_DONE_domain_lineage_auditability.py` passes: 9 tests.
- `uv run ruff check challenges/traceability_DONE_domain_lineage_auditability.py` passes.
- `uv run mypy challenges/traceability_DONE_domain_lineage_auditability.py` fails with 19 errors, mostly around optional normalize results, broad `dict[str, object]` entity shapes, conflict typing, and report values being typed as `object`.

# Summary

The implementation now solves the main traceability workflow in the challenge.
It normalizes and rejects bad source records, deduplicates source corrections,
builds upstream and downstream lineage, detects high-value conflicts, creates an
audit trail, propagates upstream compliance risk to the downstream product, and
produces a compact report that passes the embedded tests.

For an interview, this is a solid implementation-level signal. The main reason
it is not higher is type clarity and production hardening. The current code
works for the provided scenario, but several data shapes are represented as
`dict[str, object]`, which makes correctness harder to prove and is the source
of most static-analysis failures. Some edge cases are also simplified, such as
deduplication ties, validation strictness, and risk scoring calibration.

# What was good

- Entity identity is modeled clearly with stable IDs such as `PLOT:PLOT-A1`.
- Normalization converts source records into a dedicated `NormalizedRecord`
  dataclass.
- Invalid records are rejected instead of crashing the whole pipeline.
- Deduplication keeps the latest source version for each
  `(source_system, source_record_id)`.
- Entity resolution preserves source attribution and avoids silently merging
  conflicting attributes.
- The lineage graph correctly supports both upstream and downstream traversal.
- Graph traversal is iterative and cycle-safe through a visited set.
- Conflict detection finds meaningful disagreements such as `country` and
  `deforestation_risk`.
- The audit trail correctly includes source attribution, upstream lineage,
  conflict events, and compliance-risk events.
- The final report now correctly includes upstream source records and upstream
  conflicts, not only records directly attached to the product.
- Embedded tests cover the most important business behavior and now all pass.

# Missing or weak points

- `normalize_source_record` is annotated as returning
  `tuple[NormalizedRecord, dict[str, object]]`, but it actually returns either
  `(NormalizedRecord, None)` or `(None, rejection)`. The runtime behavior works,
  but the function contract is wrong.
- `normalize_source_records` reuses the name `results` for two different
  shapes: parse envelopes first, then normalized records. That makes the code
  harder to reason about and contributes to Mypy failures.
- `BusinessEntities = dict[str, dict[str, object]]` is too loose for the amount
  of structure the code expects. The implementation assumes nested fields like
  `attributes`, `source_record_ids`, and `latest_ingest_ts` have specific types,
  but the type system cannot verify that.
- Deduplication handles higher `source_version`, but it does not explicitly
  handle equal version with newer `ingest_ts`, even though the challenge
  description asks for that tie-breaker.
- The record fingerprint does not include attributes or parent IDs. Two records
  with the same metadata but different business attributes could get the same
  fingerprint.
- Rejected records are collected, but audit trail relevance for rejected records
  is not implemented.
- The traceability score is deterministic and testable, but the scoring weights
  are still simple constants without much explanation or calibration.

# Incorrect or risky points

- `datetime.fromisoformat` validates the sample timestamps, but it is permissive.
  A production source contract would usually define exact timestamp formats and
  timezone expectations.
- Missing field messages use a set representation. It happens to pass the
  current test for one missing field, but multiple missing fields could produce
  nondeterministic message ordering.
- `attributes = raw["attributes"]` accepts any value from the raw record. If a
  source sends a list or string, later code expects `.items()` and may fail.
- The audit trail marks both `"review"` and `"high"` deforestation risk as
  `risk_level="high"`. That may be acceptable for conservative reporting, but
  the choice should be explicit.
- Conflict severity is built as a plain string variable, then passed to a
  `Literal["low", "medium", "high"]` field. Runtime is fine, but the type
  checker cannot prove the value is valid.

# Code feedback

- Introduce a small result alias for normalization:

```python
NormalizeResult = tuple[NormalizedRecord | None, dict[str, object] | None]
```

- Better yet, split the success and failure paths into clearer structures:

```python
normalized, rejection = normalize_source_record(record)
if rejection is not None:
    rejected_records.append(rejection)
else:
    normalized_records.append(normalized)
```

- Add a `TypedDict` for resolved business entities instead of using
  `dict[str, object]` everywhere. This would make `source_record_ids.append`,
  `attributes[...]`, and `latest_ingest_ts` statically clear.
- Extend deduplication ranking to match the challenge text:
  `source_version`, then `ingest_ts`, then a deterministic final tie-breaker.
- Include attributes and parent IDs in the fingerprint input if the fingerprint
  is meant to identify record content.
- Convert report fields deliberately. For example, report conflicts as
  dictionaries because external report consumers should not need to know your
  internal dataclass types.
- Add hidden-edge tests for malformed `attributes`, equal-version corrections
  with different ingest times, multiple missing fields, and a small lineage
  cycle.

# Written answer feedback

- Not graded per your request.
- The written-answer section in the Python file is still blank. Teacher-style
  model answers are included below as study guidance rather than as part of the
  score.

# Topics tested

- Source-record identity vs business-entity identity
- Python normalization and validation
- Deterministic deduplication
- Entity resolution and source attribution
- Conflict detection across source systems
- Lineage graph construction
- Upstream and downstream graph traversal
- Audit-trail construction
- Compliance-risk propagation
- Traceability scoring
- Report shaping for audit consumers
- Type clarity and maintainability
- Production traceability reasoning

# Topic performance

- identity_modeling: strong — stable entity IDs and source IDs are handled separately.
- normalization_validation: okay — core validation works, but malformed nested shapes need stricter handling.
- deduplication_idempotency: okay — latest version wins, but tie-breaking by ingest time is incomplete.
- entity_resolution: strong — source attribution and conflict avoidance are represented.
- lineage_graphs: strong — upstream/downstream traversal is correct for the scenario.
- conflict_detection: strong — relevant high-risk conflicts are detected and reported.
- auditability: strong — audit events explain lineage, source attribution, conflicts, and risk.
- risk_propagation: strong — upstream plot risk reaches the downstream product report.
- traceability_scoring: okay — deterministic and useful, but simplistic.
- report_design: strong — final report includes upstream records and conflicts.
- typing_quality: weak — Mypy failures show several contracts are not accurately typed.
- production_readiness: okay — good in-memory model, but would need stronger contracts, observability, and persistence design.

# What I should study next

- `TypedDict` and dataclasses for row-shaped dictionaries.
- Versioned deduplication with deterministic tie-breakers.
- Designing audit/event tables for replayability.
- Modeling graph edges in relational/Delta tables.
- Idempotent ingestion patterns with source keys and payload hashes.
- Risk scoring: explainable weights, calibration, and avoiding double penalties.
- Validation that turns malformed input into structured rejected records.

# Better interview answer

The weakest implementation area is type clarity around structured dictionaries.
A stronger interview answer would be:

I would keep raw records flexible at the boundary, but convert them quickly into
typed internal structures. `NormalizedRecord`, `LineageEdge`, `Conflict`, and
`AuditEvent` should be dataclasses. For resolved entities, I would define a
`TypedDict` or dataclass with explicit fields: `entity_id`, `entity_type`,
`business_id`, `attributes`, `source_record_ids`, and `latest_ingest_ts`. During
aggregation I may use a list for `source_record_ids`, but before returning the
resolved entity I would convert it to a sorted tuple for deterministic audit
output. This keeps the runtime behavior clear and lets Mypy catch shape
mistakes before they become production bugs.

# Teacher-style written answer guidance

## 1. How would this model change if the data volume became petabyte scale?

Student instinct:

"I cannot keep this as Python lists and dictionaries anymore."

Teacher framing:

Correct. At petabyte scale, the first design change is moving from an in-memory
model to distributed storage and distributed compute. Your Python model is a
good domain prototype, but production needs tables, partitioning, incremental
processing, and bounded graph operations.

Stronger answer:

At petabyte scale, I would store raw source records, normalized records, lineage
edges, conflicts, and audit events in Delta tables. Normalization,
deduplication, conflict detection, and report preparation would run as Spark
jobs. I would avoid recursive in-memory graph traversal. Instead, I would
materialize lineage edges and either compute bounded-depth lineage with joins or
maintain precomputed audit views for important product batches.

The key shift is from "calculate everything in one process" to "incrementally
maintain audit-ready facts."

## 2. How would you represent business entities, source records, lineage edges, conflicts, and audit events in Delta Lake / Databricks tables?

Student instinct:

"Each concept in my code should become a table."

Teacher framing:

That is the right mapping. The important senior-level addition is separating raw
replayable data from current resolved state and audit facts.

Stronger answer:

I would use tables like:

- `bronze_source_records`: raw payload, `source_system`, `source_record_id`,
  `source_version`, `ingest_ts`, payload hash, batch id.
- `silver_normalized_records`: validated canonical records with `entity_id`,
  `entity_type`, `business_id`, attributes, parent IDs, and fingerprint.
- `gold_business_entities`: current resolved entity state and contributing
  source IDs.
- `lineage_edges`: `parent_entity_id`, `child_entity_id`, `source_record_id`,
  confidence, reason, valid-from, valid-to.
- `conflicts`: entity, attribute, values by source, source IDs, severity.
- `audit_events`: entity, event type, message, source IDs, risk level, audit run
  id, event timestamp.

The table design should let me replay raw data, rebuild state, and explain every
report.

## 3. How would you store lineage edges and audit events so they are queryable, replayable, and useful for regulatory audits?

Student instinct:

"Do not overwrite history."

Teacher framing:

Exactly. Auditors care about what you knew, when you knew it, and why a decision
was made. Append-first design is usually stronger than silently mutating old
facts.

Stronger answer:

I would store lineage edges and audit events in append-first Delta tables with
stable IDs, source record references, timestamps, and run IDs. If a correction
changes lineage, I would either append a new edge version or close the previous
edge with `valid_to`. Audit events should identify the source records and logic
that produced them.

For queryability, I would partition or cluster by entity type, entity ID, event
date, and source system depending on access patterns. For replayability, I
would keep immutable bronze records and versioned silver/gold outputs.

## 4. How would you make ingestion idempotent across retries, backfills, and corrections from source systems?

Student instinct:

"Use source-system IDs and versions as stable keys."

Teacher framing:

Good. Idempotency starts with stable identity. The next step is defining the
merge rule clearly.

Stronger answer:

I would use `(source_system, source_record_id, source_version)` as the source
record identity and keep a payload hash for exact duplicate detection. Retrying
the same batch should not create duplicate source records, lineage edges, or
audit events.

For current-state tables, I would use Delta `MERGE` with strict version guards:
update only when the incoming version is newer than the target version. Equal
versions are retries and should be ignored unless the payload hash differs, in
which case the row should be quarantined or flagged. Backfills should write to a
staging table first, deduplicate deterministically, validate expected deltas,
then merge.

## 5. How would you handle schema evolution when a new source system adds new compliance attributes or changes field names?

Student instinct:

"Add mapping logic before the canonical model."

Teacher framing:

Correct. Do not let every source system redefine your core model. Use a source
adapter layer.

Stronger answer:

I would keep source-specific parsing separate from the canonical traceability
model. Each source maps its raw fields into canonical fields and an attributes
map. New compliance attributes can land first in a flexible attributes map, then
be promoted to typed columns once they are important for reporting or controls.

For breaking source changes, I would version the contract, keep sample records
for tests, and quarantine records that violate critical expectations. Schema
evolution should be observable: new fields, missing fields, and mapping failures
should produce metrics and alerts.

## 6. How would you explain auditability to a non-technical sustainability stakeholder?

Student instinct:

"Explain it as proof behind the claim."

Teacher framing:

Yes. Avoid database language here. Talk about trust, evidence, and traceability.

Stronger answer:

Auditability means we can show the evidence behind every sustainability claim.
If we say a product batch has high deforestation risk, we can show which farm or
plot record created that risk, how that plot contributed to a lot, how the lot
moved through processing and shipment, and which finished product was affected.

It also means we can show disagreements between systems, when we received each
record, and why the final status was assigned. The goal is not just to produce a
label. The goal is to make the label explainable and defensible.

## 7. What monitoring would you add for a production traceability pipeline?

Student instinct:

"Monitor both pipeline health and business quality."

Teacher framing:

That is the right split. Senior answers include operational metrics,
data-quality metrics, and domain-specific risk metrics.

Stronger answer:

I would monitor ingestion volume by source, rejected record counts by reason,
duplicate and correction rates, late-arriving records, schema drift, conflict
counts by attribute, high-risk entity counts, downstream impacted product
counts, orphan entities, lineage edge counts, and audit report generation
failures.

I would also monitor pipeline SLAs: freshness, job duration, failed batches,
Delta merge metrics, and unexpected target row deltas. Alerts should fire on
sudden drops in source volume, spikes in rejection rate, spikes in conflicts, or
unexpected increases in high-risk downstream products.
