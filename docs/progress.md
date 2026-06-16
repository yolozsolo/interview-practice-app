# Progress

## Current task — 2026-06-16 — PySpark/Delta traceability challenge

**File:** `challenges/2026-05-30_traceability_lineage_pyspark_delta.py`

### Done

- `reject_invalid_records` — validates required fields, returns valid/invalid split, drops internal helper columns.
- `deduplicate_latest` — window-based dedup with `source_version` + `ingest_ts` + `_row_hash` tie-breaker (bug fixed: was referencing wrong DataFrame and wrong column name).
- `normalize_origins` — trims/lowercases, adds `entity_id`, parses `ingest_ts`, deduplicates.
- `normalize_batch_events` — validates, parses timestamps, deduplicates.
- `normalize_shipments` — validates, parses timestamps, deduplicates.
- `normalize_regulatory_declarations` — validates, parses timestamps, deduplicates.
- `build_lineage_edges` — unions batch-event rows and shipment rows into canonical edge schema; ORIGIN → LOT → BATCH → PRODUCT → DC chain verified.

### Verification

- PASS — `uv run ruff check challenges/2026-05-30_traceability_lineage_pyspark_delta.py`
- PASS (6/15) — `uv run pytest challenges/2026-05-30_traceability_lineage_pyspark_delta.py -q`
- FAIL (9/15) — all remaining stubs raise `NotImplementedError`; no regressions.

### Next

- [ ] Implement `propagate_compliance_risk` — BFS iterative join over `lineage_edges` from high-risk/rejected origins; flag all downstream `entity_id`s.
- [ ] Implement `detect_conflicting_attributes` — self-join origins on `entity_id`, compare `country`/`geo_risk_level`/`certification_status` across source systems.
- [ ] Implement audit summary and traceability report functions.
- [ ] Delta Lake tests (optional stretch): write/read bronze+silver, idempotent merge, lineage append.
