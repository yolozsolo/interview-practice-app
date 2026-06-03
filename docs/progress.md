# Progress Log

Newest entries go first. This file exists so the next work session can start quickly without reconstructing context from memory or chat history.

## 2026-06-03 — Finish and grade pure-Python traceability challenge

### Done

- Finished the core implementation for `challenges/traceability_DONE_domain_lineage_auditability.py`.
- Fixed the audit/report path so downstream product reports include upstream lineage, upstream source records, upstream conflicts, propagated compliance risk, and a deterministic traceability score.
- Ran the embedded checks until the pure-Python traceability challenge passed runtime, pytest, and Ruff.
- Graded the implementation only, excluding written answers, and created a Markdown grade report with teacher-style model answers for the written questions.
- Updated `docs/AI.md` with the new grading context.

### Changed files

- `challenges/traceability_DONE_domain_lineage_auditability.py` — completed implementation fixes for normalization determinism, source attribution, lineage-aware reports, risk propagation, conflict reporting, and scoring.
- `challenges/traceability_domain_lineage_auditability.md` — new grade report: 82 / 100, implementation-only, with teacher-style written-answer guidance.
- `docs/AI.md` — added the traceability grading result and current static-typing caveat.
- `docs/progress.md` — added this handoff entry.

### Verification

- PASS — `uv run python challenges/traceability_DONE_domain_lineage_auditability.py`
- PASS — `uv run pytest challenges/traceability_DONE_domain_lineage_auditability.py`
- PASS — `uv run ruff check challenges/traceability_DONE_domain_lineage_auditability.py`
- FAIL — `uv run mypy challenges/traceability_DONE_domain_lineage_auditability.py`
- Notes: Mypy still reports 19 errors, mostly from optional normalization-result typing and broad `dict[str, object]` shapes in resolved entities, conflict serialization, and report checks.

### Decisions / Notes

- Written answers were not graded, per request, but the grade report includes model answers in a teacher-student style for study.
- The implementation is functionally complete for the embedded tests, but static type cleanup remains a separate improvement task.
- The next active work is the PySpark/Delta traceability challenge starter file.

### Next

- [ ] Continue with `challenges/2026-05-30_traceability_lineage_pyspark_delta.py`.
- [ ] Start by implementing `reject_invalid_records`, `deduplicate_latest_source_records`, and `normalize_origins`.
- [ ] Later, clean up Mypy issues in `challenges/traceability_DONE_domain_lineage_auditability.py` if type hygiene becomes the focus.

## 2026-06-03 — Traceability challenge progress save

### Done

- Added a senior PySpark/Delta traceability starter challenge covering schemas, validation, deterministic deduplication, lineage edges, risk propagation, audit summaries, and optional Delta writes/MERGE practice.
- Continued implementation work in the pure-Python traceability/auditability challenge, especially audit-trail construction for upstream lineage, conflicts, and compliance risk events.
- Added the project-local `save-progress` skill workflow so end-of-session handoffs can be documented, committed, and pushed consistently.
- Updated `docs/AI.md` with the new PySpark/Delta challenge context and restartable progress-log workflow.

### Changed files

- `challenges/2026-05-30_traceability_lineage_pyspark_delta.py` — new single-file PySpark/Delta traceability interview challenge with embedded pytest checks.
- `challenges/traceability_domain_lineage_auditability.py` — in-progress implementation changes for audit trail behavior and propagated risk visibility.
- `.codex/skills/save-progress/SKILL.md` — local workflow for saving progress, updating this file, committing, and pushing.
- `docs/AI.md` — added current challenge/workflow context for future GPT handoffs.
- `docs/progress.md` — added this restartable progress entry.

### Verification

- PASS — `uv run python challenges/traceability_domain_lineage_auditability.py`
- FAIL — `uv run pytest challenges/traceability_domain_lineage_auditability.py`
- FAIL — `uv run python challenges/2026-05-30_traceability_lineage_pyspark_delta.py`
- FAIL — `uv run pytest challenges/2026-05-30_traceability_lineage_pyspark_delta.py`
- FAIL — `uv run ruff check challenges/traceability_domain_lineage_auditability.py challenges/2026-05-30_traceability_lineage_pyspark_delta.py`
- FAIL — `uv run mypy challenges/traceability_domain_lineage_auditability.py challenges/2026-05-30_traceability_lineage_pyspark_delta.py`
- Notes: the pure-Python challenge has 6 passing and 3 failing tests. Remaining failures are `test_resolved_entities_keep_source_record_attribution`, `test_traceability_score_penalizes_risk_and_conflicts`, and `test_report_propagates_compliance_risk_to_downstream_product`.
- Notes: the PySpark/Delta challenge is intentionally still a starter file; pytest has 1 passing test and 14 expected `NotImplementedError` failures.
- Notes: Ruff currently reports unused `Enum`, a lambda assignment, and unused `entity_errors` in the pure-Python challenge. Mypy reports 22 errors across the two challenge files.

### Decisions / Notes

- Keep the new PySpark/Delta challenge as a starter file with TODOs and tests rather than adding a solution.
- Treat the pure-Python traceability challenge as in-progress and commit the current learning checkpoint with failing checks documented.
- Use `docs/progress.md` as the single newest-first handoff file.

### Next

- [ ] Finish `calculate_traceability_score` and `produce_traceability_report` in `challenges/traceability_domain_lineage_auditability.py`.
- [ ] Fix the normalized record count/source-attribution regression in `test_resolved_entities_keep_source_record_attribution`.
- [ ] Clean up Ruff and Mypy issues after the remaining behavior is implemented.

## Template

```markdown
## YYYY-MM-DD — <short task title>

### Done

- ...

### Changed files

- `path/to/file.py` — short explanation

### Verification

- PASS/FAIL/NOT RUN — `command`
- Notes about failures or skipped checks

### Decisions / Notes

- ...

### Next

- [ ] Most important next step
- [ ] Second next step
- [ ] Optional cleanup/follow-up
```
