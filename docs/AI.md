# AI Project Context

This file is the compact project brief to give an AI assistant enough context
for useful discussion about this repository. Keep it current when project files,
workflow, conventions, or active practice material change.

## Project Purpose

This repository is the lightweight Python `data-engineering-pracitce` workspace.
It is not a full application. The main goal is to practice senior / lead data
engineering interview tasks through single-file challenges.

The preferred workflow is:

1. Generate one challenge file.
2. Solve the challenge inside that same file.
3. Run the embedded checks.
4. Ask for review or grading when ready.
5. Iterate on weak areas.

## Current Structure

- `main.py` is a minimal placeholder entry point.
- `challenges/` contains single-file interview challenges and related notes.
- `prompts/` contains reusable prompt templates.
- `.codex/skills/save-progress/SKILL.md` contains the project-only save-progress workflow.
- `docs/progress.md` contains the restartable daily progress log.
- `pyproject.toml` and `uv.lock` define the Python environment.
- `AGENTS.md` contains repository instructions for coding agents.
- `docs/AI.md` is this project-context handoff file.

## Challenge Conventions

Challenges should stay self-contained in one Python file under `challenges/`.
Use Python 3.12+ and prefer standard-library implementations unless the task
explicitly requires another dependency.

Generated challenge files should include these sections:

- `CHALLENGE DESCRIPTION`
- `GIVEN / STARTER DATA`
- `IMPLEMENTATION AREA`
- `WRITTEN ANSWERS`
- `SELF-CHECK / OPTIONAL TESTS`

The starter file should not include the solution. The user solves TODOs inside
the same file.

## Development Commands

Use `uv` for local commands:

```bash
uv run python challenges/<file>.py
uv run pytest challenges/<file>.py
uv run ruff check .
uv run mypy .
```

For a single challenge, prefer running checks against that file first:

```bash
uv run python challenges/<file>.py
uv run pytest challenges/<file>.py
uv run ruff check challenges/<file>.py
uv run mypy challenges/<file>.py
```

## Agent Behavior Preferences

The user uses AI as a tutor and senior pair programmer, not only as a code
generator.

When helping:

- Explain key design decisions.
- Keep changes small and understandable.
- Teach practical concepts tied to the current codebase.
- Do not grade unless the user explicitly asks for grading.
- If the user asks for help, guide with concepts and hints before giving code.
- If the user says not to show implementation or solution, do not provide code.

When grading:

- Inspect the file first.
- Run the challenge as a script and with pytest.
- Run ruff and mypy when useful.
- Give strict, practical feedback tied to interview expectations.
- Do not modify the solution while grading unless explicitly asked.

## Active Practice Context

Recent challenge topics include:

- Delta-style retry-safe customer profile ingestion.
- Clickstream sessionization with late-event and Spark reasoning.
- C-level online retail order reconciliation using a compact UCI Online Retail
  inspired extract.
- Inventory snapshot reconciliation with retry-safe updates, tombstones, stale
  version handling, and production observability.
- B-level FastAPI inventory event ingestion API with idempotent retries,
  source-version guards, rejected events, metrics, and TestClient checks.
- Senior / lead traceability domain modeling with source-record identity,
  business-entity resolution, lineage edges, audit events, compliance risk
  propagation, conflict detection, and explainability.
- Senior / lead PySpark + small local Delta Lake traceability practice covering
  explicit schemas, DataFrame validation, deterministic deduplication, lineage
  edge construction, downstream compliance-risk propagation, audit summaries,
  and optional retry-safe Delta MERGE patterns in
  `challenges/2026-05-30_traceability_lineage_pyspark_delta.py`.

Recent grading context:

- Current checkpoint in `challenges/2026-05-30_traceability_lineage_pyspark_delta.py`:
  `reject_invalid_records` is implemented with Spark DataFrame expressions, an
  `is_missing` helper, and array filtering to produce `rejection_reasons`
  without fake sentinel strings. `deduplicate_latest` has an initial
  window/`row_number` implementation. The focused rejection and deduplication
  tests pass, as does Ruff. Next, refine deduplication with a deterministic
  tie-breaker/drop helper columns, then implement `normalize_origins`.
- `challenges/traceability_DONE_domain_lineage_auditability.py` was graded at
  82 / 100 for implementation only, excluding written answers. Runtime, pytest,
  and ruff pass. Mypy still fails due to loose `dict[str, object]` shapes and
  optional normalize-result typing. The grade report is in
  `challenges/traceability_domain_lineage_auditability.md` and includes
  teacher-style written-answer guidance.
- `challenges/2026-05-18_inventory_snapshot_reconciliation.py` was graded at
  72 / 100 when grading only the implementation and written answers. The
  implementation passes runtime, pytest, and ruff checks but mypy fails.
  Idempotency and data-quality answers have useful signal; Spark/Delta and
  backfill answers remain below senior target.

These challenges are intended to practice data normalization, validation,
deduplication, deterministic ordering, retry safety, idempotency, Spark
performance reasoning, streaming concepts, order/revenue reconciliation,
customer-month aggregation, inventory state reconciliation, observability, and
written communication. The workspace now also includes FastAPI/Pydantic API
practice for data-engineering ingestion workflows, traceability-platform
modeling practice for auditability and sustainability contexts, and hands-on
PySpark/Delta traceability practice.

## Maintenance Rule

When project instructions, workflow, file structure, challenge conventions, or
active learning context change, update this file in the same change set so a new
AI assistant can quickly regain the necessary context.


## Project-Only Skill: save-progress

This repository has a local skill at `.codex/skills/save-progress/SKILL.md`.
Use it when the user asks to "save progress", "commit and push", or prepare the work so it can be continued tomorrow.

The skill maintains `docs/progress.md`, then commits and pushes the relevant changes. The progress file should summarize what was done, changed files, verification results, important decisions, and the next TODOs.
