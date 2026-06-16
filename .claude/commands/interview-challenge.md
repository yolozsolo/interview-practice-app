---
description: Generate and grade single-file 60-minute interview practice challenges for Python, PySpark, Spark, Delta Lake, and data engineering interviews.
---

# Interview Challenge Command

Use this command when the user asks to create, initialize, generate, review, or grade a technical interview practice challenge.

The goal is to support a simple workflow:

1. Generate one valid Python challenge file.
2. The user solves the challenge inside that same file.
3. Grade the completed file strictly as a senior / lead data engineering interview submission.

This is not a full application. Do not create a CLI, database, dashboard, or multi-file framework unless the user explicitly asks.

The user wants to practice by editing at most one challenge file.

## Supported intents

Use this command for requests like:

- create a challenge
- init challenge
- generate an interview task
- make me a PySpark challenge
- grade this challenge
- review my completed challenge
- score this as an interview answer

## Workflow A: Initialize Challenge

When the user asks to initialize or create a challenge, create exactly one Python file under:

- `challenges/`

Use a filename like:

- `YYYY-MM-DD_short_topic_slug.py`

Example:

- `challenges/2026-05-15_delta_merge_idempotency.py`

If the user gives a topic, use it.

If no topic is given, choose one useful topic from:

- Python data normalization, typing, and testing
- PySpark DataFrame transformations
- Spark joins, shuffle, skew, and partitioning
- Delta Lake MERGE and idempotency
- Structured Streaming checkpointing and watermarking
- Data quality and observability
- Backfills and retry safety

The challenge must be:

- one Python file only
- valid Python 3.12+
- runnable with `uv run python <file>`
- testable with `uv run pytest <file>` if tests are included
- solvable in about 60 minutes
- suitable for senior / lead data engineering interview preparation

Prefer standard library only.

Do not require a real Spark cluster.

For PySpark / Delta Lake topics:

- use plain Python simulation when useful
- allow PySpark pseudocode in comments or written answers
- ask interview-style design questions
- do not require actual Spark execution unless the user explicitly asks

## Challenge file format

The generated file must include these exact section markers:

    # =========================
    # CHALLENGE DESCRIPTION
    # =========================

    # =========================
    # GIVEN / STARTER DATA
    # =========================

    # =========================
    # IMPLEMENTATION AREA
    # =========================

    # =========================
    # WRITTEN ANSWERS
    # =========================

    # =========================
    # SELF-CHECK / OPTIONAL TESTS
    # =========================

The file must include:

- `from __future__ import annotations`
- a top-level docstring describing:
  - title
  - duration
  - difficulty
  - topics tested
  - scenario
  - tasks
  - evaluation focus
- starter data or broken code
- clear TODOs
- one implementation area
- one written-answer area
- optional pytest-style tests
- optional `if __name__ == "__main__":` self-check

Do not include the solution.

At the bottom include:

    # When finished, submit this whole file for grading.

## Challenge generation quality rules

The challenge should test practical engineering ability, not trivia.

Good challenge themes:

- normalize messy source records
- deduplicate by business key
- design retry-safe ingestion
- simulate Delta MERGE logic
- reason about late-arriving data
- identify Spark performance bottlenecks
- explain partitioning and shuffle impact
- write tests for edge cases
- explain production observability

Avoid vague tasks like:

- Explain Spark
- What is Delta Lake?
- Write any pipeline
- Discuss performance

Make the task concrete.

## Workflow B: Grade Challenge

When the user asks to grade a completed challenge file, inspect the file and grade it strictly.

Evaluate:

1. Whether the file compiles
2. Whether the implementation likely works
3. Whether included tests pass or are meaningful
4. Python code quality
5. Type clarity
6. Edge-case handling
7. PySpark / Spark reasoning, if relevant
8. Delta Lake reasoning, if relevant
9. Performance awareness
10. Idempotency and retry-safety
11. Testing mindset
12. Production readiness
13. Communication clarity
14. Seniority signal

If possible, run:

- `uv run python <file>`
- `uv run pytest <file>`
- `uv run ruff check <file>`
- `uv run mypy <file>`

Do not modify the user's solution while grading unless explicitly asked.

## Grading output format

Return grading in this exact structure:

    # Grade

    Score: X / 100
    Level estimate: below target / acceptable / strong / excellent

    # Compile / runtime concerns

    - ...

    # Summary

    ...

    # What was good

    - ...

    # Missing or weak points

    - ...

    # Incorrect or risky points

    - ...

    # Code feedback

    - ...

    # Written answer feedback

    - ...

    # Topics tested

    - ...

    # Topic performance

    - topic.name: strong / okay / weak — short reason

    # What I should study next

    - ...

    # Better interview answer

    Provide a concise stronger version of the weakest part.

## Grading rules

Be strict.

Do not flatter the user.

If something is vague, mark it as vague.

If code does not compile, the score should be significantly reduced.

If the implementation works but ignores edge cases, explain that.

If tests are missing or shallow, say so.

If production concerns are missing, say so.

If Spark / Delta reasoning is only buzzwords, mark it as weak.

Reward:

- correctness
- clean Python
- explicit assumptions
- good edge-case handling
- good test cases
- idempotency thinking
- performance awareness
- clear interview communication

Do not rewrite the entire solution unless the user asks.

## Scoring guide

Use this rough scale:

- 90-100: excellent senior/lead-level answer
- 80-89: strong, interview-positive
- 70-79: acceptable but with notable gaps
- 60-69: borderline
- 40-59: below target
- 0-39: serious correctness or comprehension issues

Be honest. The purpose is improvement, not encouragement.

## Default topic taxonomy

Use these tags where relevant:

- `python.basics`
- `python.oop`
- `python.typing`
- `python.testing`
- `python.data_modeling`
- `python.error_handling`
- `pyspark.dataframe_api`
- `pyspark.joins`
- `pyspark.window_functions`
- `pyspark.partitioning`
- `pyspark.shuffle`
- `pyspark.batch`
- `pyspark.streaming`
- `pyspark.watermarking`
- `pyspark.checkpointing`
- `spark.architecture`
- `spark.driver_executor`
- `spark.performance`
- `spark.skew`
- `spark.adaptive_query_execution`
- `spark.file_sizing`
- `spark.resource_tuning`
- `delta.transaction_log`
- `delta.merge`
- `delta.schema_evolution`
- `delta.time_travel`
- `delta.optimize`
- `delta.vacuum`
- `delta.compaction`
- `delta.change_data_feed`
- `engineering.idempotency`
- `engineering.backfills`
- `engineering.data_quality`
- `engineering.observability`
- `engineering.testing`
- `engineering.retry_safety`
- `engineering.lineage`

## Important constraints

This command should keep the workflow simple.

Do not create:

- a full app
- a CLI
- a database
- a dashboard
- a package architecture
- multiple challenge files for one task

Unless explicitly requested, only create or grade one challenge file at a time.
