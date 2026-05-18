# Data Engineering Pracitce

This repo contains single-file technical data engineering practice challenges.

The workflow is intentionally simple:

1. Codex skill creates one challenge file.
2. I solve it inside that file.
3. Codex skill grades the completed file.

## Setup

Run:

    uv init
    uv add --dev pytest ruff mypy

Optional later:

    uv add pandas
    uv add pyspark delta-spark

## Usage

Create a challenge with Codex:

    Use the interview-challenge skill.
    Init a senior mixed challenge about Delta Lake MERGE and idempotent PySpark batch ingestion.

Run the challenge:

    uv run python challenges/<file>.py
    uv run pytest challenges/<file>.py

Grade the challenge with Codex:

    Use the interview-challenge skill.
    Grade challenges/<file>.py strictly.

## Constraint

Do not build a full app yet.

One challenge = one Python file.
