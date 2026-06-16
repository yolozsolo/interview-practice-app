# Data Engineering Pracitce

This repo contains single-file technical data engineering practice challenges.

The workflow is intentionally simple:

1. Use `/interview-challenge` to create one challenge file.
2. Solve it inside that file.
3. Use `/interview-challenge` to grade the completed file.

## Setup

Run:

    uv init
    uv add --dev pytest ruff mypy

Optional later:

    uv add pandas
    uv add pyspark delta-spark

## Usage

Create a challenge:

    /interview-challenge
    Init a senior mixed challenge about Delta Lake MERGE and idempotent PySpark batch ingestion.

Run the challenge:

    uv run python challenges/<file>.py
    uv run pytest challenges/<file>.py

Grade the challenge:

    /interview-challenge
    Grade challenges/<file>.py strictly.

## Slash Commands

- `/interview-challenge` — generate or grade a single-file 60-minute challenge
- `/engineering-case` — generate or review a realistic C-level data engineering case
- `/save-progress` — commit and push current work with progress notes

## Constraint

Do not build a full app yet.

One challenge = one Python file.
