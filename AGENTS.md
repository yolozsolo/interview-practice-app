# Repository Guidelines

## Project Structure & Module Organization

This repository is the lightweight Python `data-engineering-pracitce` workspace, not a full application. Keep the workflow centered on single-file challenges.

- `main.py` contains the minimal project entry point.
- `challenges/` stores single-file challenge files and related notes.
- `docs/AI.md` stores compact project context for GPT-style discussion and should be kept current.
- `skills/SKILL.md` defines the local `interview-challenge` workflow for creating and grading practice tasks.
- `skills/references/` contains supporting grading material.
- `prompts/` stores reusable prompt templates.
- `pyproject.toml` and `uv.lock` define the Python 3.12+ environment and dependencies.

## Build, Test, and Development Commands

Use `uv` for dependency management and command execution.

- `uv sync` installs the locked environment.
- `uv run python main.py` runs the placeholder entry point.
- `uv run python challenges/<file>.py` runs a challenge file directly.
- `uv run pytest challenges/<file>.py` runs pytest-style self-checks embedded in a challenge.
- `uv run ruff check .` checks Python style and common issues.
- `uv run mypy .` runs static type checking.

## Coding Style & Naming Conventions

Target Python 3.12+. Use 4-space indentation, clear function names, and type hints for public helper functions inside challenge files. Prefer small pure functions that can be tested in the same file. Name challenge files with a date and topic slug, for example `2026-05-15_delta_merge_idempotency.py`.

For generated challenge files, follow the section markers described in `skills/SKILL.md` and keep the solution out of starter files.

## Testing Guidelines

Tests are usually embedded in each challenge file under a self-check section. Use pytest naming conventions: `test_<behavior>()` for test functions and explicit assertions for edge cases. When grading or validating a solution, run the challenge as a script and then run pytest against the same file.

## Commit & Pull Request Guidelines

There is no established Git history yet, so use concise imperative commit messages such as `Add delta merge challenge` or `Update grading rubric`. Pull requests should include the purpose, files changed, validation commands run, and any known limitations. For challenge changes, mention the target topic, difficulty, and expected runtime.

## Agent-Specific Instructions

Keep edits small and instructional. When creating challenges, add exactly one Python file unless asked otherwise. When grading, inspect and run the file first, then give strict, practical feedback tied to interview expectations.

Whenever changes are made to project instructions, workflow, file structure, challenge conventions, or active learning context, update `docs/AI.md` in the same change set so it remains a useful handoff document for future GPT discussions.
