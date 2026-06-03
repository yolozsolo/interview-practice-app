---
name: save-progress
description: Project-only skill for ending a work session by documenting current progress, committing the work, and pushing it to the remote repository.
---

# save-progress

Use this skill only for this repository: `interview-practice-app`.

The goal is to make the repository restartable the next day. After this skill runs, the user should be able to open the progress file and quickly understand:

- what was done today,
- what changed in the repository,
- which checks passed or failed,
- what decisions were made,
- what remains to do next.

## Trigger phrases

Use this skill when the user says something like:

- "save progress"
- "commit and push"
- "end of day save"
- "write down where I am"
- "prepare this so I can continue tomorrow"
- "save current task progress"

## Progress file

Maintain this file:

```text
docs/progress.md
```

Do not create separate dated progress files unless the user explicitly asks. Keep the newest entry at the top so the file is useful immediately after opening it.

## Standard workflow

1. Inspect the repository state.

```bash
git status --short
git branch --show-current
git diff --stat
git diff --name-only
```

2. Identify the active task from the changed files, recent user messages, and existing progress file.

3. Run the smallest useful verification checks before committing.

For challenge-only work, prefer file-scoped commands first:

```bash
uv run python challenges/<file>.py
uv run pytest challenges/<file>.py
uv run ruff check challenges/<file>.py
uv run mypy challenges/<file>.py
```

For broader repository changes, consider:

```bash
uv run pytest
uv run ruff check .
uv run mypy .
```

If checks are slow, unavailable, or not relevant, record that clearly in `docs/progress.md` instead of pretending they passed.

4. Update `docs/progress.md` before committing.

Use this entry format:

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

Keep entries practical. Write for "tomorrow me", not for a formal report.

5. Review what will be committed.

```bash
git status --short
git diff --stat
```

6. Stage the relevant files.

```bash
git add <changed-files> docs/progress.md
```

Do not blindly stage unrelated local files, secrets, virtual environments, caches, notebooks with huge output, or generated data unless the user explicitly asks.

7. Commit with a short, descriptive message.

Recommended format:

```bash
git commit -m "Save progress on <task>"
```

For completed work, use a more specific message:

```bash
git commit -m "Implement <feature/task>"
```

8. Push the current branch.

```bash
git push
```

If the branch has no upstream, use the branch name from `git branch --show-current`:

```bash
git push -u origin <branch-name>
```

## Safety rules

- Never commit secrets, credentials, `.env` files, private keys, tokens, or large local data files.
- Never use destructive git commands such as `git reset --hard`, `git clean -fd`, or force-push unless the user explicitly asks and the risk is explained.
- If tests fail, still update `docs/progress.md` with the failure and next step. Commit only if the state is useful and the commit message/progress entry clearly says the work is in progress.
- If push fails because of authentication or remote divergence, do not guess. Report the exact problem and leave the local commit ready.
- Keep this skill project-only. Do not apply it to unrelated repositories.

## Final response after running the skill

Report only the important result:

- progress file updated,
- commit hash/message if commit succeeded,
- push result,
- checks run and whether they passed,
- the top next step from `docs/progress.md`.
