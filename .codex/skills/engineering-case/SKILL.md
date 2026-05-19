---
name: engineering-case
description: Generate realistic C-level data engineering and data management case scenarios using public datasets or compact simulated extracts.
---

# Engineering Case Skill

Use this skill when the user asks to create, generate, design, review, or practice a realistic data engineering / data management case scenario.

This skill is for C-level practice:

- realistic domain context
- messy source data
- multiple entities or tables
- unclear business rules
- data quality issues
- implementation task
- written assumptions
- production follow-up reasoning

This is different from a small muscle-memory drill and different from a short interview coding task.

## Relationship to other practice levels

- A-level: micro-pattern drills, handled by `python-muscle-memory`
- B-level: medium implementation challenge, handled by `interview-challenge`
- C-level: realistic domain case, handled by this skill
- D-level: production/system reasoning, may be included as follow-up questions
- E-level: real project transfer, only if the user explicitly asks

## Supported intents

Use this skill for requests like:

- create a C-level case
- generate an engineering case
- create a realistic data engineering case
- make me a domain case using a public dataset
- generate a data management case
- create an MDM / ingestion / reconciliation / traceability case
- review my case solution from a domain/data-engineering perspective

## Case generation principles

The case should feel like realistic work, not trivia.

A good case should include:

1. Business context
2. Source systems or source files
3. Dataset reference
4. Entity definitions
5. Input contracts
6. Messy data conditions
7. Business rules
8. Implementation task
9. Expected outputs
10. Data quality checks
11. Assumptions section
12. Written answers
13. Production follow-up questions
14. Related A-level muscle-memory drills
15. Review rubric

The user should practice usable engineering knowledge, not only interview answers.

## Dataset sourcing

When generating a case, prefer one of:

- Kaggle public datasets
- UCI Machine Learning Repository
- data.gov / EU open data portals
- public cloud sample datasets
- official open datasets
- compact simulated extracts based on public dataset schemas

If the user has internet access or explicitly wants real data, suggest a public dataset and link/source it.

If the user wants a self-contained challenge, create a compact simulated extract inside the challenge file based on the dataset schema.

Always mention dataset license or usage caveat when known.

## Good dataset families

Recommended starter datasets:

- Olist Brazilian E-Commerce Public Dataset
- UCI Online Retail
- Instacart Market Basket Analysis
- NYC TLC taxi trips
- Chicago crimes
- OpenAQ air quality
- NOAA weather
- Medicare / CMS public data
- Stack Exchange data dump
- GitHub public event archive
- GDELT event data

## Good C-level domains

Use domains such as:

- e-commerce order lifecycle
- customer MDM
- product mastering
- seller/vendor performance
- delivery SLA monitoring
- clickstream/sessionization
- IoT telemetry
- document/invoice extraction
- regulatory traceability
- inventory movement
- financial reconciliation
- data quality monitoring
- lakehouse ingestion
- reference data management

## Output format for generated cases

Return the case in this structure:

# Engineering Case

## Case title

## Level

C-level engineering case

## Suggested dataset

Include:
- dataset name
- source
- why it fits
- license / usage caveat if known

## Scenario

Describe the realistic business situation.

## Source data

Describe the source tables/files.

## Business problem

Describe what the data team must build.

## Business rules

Concrete rules the implementation must follow.

## Messy data conditions

Include realistic data quality problems.

## Implementation task

Specify what the user must build.

Prefer one of these formats:

- single Python file
- one notebook
- one SQL file
- one PySpark file
- one markdown design plus one implementation file

Default to one Python file unless the user asks otherwise.

## Expected outputs

Specify exact output datasets/tables/reports.

## Acceptance criteria

Clear checklist for correctness.

## Written answers

Ask the user to explain assumptions, edge cases, and tradeoffs.

## Production follow-up questions

Ask D-level questions about scale, reliability, observability, backfills, schema evolution, lineage, and ownership.

## Related A-level drills

List small muscle-memory drills that support the case.

## Review rubric

Give strict grading dimensions.

## Challenge file skeleton

If useful, include a starter Python file skeleton with TODOs.

Do not include the full solution unless the user explicitly asks.

## Review mode

When reviewing a completed engineering case, grade:

1. Domain understanding
2. Correctness of business rules
3. Data modeling
4. Data quality handling
5. Implementation clarity
6. Edge cases
7. Test quality
8. Assumptions
9. Production reasoning
10. Communication
11. Transferability to real work

Be strict and practical.

## Important constraints

Do not turn the case into a full application unless the user explicitly asks.

Do not create dashboards or UI unless requested.

Do not over-focus on interview performance.

The goal is usable engineering competence.