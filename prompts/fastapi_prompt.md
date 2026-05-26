Use the interview-challenge skill.

Create one B-level FastAPI challenge.

Topic:
FastAPI API around a realistic data-engineering workflow.

Goal:
Generate a single-file challenge that helps me practice FastAPI fundamentals in a data-engineering context.

Do not repeat or replace the existing interview-challenge skill rules. Follow the existing repository conventions for:
- one file under challenges/
- Python 3.12+
- existing challenge section markers
- TODO-based starter implementation
- no completed solution
- pytest-compatible self-checks
- strict interview-practice style

FastAPI-specific requirements:
- Use FastAPI and Pydantic.
- Create a FastAPI app in the challenge file.
- Include at least 2 endpoints.
- Include at least 1 request model and 1 response model.
- Include validation logic.
- Include at least one HTTPException path.
- Include tests using fastapi.testclient.TestClient.
- Use an in-memory store or compact starter dataset.
- Keep it solvable in 60–90 minutes.
- Do not create a full multi-file API project.

Choose one practical scenario from this family:
- webhook ingestion API
- IoT telemetry ingestion API
- inventory event ingestion API
- customer profile batch upload API
- rejected-record reporting API
- job submission and status API
- query API over cleaned aggregate data

The challenge should include at least three of these concepts:
- idempotency
- deduplication
- request/schema validation
- rejected records with reasons
- retry-safe behavior
- deterministic ordering
- status tracking
- aggregate query endpoint
- simple observability counters
- source versioning
- late-arriving events

The generated challenge should test:
- FastAPI route design
- Pydantic model design
- clean separation between endpoint layer and core logic, even inside one file
- validation and error handling
- testability with TestClient
- practical production reasoning

Written-answer prompts should focus on FastAPI + data engineering, for example:
- How would this API behave safely under client retries?
- Where would rejected records be stored in production?
- What would change at 10k requests/minute?
- How would you monitor failures and bad input rates?
- How would you secure the endpoint?
- How would you evolve the request schema without breaking clients?

Dependency handling:
- If FastAPI is already available in the project, use it.
- If dependency changes are needed, make the minimal pyproject.toml change only.
- Do not add a database, ORM, Alembic, Docker, authentication framework, or package architecture unless I explicitly ask.

Important:
The output should be the actual generated challenge file, not a generic explanation.
Do not solve the TODOs.
Do not create an engineering-case style dataset scenario unless I explicitly ask for C-level.