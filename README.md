# race-engineer-core

Open-source race intelligence core for live Formula 1 session analysis. Built around the OpenF1 data feed, structured race-state models, and MCP-compatible tooling.

## What this repo contains

- Structured domain models for F1 sessions, stints, tyres, gaps, and pit strategy
- OpenF1 adapter layer for fetching and normalising live session data
- Resolver primitives for answering race-state questions with evidence and confidence
- Strategy inference primitives (undercut, overcut, DRS train, safety car opportunity)
- Evaluation harness for grading answer quality
- Examples of structured race-state queries and expected responses
- MCP-compatible interfaces for tool use and agent orchestration

## Package structure

```
src/race_engineer_core/
    models/       domain entities and answer types
    adapters/     OpenF1 and external data source normalisation
    resolver/     question-answering primitives with evidence and confidence
    strategy/     strategy inference types and primitives
    evals/        evaluation harness and grading types
    examples/     example queries and expected responses
```

## Local setup

Requires Python 3.11+.

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
```

Run tests:

```bash
pytest tests/
```

Copy `.env.example` to `.env` and fill in any required values before running examples.

## Licence

See `LICENSE`.
