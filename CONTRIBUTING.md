# Contributing

## Development setup

```bash
python -m venv .venv
source .venv/Scripts/activate   # Windows
# source .venv/bin/activate     # macOS / Linux
pip install -e ".[dev]"
```

## Running tests

```bash
pytest                          # all tests
pytest tests/pipeline/          # pipeline only
pytest tests/sources/live_timing/  # live timing source only
pytest tests/sources/openf1/    # OpenF1 source only
```

## Linting and type checking

```bash
ruff check src/ tests/
mypy src/
```

## Pull request guidelines

- One logical change per PR.
- All tests must pass (`pytest`).
- `ruff check` and `mypy` must pass with no new errors.
- Update docstrings for any changed public API.
- Do not add features beyond what the PR description states.
