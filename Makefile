.PHONY: sync lint format test check

sync:
	uv sync --all-groups

lint:
	uv run ruff check .

format:
	uv run ruff format .

test:
	uv run pytest -v

check: lint test
