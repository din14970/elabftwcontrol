.DEFAULT_GOAL := default

default: check build

build:
	uv build

check: format lint test

sync:
	uv sync --group dev --group lint --group format --group test --group lsp --all-extras

format:
	uv run ruff format src
	uv run ruff format tests
	uv run isort src
	uv run isort tests

lint:
	uv run ruff check
	uv run mypy src

test:
	uv run pytest -vv

test-debug:
	uv run pytest --pdb -vv

requirements:
	uv lock
	uv sync

publish-test:
	uv publish --publish-url https://test.pypi.org/legacy/

publish:
	uv publish

clean-cache:
	find . -type d -name "*__pycache__" | xargs rm -rf
