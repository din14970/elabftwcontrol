.DEFAULT_GOAL := default

default: check build

build:
	poetry build -n -v

lint:
	black .
	isort .
	ruff ./src
	mypy .

register-test-repo:
	poetry config repositories.test-pypi https://test.pypi.org/legacy/

test:
	pytest -v

version-patch:
	poetry version patch

version-minor:
	poetry version minor

version-major:
	poetry version major

check: lint test

requirements:
	pip-compile requirements.in
	pip-compile dev-requirements.in
	pip-sync requirements.txt dev-requirements.txt
	pip install -e .

publish-dry:
	poetry publish --dry-run

publish-test:
	poetry publish -r test-pypi

publish:
	poetry publish

clean-cache:
	find . -type d -name "*__pycache__" | xargs rm -rf
