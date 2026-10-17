PY ?= python
export PYTHONPATH := src

.PHONY: install test lint demo render check-render

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests

demo:
	$(PY) -m elt generate
	$(PY) -m elt simulate

render:
	$(PY) -m elt render > snowflake/generated/pipeline.sql

check-render:
	$(PY) -m elt render | diff -u snowflake/generated/pipeline.sql -
