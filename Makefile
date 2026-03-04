.PHONY: setup sources discover pipeline app test lint push

setup:
	uv sync

sources:
	uv run python src/test_sources.py

discover:
	uv run python src/discover_ids.py

pipeline:
	uv run python src/pipeline.py

app:
	uv run streamlit run app/streamlit_app.py

test:
	uv run pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	uv run ruff check src/ tests/ app/ && uv run black --check src/ tests/ app/

push: lint test
	git push origin main
