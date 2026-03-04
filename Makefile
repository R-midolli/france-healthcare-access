.PHONY: setup sources discover pipeline app test lint push

setup:
	python -m venv .venv && .venv/Scripts/pip install -r requirements-dev.txt

sources:
	python src/test_sources.py

discover:
	python src/discover_ids.py

pipeline:
	python src/pipeline.py

app:
	streamlit run app/streamlit_app.py

test:
	pytest tests/ -v --cov=src --cov-report=term-missing

lint:
	ruff check src/ tests/ app/ && black --check src/ tests/ app/

push: lint test
	git push origin main
