.PHONY: checks start replay verify snapshot recovery-test catalog-generate catalog-load stop
SHELL := bash
CATALOG_SOURCE ?= data/UserBehavior.csv

checks:
	PYTHONPATH=clients:services:tools:libs python -m unittest discover -s tests/python -v
	ruff check clients services tools libs tests/python scripts flink-python-pipeline
	ruff format --check clients services tools libs tests/python scripts flink-python-pipeline
	python -m compileall -q clients services tools libs scripts flink-python-pipeline
	mvn -B test
	mvn -B package -DskipTests
	docker compose -f infra/docker-compose.yml --profile core config --quiet
	docker compose -f infra/docker-compose.yml --profile core --profile catalog --profile api config --quiet

start:
	bash scripts/start.sh

replay:
	bash scripts/replay.sh

verify:
	PYTHONPATH=clients:services:tools:libs python scripts/verify.py

snapshot:
	PYTHONPATH=clients:services:tools:libs python scripts/verify.py --snapshot artifacts/uninterrupted.json --snapshot-only

recovery-test:
	bash scripts/recovery_test.sh

catalog-generate:
	PYTHONPATH=clients:services:tools:libs python -m taobao_catalog "$(CATALOG_SOURCE)"

catalog-load:
	bash scripts/load_product_catalog.sh

stop:
	bash scripts/stop.sh
