.PHONY: checks start replay verify snapshot recovery-test catalog-generate catalog-load stop
SHELL := bash
CATALOG_SOURCE ?= data/UserBehavior.csv

checks:
	PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v
	ruff check producer scripts flink-python-pipeline
	ruff format --check producer scripts flink-python-pipeline
	python -m compileall -q producer/src scripts flink-python-pipeline
	mvn -B test
	mvn -B package -DskipTests
	docker compose -f infra/docker-compose.yml --profile core config --quiet
	docker compose -f infra/docker-compose.yml --profile core --profile catalog --profile api config --quiet

start:
	bash scripts/start.sh

replay:
	bash scripts/replay.sh

verify:
	PYTHONPATH=producer/src python scripts/verify.py

snapshot:
	PYTHONPATH=producer/src python scripts/verify.py --snapshot artifacts/uninterrupted.json --snapshot-only

recovery-test:
	bash scripts/recovery_test.sh

catalog-generate:
	PYTHONPATH=producer/src python -m taobao_catalog "$(CATALOG_SOURCE)"

catalog-load:
	bash scripts/load_product_catalog.sh

stop:
	bash scripts/stop.sh
