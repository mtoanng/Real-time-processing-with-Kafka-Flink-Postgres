SHELL := bash

.PHONY: checks start replay verify recovery-test stop

checks:
	PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v
	ruff check producer scripts flink-sql-pipeline
	ruff format --check producer scripts flink-sql-pipeline
	bash -n scripts/*.sh
	mvn -B test
	mvn -B package -DskipTests
	RUNTIME_PROFILE=core python flink-sql-pipeline/render.py
	docker compose -f infra/docker-compose.yml --profile core config --quiet

start:
	bash scripts/start.sh

replay:
	bash scripts/replay.sh

verify:
	PYTHONPATH=producer/src python scripts/verify.py

recovery-test:
	bash scripts/recovery_test.sh

stop:
	bash scripts/stop.sh
