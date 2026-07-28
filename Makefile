SHELL := bash

.PHONY: checks start replay verify recovery-test stop

checks:
	PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v
	ruff check producer scripts
	ruff format --check producer scripts
	mvn -B test
	mvn -B -pl flink-jobs/taobao-stream-job -am package -DskipTests
	docker compose -f infra/docker-compose.yml config --quiet

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
