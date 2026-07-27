SHELL := bash

.PHONY: checks test package infra-config remote-up remote-down schema publish-fixture run-job checkpoint-experiment terraform-validate teardown

checks: test package infra-config terraform-validate

test:
	PYTHONPATH=producer/src python -m unittest discover -s producer/tests -v
	ruff check producer scripts
	ruff format --check producer scripts
	mvn -B test

package:
	mvn -B -pl flink-jobs/taobao-stream-job -am package

infra-config:
	docker compose -f infra/docker-compose.yml --profile core config --quiet
	docker compose -f infra/docker-compose.yml --profile serving config --quiet
	docker compose -f infra/docker-compose.yml --profile cdc config --quiet
	docker compose -f infra/docker-compose.yml --profile observability config --quiet

remote-up:
	bash scripts/run.sh

remote-down:
	bash scripts/stop.sh

schema:
	bash scripts/register_schemas.sh

publish-fixture:
	bash scripts/replay.sh

run-job:
	bash scripts/run_flink.sh

lookup-user:
	bash scripts/lookup_active_cart.sh $(USER_ID)

checkpoint-experiment:
	bash scripts/run_checkpoint_experiment.sh

terraform-validate:
	terraform -chdir=infra/terraform fmt -check
	terraform -chdir=infra/terraform init -backend=false -input=false
	terraform -chdir=infra/terraform validate

teardown:
	bash scripts/teardown_demo.sh
