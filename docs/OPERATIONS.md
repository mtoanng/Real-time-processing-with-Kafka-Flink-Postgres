# Operations and Recovery

## Configuration

Core requires Kafka, Schema Registry, ClickHouse, and persistent checkpoint
settings. `serving` additionally requires Redis configuration. The
checked-in `cdc` configuration belongs to the deprecated behavior-rule
extension and is not a current release target.

Important recovery values:

```text
FLINK_CHECKPOINTING_ENABLED=true
FLINK_CHECKPOINT_INTERVAL_MS=60000
FLINK_CHECKPOINT_DIR=file:///persistent/path
FLINK_RESTART_ATTEMPTS=3
FLINK_RESTART_DELAY_MS=10000
FLINK_DEDUP_RETENTION_HOURS=168
```

The Compose Flink services mount `flink_checkpoints` at
`/var/lib/flink/checkpoints`, so TaskManager/JobManager process restarts retain
completed checkpoints unless volumes are removed.

## Experiment A: replay identity

Prerequisite: one running checkpointed job and a fresh bounded database.

```bash
RUN_A_ID=fixture-run-a RUN_B_ID=fixture-run-b \
  bash scripts/run_replay_identity_experiment.sh
```

The independent verifier expects 999 canonical raw events, 995 canonical metric
rows, two invalid observations, 999 second-attempt duplicates, and two late
events. It compares SHA-256 digests over stable business columns.

## Experiment B: failure recovery

First capture an uninterrupted result:

```bash
PYTHONPATH=producer/src python scripts/canonical_snapshot.py capture \
  --output docs/evidence/latest/recovery/uninterrupted.json
```

Against a separate fresh database/job:

```bash
export BASELINE_SNAPSHOT=docs/evidence/latest/recovery/uninterrupted.json
export FLINK_JOB_ID=<job-id>
export FLINK_REST_URL=http://localhost:8082
export FLINK_FAILURE_COMMAND='docker compose -f infra/docker-compose.yml restart flink-taskmanager'
RECOVERY_TEST_CONFIRM=YES bash scripts/run_flink_recovery_test.sh
```

The script waits for a completed checkpoint, restarts the TaskManager, waits for
the job to return to `RUNNING`, and compares stable canonical results. Under
`serving`, a normalized user-100 cart snapshot is included.

## Rollback

Stop the candidate job and resubmit the last known JAR only from a compatible
checkpoint. State compatibility across application upgrades is not demonstrated
in this project. If compatibility is unknown, use a fresh consumer group,
database, and bounded replay.

## Troubleshooting

- Missing checkpoint directory: create persistent storage visible to every
  Flink process; do not use an ephemeral `/tmp` path.
- Canonical counts differ: confirm queries use canonical views and compare
  stable fields, not ingestion timestamps or replay IDs.
- Duplicate count is zero in experiment A: keep the same running job/state
  across both replays and finish run B within the dedup retention horizon.
- Redis errors in core: verify `RUNTIME_PROFILE=core`; core must not open a
  Redis client.
- Legacy CDC errors in core: verify no rules topic or Connect startup is being
  invoked. Product CDC is not implemented.
