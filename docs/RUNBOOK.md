# Runbook

Run the complete composition only on a disposable host with Docker, Java 11+,
Maven, Python 3.11+, `curl`, and enough memory for Kafka, Flink, ClickHouse,
Schema Registry, and Redis.

## Checks

Prediction: 22 Python tests and 33 Java tests pass; the shaded JAR packages and
the single Compose model renders.

```bash
make checks
```

## Start, replay, and verify

```bash
cp .env.example .env
make start
make replay
make verify
```

`make start` packages the job, starts the six runtime services, and registers
the Avro schema. `make replay` publishes `golden-a,golden-b` before submitting
the bounded Kafka source so end-of-input advances the final watermark.

`make verify` requires exact equality with the committed golden contract for:

- 11 canonical raw business rows;
- five metric keys and values;
- two invalid, eleven duplicate, and one late quality identities;
- user 1 cart `{101: "11|1511658004000|1511658005000"}`;
- user 2 empty cart;
- a positive bounded TTL on every existing cart key.

## Recovery experiment

This is a controlled failure and must run on a disposable host. First capture
an uninterrupted snapshot, then repeat the same bounded input with
`KAFKA_SOURCE_BOUNDED=false`, a short checkpoint interval, and a running job.
After one checkpoint completes:

```bash
RECOVERY_TEST_CONFIRM=YES \
FLINK_JOB_ID=<job-id> \
BASELINE_SNAPSHOT=artifacts/uninterrupted.json \
make recovery-test
```

The script restarts the TaskManager, waits for the job to return to `RUNNING`,
captures canonical raw/metric/quality/cart state, and requires exact equality
with the uninterrupted snapshot. Until that command succeeds with real
services, recovery is `NOT VERIFIED`.

## Controlled failure experiment

Set `FLINK_MAX_OUT_OF_ORDERNESS_MS=0`, rerun from clean disposable volumes, and
predict which fixture rows become late before executing. The canonical raw
count must remain unchanged while metric and late-quality results change.
Restore the default `5000` afterward.

## Stop

```bash
make stop
```

This stops containers but preserves named volumes. Remove volumes only as a
separate, explicitly approved disposable-host cleanup.
