# Recovery Verification Runbook

> Archived pre-core-refactor runbook. See `docs/OPERATIONS.md`.

Status: **requires live deployment verification**.

## Expected behavior

Kafka offsets and Flink keyed/broadcast state are restored from an at-least-once
checkpoint. Records between the completed checkpoint and failure may be
processed again. Cassandra mutations are idempotent; ClickHouse can contain
physical duplicates, while committed deduplicated views must retain the same
business results.

## Controlled experiment

1. Run the deterministic fixture with a unique replay run ID.
2. Wait for and record one completed checkpoint.
3. Record physical and deduplicated ClickHouse counts plus user 100's cart.
4. Set `RECOVERY_TEST_CONFIRM=YES`, identify the job, and configure a safe
   TaskManager restart command on the disposable host.
5. Run `scripts/run_flink_recovery_test.sh`.
6. Wait for recovery and repeat `scripts/verify_bounded_pipeline.sh` using the
   same run ID.
7. Compare physical rows, deduplicated rows, metrics, invalid/late audits, and
   Cassandra state.

## Pass criteria

- Flink reports recovery from a completed checkpoint.
- Deduplicated raw, audit, and metric results match independent expectations.
- User 100 has item 501 and does not have purchased item 500.
- Any increase in physical ClickHouse rows is documented as at-least-once
  transport duplication.
- No exactly-once or production-readiness claim is made.

Store sanitized evidence under `docs/evidence/final-e2e/`. Never store tokens,
Secure Connect Bundles, private keys, or full raw data.
