# Final E2E Evidence

This directory is reserved for bounded cloud E2E evidence. The preparation task
does not run a cloud deployment, so no success evidence is claimed here.

Use `scripts/collect_cloud_evidence.sh` after an approved run. Expected evidence
includes run metadata, Compose status, produced/valid/invalid/late reconciliation,
deduplicated ClickHouse raw and rollup checks, Redis active-cart lookup,
optional checkpoint/rule-update logs, Grafana configuration, and teardown
verification. S3 is not an event sink.

Do not store credentials, private keys, full-scale raw data, or service tokens.
Until a real bounded run is completed, cloud E2E remains `NOT VERIFIED`.
