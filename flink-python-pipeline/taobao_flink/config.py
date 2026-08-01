"""Runtime configuration for the single SQL/PyFlink streaming job.

The configuration names the Kafka contracts and recovery policy; it does not
contain business rules.  Core startup needs Kafka, Schema Registry, Flink,
ClickHouse and Redis only. Catalog CDC bypasses this job.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _positive_int(name: str, default: int) -> int:
    """Read a required-positive integer setting with a safe demo default."""
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable deployment contract for sources, materialization topics and state.

    ``replay_run_id`` is deliberately absent: it is event lineage supplied by
    the source, not a runtime setting or canonical business key.
    """

    kafka_bootstrap_servers: str
    kafka_security_protocol: str
    kafka_sasl_mechanism: str
    kafka_sasl_username: str
    kafka_sasl_password: str
    schema_registry_url: str
    schema_registry_basic_auth: str
    behavior_topic: str
    raw_topic: str
    metrics_topic: str
    quality_topic: str
    cart_mutation_topic: str
    consumer_group: str
    checkpoint_dir: str
    checkpoint_interval_ms: int
    restart_attempts: int
    restart_delay_ms: int
    dedup_retention_hours: int
    cart_state_ttl_hours: int
    max_out_of_orderness_ms: int
    bounded: bool
    connector_jar: Path | None

    @classmethod
    def from_environment(cls) -> PipelineConfig:
        """Validate environment variables before the job contacts external services."""
        bounded = os.getenv("KAFKA_SOURCE_BOUNDED", "true").lower() == "true"
        connector = os.getenv("FLINK_CONNECTOR_JAR", "").strip()
        checkpoint_dir = os.getenv("FLINK_CHECKPOINT_DIR", "").strip()
        if not checkpoint_dir:
            raise ValueError("FLINK_CHECKPOINT_DIR is required for checkpoint-consistent recovery")
        security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
        if security_protocol not in {"PLAINTEXT", "SASL_SSL"}:
            raise ValueError("KAFKA_SECURITY_PROTOCOL must be PLAINTEXT or SASL_SSL")
        sasl_values = [
            os.getenv("KAFKA_SASL_MECHANISM", ""),
            os.getenv("KAFKA_SASL_USERNAME", ""),
            os.getenv("KAFKA_SASL_PASSWORD", ""),
        ]
        if security_protocol == "SASL_SSL" and not all(sasl_values):
            raise ValueError("SASL_SSL requires mechanism, username and password")
        return cls(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            kafka_security_protocol=security_protocol,
            kafka_sasl_mechanism=sasl_values[0],
            kafka_sasl_username=sasl_values[1],
            kafka_sasl_password=sasl_values[2],
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
            schema_registry_basic_auth=os.getenv("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", ""),
            behavior_topic=os.getenv("KAFKA_TOPIC", "user-behavior-events"),
            raw_topic=os.getenv("KAFKA_RAW_OUTPUT_TOPIC", "taobao-raw-events"),
            metrics_topic=os.getenv("KAFKA_METRICS_OUTPUT_TOPIC", "taobao-item-metrics-1m"),
            quality_topic=os.getenv("KAFKA_QUALITY_OUTPUT_TOPIC", "taobao-quality-events"),
            cart_mutation_topic=os.getenv(
                "KAFKA_CART_MUTATION_TOPIC", "taobao-active-cart-mutations"
            ),
            consumer_group=os.getenv("KAFKA_CONSUMER_GROUP", "taobao-python-sql-pipeline"),
            checkpoint_dir=checkpoint_dir,
            checkpoint_interval_ms=_positive_int("FLINK_CHECKPOINT_INTERVAL_MS", 60_000),
            restart_attempts=_positive_int("FLINK_RESTART_ATTEMPTS", 3),
            restart_delay_ms=_positive_int("FLINK_RESTART_DELAY_MS", 10_000),
            dedup_retention_hours=_positive_int("FLINK_DEDUP_RETENTION_HOURS", 168),
            cart_state_ttl_hours=_positive_int("FLINK_CART_STATE_TTL_HOURS", 168),
            max_out_of_orderness_ms=_positive_int("FLINK_MAX_OUT_OF_ORDERNESS_MS", 5_000),
            bounded=bounded,
            connector_jar=Path(connector) if connector else None,
        )
