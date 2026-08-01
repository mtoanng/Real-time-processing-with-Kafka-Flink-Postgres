"""Kafka/Schema Registry transport shared by approved behavior-event ingress clients."""

from __future__ import annotations

import json
import os
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol

from taobao_events.contracts import UserBehaviorEvent

DEFAULT_TOPIC = "user-behavior-events"


class ProducerLike(Protocol):
    """Small producer surface used to unit-test publishing without Kafka."""

    def produce(self, **kwargs: object) -> None: ...

    def poll(self, timeout: float) -> object: ...

    def flush(self, timeout: float | None = None) -> int: ...


def event_to_avro(event: UserBehaviorEvent, _context: object = None) -> dict[str, str | int]:
    """Adapt the portable event contract to the Avro serializer callback."""
    return event.to_dict()


def kafka_key(event: UserBehaviorEvent) -> str:
    """Partition behavior events by user without changing event identity."""
    return str(event.user_id)


def kafka_security_options(environment: Mapping[str, str]) -> dict[str, str]:
    """Render either local PLAINTEXT or managed SASL_SSL client settings."""
    protocol = environment.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    if protocol not in {"PLAINTEXT", "SASL_SSL"}:
        raise ValueError("KAFKA_SECURITY_PROTOCOL must be PLAINTEXT or SASL_SSL")

    options = {"security.protocol": protocol}
    sasl_values = {
        "KAFKA_SASL_MECHANISM": environment.get("KAFKA_SASL_MECHANISM", "").strip(),
        "KAFKA_SASL_USERNAME": environment.get("KAFKA_SASL_USERNAME", "").strip(),
        "KAFKA_SASL_PASSWORD": environment.get("KAFKA_SASL_PASSWORD", "").strip(),
        "KAFKA_SASL_JAAS_CONFIG": environment.get("KAFKA_SASL_JAAS_CONFIG", "").strip(),
    }
    if protocol == "PLAINTEXT":
        if any(sasl_values.values()):
            raise ValueError("SASL settings require KAFKA_SECURITY_PROTOCOL=SASL_SSL")
        return options

    if not sasl_values["KAFKA_SASL_MECHANISM"]:
        raise ValueError("KAFKA_SASL_MECHANISM is required for SASL_SSL")
    has_user_password = bool(sasl_values["KAFKA_SASL_USERNAME"]) and bool(
        sasl_values["KAFKA_SASL_PASSWORD"]
    )
    if bool(sasl_values["KAFKA_SASL_USERNAME"]) != bool(sasl_values["KAFKA_SASL_PASSWORD"]):
        raise ValueError("KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD must be provided together")
    if not has_user_password and not sasl_values["KAFKA_SASL_JAAS_CONFIG"]:
        raise ValueError(
            "SASL_SSL requires KAFKA_SASL_JAAS_CONFIG or both "
            "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD"
        )

    options["sasl.mechanism"] = sasl_values["KAFKA_SASL_MECHANISM"]
    if has_user_password:
        options["sasl.username"] = sasl_values["KAFKA_SASL_USERNAME"]
        options["sasl.password"] = sasl_values["KAFKA_SASL_PASSWORD"]
    if sasl_values["KAFKA_SASL_JAAS_CONFIG"]:
        options["sasl.jaas.config"] = sasl_values["KAFKA_SASL_JAAS_CONFIG"]
    return options


def _load_schema_registry_client() -> tuple[type[Any], type[Any], type[Any], type[Any]]:
    """Load optional Kafka dependencies only when a real publisher is requested."""
    try:
        from confluent_kafka import SerializingProducer
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroSerializer
        from confluent_kafka.serialization import StringSerializer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka publishing requires the optional dependency: pip install -e '.[kafka]'"
        ) from exc
    return SerializingProducer, SchemaRegistryClient, AvroSerializer, StringSerializer


def build_schema_registry_producer(
    *,
    bootstrap_servers: str,
    schema_registry_url: str,
    schema_path: Path,
    environment: Mapping[str, str] | None = None,
) -> ProducerLike:
    """Build an idempotent Avro producer that uses the committed schema contract."""
    if not bootstrap_servers.strip():
        raise ValueError("bootstrap_servers must not be blank")
    if not schema_registry_url.strip():
        raise ValueError("schema_registry_url must not be blank")

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema_text = json.dumps(schema, separators=(",", ":"))
    producer_type, registry_type, avro_serializer_type, string_serializer_type = (
        _load_schema_registry_client()
    )

    active_environment = os.environ if environment is None else environment
    registry_config: dict[str, str] = {"url": schema_registry_url}
    registry_auth = active_environment.get("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "").strip()
    if registry_auth:
        registry_config["basic.auth.user.info"] = registry_auth

    registry = registry_type(registry_config)
    value_serializer = avro_serializer_type(
        registry,
        schema_str=schema_text,
        to_dict=event_to_avro,
        conf={"auto.register.schemas": False},
    )
    producer_config: dict[str, object] = {
        "bootstrap.servers": bootstrap_servers,
        "key.serializer": string_serializer_type("utf_8"),
        "value.serializer": value_serializer,
        "enable.idempotence": True,
        "acks": "all",
    }
    producer_config.update(kafka_security_options(active_environment))
    return producer_type(producer_config)


class KafkaEventPublisher:
    """Publish behavior events and surface broker acknowledgement failures."""

    def __init__(self, producer: ProducerLike, *, topic: str = DEFAULT_TOPIC) -> None:
        if not topic.strip():
            raise ValueError("topic must not be blank")
        self._producer = producer
        self._topic = topic
        self._delivery_errors: list[str] = []
        self._lock = threading.Lock()

    def publish(self, event: UserBehaviorEvent) -> None:
        """Queue one event; callers must later flush or use ``publish_confirmed``."""

        def delivered(error: object, _message: object) -> None:
            if error is not None:
                self._delivery_errors.append(str(error))

        self._producer.produce(
            topic=self._topic,
            key=kafka_key(event),
            value=event,
            on_delivery=delivered,
        )
        self._producer.poll(0)

    def publish_confirmed(self, event: UserBehaviorEvent, timeout: float = 10.0) -> None:
        """Publish one HTTP-ingress event and wait for Kafka acknowledgement."""
        with self._lock:
            self.publish(event)
            self.close(timeout)

    def close(self, timeout: float = 30.0) -> None:
        """Flush pending records and turn delivery errors into a caller-visible failure."""
        remaining = self._producer.flush(timeout)
        if remaining:
            raise RuntimeError(f"Kafka flush timed out with {remaining} message(s) pending")
        if self._delivery_errors:
            raise RuntimeError(f"Kafka delivery failed: {self._delivery_errors[0]}")
