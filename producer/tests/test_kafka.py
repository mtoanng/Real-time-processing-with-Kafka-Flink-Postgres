from __future__ import annotations

import unittest
from pathlib import Path

from taobao_replay.contracts import parse_event
from taobao_replay.kafka import (
    KafkaEventPublisher,
    event_to_avro,
    kafka_key,
    kafka_security_options,
)


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.polls: list[float] = []
        self.remaining = 0

    def produce(self, **kwargs: object) -> None:
        self.messages.append(kwargs)

    def poll(self, timeout: float) -> None:
        self.polls.append(timeout)

    def flush(self, timeout: float | None = None) -> int:
        return self.remaining


class KafkaPublisherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.event = parse_event(
            ("42", "500", "50", "buy", "1511658020"),
            source_sequence=7,
            replay_run_id="test-run",
        )

    def test_event_maps_to_avro_contract_and_user_id_key(self) -> None:
        payload = event_to_avro(self.event)

        self.assertEqual(payload["event_id"], self.event.event_id)
        self.assertEqual(payload["behavior_type"], "buy")
        self.assertEqual(payload["event_time_ms"], 1_511_658_020_000)
        self.assertEqual(kafka_key(self.event), "42")

    def test_publisher_preserves_topic_key_and_event(self) -> None:
        producer = FakeProducer()
        publisher = KafkaEventPublisher(producer, topic="user-behavior-events")

        publisher.publish(self.event)
        publisher.close()

        self.assertEqual(len(producer.messages), 1)
        self.assertEqual(producer.messages[0]["topic"], "user-behavior-events")
        self.assertEqual(producer.messages[0]["key"], "42")
        self.assertIs(producer.messages[0]["value"], self.event)
        self.assertEqual(producer.polls, [0])

    def test_flush_timeout_is_explicit(self) -> None:
        producer = FakeProducer()
        producer.remaining = 1
        publisher = KafkaEventPublisher(producer)

        with self.assertRaisesRegex(RuntimeError, "1 message"):
            publisher.close(timeout=0.1)

    def test_plaintext_security_mode_needs_no_sasl_values(self) -> None:
        self.assertEqual(kafka_security_options({}), {"security.protocol": "PLAINTEXT"})

    def test_managed_sasl_ssl_security_mode_maps_credentials(self) -> None:
        options = kafka_security_options(
            {
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "key",
                "KAFKA_SASL_PASSWORD": "secret",
            }
        )
        self.assertEqual(
            options,
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "key",
                "sasl.password": "secret",
            },
        )

    def test_partial_sasl_configuration_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "KAFKA_SASL_MECHANISM"):
            kafka_security_options({"KAFKA_SECURITY_PROTOCOL": "SASL_SSL"})
        with self.assertRaisesRegex(ValueError, "provided together"):
            kafka_security_options(
                {
                    "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                    "KAFKA_SASL_MECHANISM": "PLAIN",
                    "KAFKA_SASL_USERNAME": "key",
                }
            )

    def test_confluent_avro_wire_round_trip_uses_registered_schema_id(self) -> None:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
        from confluent_kafka.serialization import MessageField, SerializationContext

        repository_root = Path(__file__).resolve().parents[2]
        schema_text = (repository_root / "schemas/user-behavior-event.avsc").read_text(
            encoding="utf-8"
        )
        registry = SchemaRegistryClient.new_client({"url": "mock://phase2-tests"})
        context = SerializationContext("user-behavior-events", MessageField.VALUE)
        serializer = AvroSerializer(
            registry,
            schema_str=schema_text,
            to_dict=event_to_avro,
            conf={"auto.register.schemas": True},
        )

        encoded = serializer(self.event, context)
        decoded = AvroDeserializer(registry)(encoded, context)
        registered = registry.get_latest_version("user-behavior-events-value")

        self.assertEqual(encoded[0], 0)
        self.assertEqual(int.from_bytes(encoded[1:5], byteorder="big"), registered.schema_id)
        self.assertEqual(decoded, event_to_avro(self.event))


if __name__ == "__main__":
    unittest.main()
