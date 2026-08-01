from __future__ import annotations

import importlib.util
import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "flink-python-pipeline"
sys.path.insert(0, str(PIPELINE))

from taobao_flink.config import PipelineConfig  # noqa: E402
from taobao_flink.logic import (  # noqa: E402
    apply_cart_event,
    quality_event_id,
    validation_reason,
)
from taobao_flink.sql import insert_statements, render_tables, table_ddl  # noqa: E402
from taobao_serving.redis_cart import apply_mutation  # noqa: E402


class FakeRedis:
    def __init__(self) -> None:
        self.actions = []

    def hset(self, *args):
        self.actions.append(("hset", args))

    def expire(self, *args):
        self.actions.append(("expire", args))

    def hdel(self, *args):
        self.actions.append(("hdel", args))


def config() -> PipelineConfig:
    return PipelineConfig(
        kafka_bootstrap_servers="kafka:29092",
        kafka_security_protocol="PLAINTEXT",
        kafka_sasl_mechanism="",
        kafka_sasl_username="",
        kafka_sasl_password="",
        schema_registry_url="http://schema-registry:8081",
        schema_registry_basic_auth="",
        behavior_topic="user-behavior-events",
        raw_topic="taobao-raw-events",
        metrics_topic="taobao-item-metrics-1m",
        quality_topic="taobao-quality-events",
        cart_mutation_topic="taobao-active-cart-mutations",
        consumer_group="test",
        checkpoint_dir="file:///tmp/checkpoints",
        checkpoint_interval_ms=60_000,
        restart_attempts=3,
        restart_delay_ms=10_000,
        dedup_retention_hours=168,
        cart_state_ttl_hours=168,
        max_out_of_orderness_ms=5_000,
        bounded=True,
        connector_jar=None,
    )


class PythonSqlPipelineTests(unittest.TestCase):
    def test_sql_preserves_behavior_outputs_and_source_category_metric_grain(self) -> None:
        ddl = render_tables(config())
        inserts = "\n".join(insert_statements())
        self.assertIn("'value.format' = 'avro-confluent'", ddl)
        self.assertIn("behavior_type STRING NOT NULL", ddl)
        self.assertIn("'connector' = 'upsert-kafka'", ddl)
        self.assertIn("category_id AS source_category_id", inserts)
        self.assertNotIn("JOIN product_catalog", inserts)
        self.assertNotIn("product_catalog", ddl)
        self.assertEqual(5, len(table_ddl(config())))

    def test_managed_kafka_security_is_rendered_as_connector_configuration(self) -> None:
        managed = replace(
            config(),
            kafka_security_protocol="SASL_SSL",
            kafka_sasl_mechanism="PLAIN",
            kafka_sasl_username="key",
            kafka_sasl_password="secret",
            schema_registry_basic_auth="schema:secret",
        )
        ddl = render_tables(managed)
        self.assertIn("'properties.security.protocol' = 'SASL_SSL'", ddl)
        self.assertIn("'properties.sasl.mechanism' = 'PLAIN'", ddl)
        self.assertIn(
            "'value.avro-confluent.basic-auth.credentials-source' = 'USER_INFO'",
            ddl,
        )

    def test_validation_rules_remain_source_faithful(self) -> None:
        self.assertIsNone(validation_reason("a", 1, 2, 3, "pv", 1))
        self.assertEqual("INVALID_BEHAVIOR_TYPE", validation_reason("a", 1, 2, 3, "view", 1))

    def test_cart_lifecycle_and_stale_event_protection(self) -> None:
        state, cart = apply_cart_event(
            None,
            user_id=1,
            item_id=2,
            category_id=3,
            behavior_type="cart",
            event_time_ms=200,
            source_sequence=2,
        )
        self.assertEqual("UPSERT", cart.operation)
        stale_state, stale = apply_cart_event(
            state,
            user_id=1,
            item_id=2,
            category_id=3,
            behavior_type="buy",
            event_time_ms=100,
            source_sequence=1,
        )
        self.assertEqual(state, stale_state)
        self.assertIsNone(stale)
        bought, delete = apply_cart_event(
            state,
            user_id=1,
            item_id=2,
            category_id=3,
            behavior_type="buy",
            event_time_ms=300,
            source_sequence=3,
        )
        self.assertFalse(bought.active)
        self.assertEqual("DELETE", delete.operation)
        unchanged, none = apply_cart_event(
            bought,
            user_id=1,
            item_id=2,
            category_id=3,
            behavior_type="fav",
            event_time_ms=400,
            source_sequence=4,
        )
        self.assertEqual(bought, unchanged)
        self.assertIsNone(none)

    def test_quality_identity_is_deterministic(self) -> None:
        first = quality_event_id("DUPLICATE", "event", "run-b", 7, "DUPLICATE_WITHIN_RETENTION")
        second = quality_event_id("DUPLICATE", "event", "run-b", 7, "DUPLICATE_WITHIN_RETENTION")
        self.assertEqual(first, second)

    def test_redis_adapter_applies_idempotent_mutations(self) -> None:
        redis = FakeRedis()
        apply_mutation(
            redis,
            {
                "operation": "UPSERT",
                "user_id": 1,
                "item_id": 2,
                "category_id": 3,
                "added_at_ms": 10,
                "last_updated_at_ms": 20,
            },
            60,
        )
        apply_mutation(
            redis,
            {
                "operation": "DELETE",
                "user_id": 1,
                "item_id": 2,
                "category_id": 3,
                "added_at_ms": 0,
                "last_updated_at_ms": 30,
            },
            60,
        )
        self.assertEqual("hset", redis.actions[0][0])
        self.assertEqual("hdel", redis.actions[-1][0])

    def test_pyflink_runtime_is_not_required_for_contract_tests(self) -> None:
        self.assertIsNotNone(importlib.util.find_spec("taobao_flink.logic"))

    def test_platform_contracts_cover_catalog_and_bounded_dedup(self) -> None:
        postgres = (ROOT / "infra/postgres/init.sql").read_text(encoding="utf-8")
        clickhouse = (ROOT / "infra/clickhouse/schema.sql").read_text(encoding="utf-8")
        operators = (PIPELINE / "taobao_flink/operators.py").read_text(encoding="utf-8")
        compose = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
        connector = (ROOT / "infra/debezium/product-catalog-connector.json").read_text(
            encoding="utf-8"
        )
        self.assertIn("catalog_version cannot move backwards", postgres)
        self.assertIn("product_catalog_no_delete", postgres)
        self.assertIn("product_catalog_current_canonical", clickhouse)
        self.assertIn("kafka_topic_list = 'product-catalog-cdc'", clickhouse)
        self.assertIn("ExtractNewRecordState", connector)
        self.assertNotIn("taobao-product-catalog-current", clickhouse)
        self.assertIn("StateTtlConfig.new_builder", operators)
        self.assertIn("DUPLICATE_WITHIN_RETENTION", operators)
        self.assertIn("cleanup.policy=compact", compose)
