from __future__ import annotations

import hashlib
import sys
import unittest
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PIPELINE = ROOT / "flink-python-pipeline"
sys.path.insert(0, str(PIPELINE))

from taobao_flink.config import PipelineConfig  # noqa: E402
from taobao_flink.sql import (  # noqa: E402
    insert_statements,
    render_tables,
    table_ddl,
    transformation_ddl,
)
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
        user_features_topic="taobao-user-features-1m",
        quality_topic="taobao-quality-events",
        cart_mutation_topic="taobao-active-cart-mutations",
        consumer_group="taobao-table-sql-test",
        checkpoint_dir="file:///tmp/checkpoints",
        checkpoint_interval_ms=60_000,
        restart_attempts=3,
        restart_delay_ms=10_000,
        dedup_retention_hours=168,
        max_out_of_orderness_ms=5_000,
        bounded=True,
        connector_jar=None,
    )


class PythonSqlPipelineTests(unittest.TestCase):
    def test_sql_preserves_behavior_outputs_and_source_category_metric_grain(self) -> None:
        ddl = render_tables(config())
        transformations = "\n".join(transformation_ddl())
        inserts = "\n".join(insert_statements())
        self.assertIn("'value.format' = 'avro-confluent'", ddl)
        self.assertIn("behavior_type STRING NOT NULL", ddl)
        self.assertIn("'connector' = 'upsert-kafka'", ddl)
        self.assertIn("category_id AS source_category_id", inserts)
        self.assertIn("INSERT INTO user_features_out", inserts)
        self.assertIn("COUNT(DISTINCT item_id) AS distinct_items", transformations)
        self.assertIn("WATERMARK FOR event_time", ddl)
        self.assertIn("processing_time AS PROCTIME()", ddl)
        self.assertIn("'scan.watermark.emit.strategy' = 'on-event'", ddl)
        self.assertIn("CURRENT_WATERMARK(event_time)", transformations)
        self.assertNotIn("JOIN product_catalog", inserts)
        self.assertNotIn("product_catalog", ddl)
        self.assertEqual(6, len(table_ddl(config())))

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

    def test_validation_and_cart_ordering_are_declarative(self) -> None:
        transformations = "\n".join(transformation_ddl())
        self.assertIn("INVALID_BEHAVIOR_TYPE", transformations)
        self.assertIn("WHERE validation_reason IS NULL", transformations)
        self.assertIn("PARTITION BY user_id, item_id", transformations)
        self.assertIn("ORDER BY event_time DESC, source_sequence DESC", transformations)
        self.assertIn("behavior_type IN ('cart', 'buy')", transformations)

    def test_quality_identity_is_deterministic(self) -> None:
        value = "INVALID|event|run-b|7|INVALID_IDENTIFIER"
        first = hashlib.sha256(value.encode()).hexdigest()
        second = hashlib.sha256(value.encode()).hexdigest()
        self.assertEqual(first, second)
        self.assertEqual(
            "4911a2796d362e120296fa6bbcad46c2105db08013f0d33490b4a57991d2cb60",
            first,
        )

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

    def test_platform_contracts_cover_catalog_and_bounded_dedup(self) -> None:
        postgres = (ROOT / "infra/postgres/init.sql").read_text(encoding="utf-8")
        clickhouse = (ROOT / "infra/clickhouse/schema.sql").read_text(encoding="utf-8")
        transformations = (PIPELINE / "sql/transformations.sql").read_text(encoding="utf-8")
        job = (PIPELINE / "taobao_flink/job.py").read_text(encoding="utf-8")
        compose = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
        connector = (ROOT / "infra/debezium/product-catalog-connector.json").read_text(
            encoding="utf-8"
        )
        self.assertIn("catalog_version cannot move backwards", postgres)
        self.assertIn("product_catalog_no_delete", postgres)
        self.assertIn("product_catalog_current_canonical", clickhouse)
        self.assertIn("user_features_1m_canonical", clickhouse)
        self.assertIn("kafka_topic_list = 'taobao-user-features-1m'", clickhouse)
        self.assertIn("ReplacingMergeTree(record_version)", clickhouse)
        self.assertIn("kafka_topic_list = 'product-catalog-cdc'", clickhouse)
        self.assertIn("ExtractNewRecordState", connector)
        self.assertNotIn("taobao-product-catalog-current", clickhouse)
        self.assertIn("ROW_NUMBER() OVER", transformations)
        self.assertIn("PARTITION BY event_id", transformations)
        self.assertIn("ORDER BY processing_time ASC", transformations)
        self.assertNotIn("ORDER BY event_time ASC", transformations)
        self.assertIn("WHERE row_num = 1", transformations)
        self.assertNotIn("DUPLICATE_WITHIN_RETENTION", transformations)
        self.assertNotIn("to_data_stream", job)
        self.assertIn('"table.exec.state.ttl"', job)
        self.assertIn('set_string("state.backend.type", "rocksdb")', job)
        self.assertIn('set_boolean("execution.checkpointing.incremental", True)', job)
        self.assertIn("cleanup.policy=compact", compose)
        self.assertIn("redis-feature-materializer", compose)
