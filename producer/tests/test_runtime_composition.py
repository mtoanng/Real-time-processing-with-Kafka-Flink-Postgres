from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
ROOT_POM = (ROOT / "pom.xml").read_text(encoding="utf-8")
SQL = (ROOT / "flink-sql-pipeline/pipeline.sql.template").read_text(encoding="utf-8")
CLICKHOUSE = (ROOT / "infra/clickhouse/schema.sql").read_text(encoding="utf-8")


class RuntimeCompositionTests(unittest.TestCase):
    def test_core_profile_contains_only_streaming_analytics_services(self) -> None:
        core_services = {
            service
            for service, body in re.findall(
                r"^  ([a-z][a-z0-9-]+):\n(.*?)(?=^  [a-z][a-z0-9-]+:\n|^volumes:)",
                COMPOSE,
                re.MULTILINE | re.DOTALL,
            )
            if 'profiles: ["core"]' in body
        }
        self.assertEqual(
            {
                "kafka",
                "schema-registry",
                "clickhouse",
                "flink-jobmanager",
                "flink-taskmanager",
            },
            core_services,
        )
        for service in ("postgres", "kafka-connect", "redis", "api"):
            block = COMPOSE.split(f"  {service}:\n", 1)[1].split("\n  ", 1)[0]
            self.assertIn('profiles: ["legacy"]', block)

    def test_active_build_and_runtime_submit_sql_not_java_business_code(self) -> None:
        self.assertIn("<module>flink-sql-pipeline</module>", ROOT_POM)
        self.assertNotIn("<module>flink-jobs/taobao-stream-job</module>", ROOT_POM)
        self.assertIn("/opt/flink/sql/pipeline.sql", COMPOSE)
        self.assertIn("taobao-sql-connectors.jar", COMPOSE)
        self.assertFalse(list((ROOT / "flink-sql-pipeline").rglob("*.java")))

    def test_sql_contains_core_business_contract(self) -> None:
        for contract in (
            "value.format' = 'avro-confluent",
            "WATERMARK FOR event_time",
            "COUNT(*) OVER",
            "table.exec.state.ttl",
            "TUMBLE(TABLE accepted_unique_events",
            "category_id AS source_category_id",
            "'INVALID' AS quality_type",
            "'DUPLICATE' AS quality_type",
            "'LATE' AS quality_type",
            "sink.delivery-guarantee' = 'at-least-once",
        ):
            self.assertIn(contract, SQL)
        group_by = SQL.split("GROUP BY ", 1)[1].split(";", 1)[0]
        self.assertNotIn("replay_run_id", group_by)

    def test_clickhouse_boundary_uses_replay_independent_replacement_keys(self) -> None:
        self.assertEqual(3, CLICKHOUSE.count("ENGINE = Kafka"))
        self.assertIn("ORDER BY (toDate(event_time), event_id)", CLICKHOUSE)
        self.assertIn("ORDER BY (window_start, item_id, source_category_id)", CLICKHOUSE)
        self.assertIn("FROM taobao_behavior.raw_behavior_events FINAL", CLICKHOUSE)
        self.assertIn("FROM taobao_behavior.item_metrics_1m FINAL", CLICKHOUSE)
        self.assertNotIn("ORDER BY (replay_run_id", CLICKHOUSE)


if __name__ == "__main__":
    unittest.main()
