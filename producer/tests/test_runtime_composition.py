from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
JOB = (ROOT / "flink-python-pipeline/taobao_flink/job.py").read_text(encoding="utf-8")
INSERTS = (ROOT / "flink-python-pipeline/sql/inserts.sql").read_text(encoding="utf-8")


class RuntimeCompositionTests(unittest.TestCase):
    def test_runtime_profiles_preserve_core_catalog_and_api_boundaries(self) -> None:
        services_block = COMPOSE.split("services:\n", 1)[1].split("\nvolumes:", 1)[0]
        services = set(re.findall(r"^  ([a-z][a-z0-9-]+):$", services_block, re.MULTILINE))
        self.assertEqual(
            {
                "kafka",
                "kafka-init",
                "schema-registry",
                "clickhouse",
                "redis",
                "redis-cart-materializer",
                "flink-jobmanager",
                "flink-taskmanager",
                "flink-submit",
                "postgres",
                "kafka-connect",
                "api",
            },
            services,
        )
        self.assertIn('profiles: ["core"]', COMPOSE)
        self.assertIn('profiles: ["catalog"]', COMPOSE)
        self.assertIn('profiles: ["api"]', COMPOSE)

    def test_one_job_writes_all_declared_materialized_outputs(self) -> None:
        self.assertEqual(1, JOB.count('.execute("Taobao Python-SQL Streaming Platform")'))
        self.assertEqual(1, JOB.count("table_env.to_data_stream("))
        self.assertIn("table_env.to_data_stream(classified)", JOB)
        self.assertIn('"table.exec.uid.generation", "ALWAYS"', JOB)
        self.assertIn("ImmediateWatermarkStrategy.create", JOB)
        self.assertIn("statement_set.attach_as_datastream()", JOB)
        self.assertIn(
            "/opt/flink/lib/taobao-sql-connectors.jar:ro",
            COMPOSE,
        )
        self.assertIn("INSERT INTO raw_events_out", INSERTS)
        self.assertIn("INSERT INTO metrics_out", INSERTS)
        self.assertIn("INSERT INTO quality_events_out", INSERTS)
        self.assertIn("INSERT INTO product_catalog_out", INSERTS)
        self.assertIn("INSERT INTO cart_mutations_out", INSERTS)


if __name__ == "__main__":
    unittest.main()
