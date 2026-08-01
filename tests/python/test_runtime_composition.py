from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
JOB = (ROOT / "flink-python-pipeline/taobao_flink/job.py").read_text(encoding="utf-8")
INSERTS = (ROOT / "flink-python-pipeline/sql/inserts.sql").read_text(encoding="utf-8")
START = (ROOT / "scripts/start.sh").read_text(encoding="utf-8")


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
                "redis-feature-materializer",
                "flink-jobmanager",
                "flink-taskmanager",
                "flink-checkpoint-init",
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
        self.assertEqual(1, JOB.count(".execute()"))
        self.assertNotIn("to_data_stream", JOB)
        self.assertNotIn("StreamExecutionEnvironment", JOB)
        self.assertNotIn("taobao_flink.operators", JOB)
        self.assertIn('"table.exec.uid.generation", "ALWAYS"', JOB)
        self.assertIn("taskmanager.memory.process.size:", COMPOSE)
        self.assertNotIn("get_gateway", JOB)
        self.assertIn("table_env.create_statement_set()", JOB)
        self.assertIn("image: ${FLINK_RUNTIME_IMAGE:-taobao-flink-runtime:local}", COMPOSE)
        self.assertNotIn("/opt/flink/lib/taobao-flink-connectors.jar:ro", COMPOSE)
        self.assertIn("INSERT INTO raw_events_out", INSERTS)
        self.assertIn("INSERT INTO metrics_out", INSERTS)
        self.assertIn("INSERT INTO user_features_out", INSERTS)
        self.assertIn("INSERT INTO quality_events_out", INSERTS)
        self.assertNotIn("product_catalog", INSERTS)
        self.assertIn("INSERT INTO cart_mutations_out", INSERTS)
        self.assertIn("clickhouse-client", START)
        self.assertIn("infra/clickhouse/schema.sql", START)

    def test_active_flink_authoring_surface_is_table_api_only(self) -> None:
        package = ROOT / "flink-python-pipeline/taobao_flink"
        active_python = "\n".join(
            path.read_text(encoding="utf-8") for path in sorted(package.glob("*.py"))
        )
        self.assertNotIn("pyflink.datastream", active_python)
        self.assertNotIn("ProcessFunction", active_python)
        self.assertFalse((package / "operators.py").exists())
        self.assertFalse((package / "logic.py").exists())

    def test_repository_contains_no_authored_java_pipeline(self) -> None:
        self.assertFalse((ROOT / "flink-jobs").exists())
        authored_java = list((ROOT / "flink-connectors").rglob("*.java"))
        self.assertEqual([], authored_java)

    def test_replay_client_is_outside_the_pipeline_service_boundary(self) -> None:
        self.assertTrue((ROOT / "clients/taobao_replay").is_dir())
        self.assertTrue((ROOT / "libs/taobao_events").is_dir())
        service_source = "\n".join(
            path.read_text(encoding="utf-8") for path in sorted((ROOT / "services").rglob("*.py"))
        )
        self.assertNotIn("taobao_replay", service_source)


if __name__ == "__main__":
    unittest.main()
