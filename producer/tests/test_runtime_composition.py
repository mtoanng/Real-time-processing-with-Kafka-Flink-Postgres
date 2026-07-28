from __future__ import annotations

import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = (ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
JOB = (
    ROOT / "flink-jobs/taobao-stream-job/src/main/java/com/taobao/behavior/TaobaoStreamJob.java"
).read_text(encoding="utf-8")


class RuntimeCompositionTests(unittest.TestCase):
    def test_single_runtime_contains_only_the_six_core_services(self) -> None:
        services_block = COMPOSE.split("services:\n", 1)[1].split("\nvolumes:", 1)[0]
        services = set(re.findall(r"^  ([a-z][a-z0-9-]+):$", services_block, re.MULTILINE))
        self.assertEqual(
            {
                "kafka",
                "schema-registry",
                "clickhouse",
                "redis",
                "flink-jobmanager",
                "flink-taskmanager",
            },
            services,
        )
        self.assertNotIn("profiles:", COMPOSE)

    def test_one_job_writes_all_declared_materialized_outputs(self) -> None:
        self.assertEqual(1, JOB.count("KafkaSource.<UserBehaviorEvent>builder()"))
        self.assertIn('"raw_behavior_events"', JOB)
        self.assertIn('"item_metrics_1m"', JOB)
        self.assertIn('"stream_quality_events"', JOB)
        self.assertIn("new RedisActiveCartSink", JOB)


if __name__ == "__main__":
    unittest.main()
