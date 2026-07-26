import unittest
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


class RuntimeProfileFilesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.compose = (REPOSITORY_ROOT / "infra/docker-compose.yml").read_text(encoding="utf-8")
        self.environment = (REPOSITORY_ROOT / ".env.example").read_text(encoding="utf-8")

    def test_core_profile_is_kafka_registry_flink_and_clickhouse(self) -> None:
        for service in (
            "kafka",
            "schema-registry",
            "flink-jobmanager",
            "flink-taskmanager",
            "clickhouse",
        ):
            self.assertIn(f"  {service}:", self.compose)
        self.assertIn('profiles: ["core", "serving", "cdc", "observability"]', self.compose)
        cassandra_section = self.compose.split("  cassandra:", 1)[1].split("  postgres:", 1)[0]
        self.assertIn('profiles: ["serving"]', cassandra_section)
        self.assertNotIn('"core"', cassandra_section)

    def test_optional_profiles_isolate_their_services(self) -> None:
        self.assertIn('profiles: ["serving"]', self.compose)
        self.assertGreaterEqual(self.compose.count('profiles: ["cdc"]'), 2)
        self.assertIn('profiles: ["observability"]', self.compose)

    def test_environment_keeps_cassandra_optional_and_recovery_explicit(self) -> None:
        self.assertIn("RUNTIME_PROFILE=core", self.environment)
        self.assertIn("CASSANDRA_MODE=\n", self.environment)
        self.assertIn("FLINK_CHECKPOINTING_ENABLED=true", self.environment)
        self.assertIn("FLINK_CHECKPOINT_DIR=file:///var/lib/flink/checkpoints", self.environment)
        self.assertIn("FLINK_RESTART_ATTEMPTS=3", self.environment)
        self.assertIn("FLINK_DEDUP_RETENTION_HOURS=168", self.environment)

    def test_active_environment_has_no_s3_event_archive_variable(self) -> None:
        self.assertNotIn("S3_ARCHIVE_URI", self.environment)


if __name__ == "__main__":
    unittest.main()
