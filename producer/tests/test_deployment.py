from __future__ import annotations

import unittest
from pathlib import Path

from scripts.package_deployment import deployment_files
from scripts.preflight import configuration_errors

ROOT = Path(__file__).resolve().parents[2]


class DeploymentContractTests(unittest.TestCase):
    def test_cloud_preflight_accepts_only_external_secured_endpoints(self) -> None:
        environment = {
            "KAFKA_BOOTSTRAP_SERVERS": "pkc-example.us-east-1.aws.confluent.cloud:9092",
            "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
            "KAFKA_SASL_MECHANISM": "PLAIN",
            "KAFKA_SASL_USERNAME": "kafka-key",
            "KAFKA_SASL_PASSWORD": "kafka-secret",
            "SCHEMA_REGISTRY_URL": "https://psrc-example.us-east-2.aws.confluent.cloud",
            "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO": "schema-key:schema-secret",
            "KAFKA_CONNECT_REPLICATION_FACTOR": "3",
            "POSTGRES_PASSWORD": "non-default-postgres",
            "CLICKHOUSE_PASSWORD": "non-default-clickhouse",
        }

        self.assertEqual([], configuration_errors("cloud-demo", environment))

        environment["KAFKA_SECURITY_PROTOCOL"] = "PLAINTEXT"
        self.assertIn(
            "KAFKA_SECURITY_PROTOCOL must be SASL_SSL for cloud-demo",
            configuration_errors("cloud-demo", environment),
        )

    def test_deployment_archive_excludes_secrets_raw_data_and_build_outputs(self) -> None:
        files = {path.as_posix() for path in deployment_files(ROOT)}

        self.assertIn("infra/docker-compose.yml", files)
        self.assertIn("docs/AWS_DEPLOYMENT.md", files)
        self.assertNotIn(".env", files)
        self.assertNotIn("data/UserBehavior.csv", files)
        self.assertFalse(any("/target/" in path or "__pycache__" in path for path in files))


if __name__ == "__main__":
    unittest.main()
