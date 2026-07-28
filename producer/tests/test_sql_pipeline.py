from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RENDERER = ROOT / "flink-sql-pipeline/render.py"
SPEC = importlib.util.spec_from_file_location("flink_sql_render", RENDERER)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class SqlPipelineTests(unittest.TestCase):
    def test_default_render_is_complete_and_uses_internal_core_endpoints(self) -> None:
        sql = MODULE.render({})

        self.assertNotIn("{{", sql)
        self.assertIn("'properties.bootstrap.servers' = 'kafka:29092'", sql)
        self.assertIn(
            "'value.avro-confluent.url' = 'http://schema-registry:8080/apis/ccompat/v7'", sql
        )
        self.assertIn("SET 'table.exec.state.ttl' = '168 h'", sql)
        self.assertIn("INTERVAL '5' SECOND", sql)

    def test_managed_security_is_rendered_as_connector_configuration(self) -> None:
        sql = MODULE.render(
            {
                "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
                "KAFKA_SASL_MECHANISM": "PLAIN",
                "KAFKA_SASL_USERNAME": "key",
                "KAFKA_SASL_PASSWORD": "secret",
                "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO": "registry:secret",
            }
        )

        self.assertIn("'properties.security.protocol' = 'SASL_SSL'", sql)
        self.assertIn("'properties.sasl.mechanism' = 'PLAIN'", sql)
        self.assertIn("'value.avro-confluent.basic-auth.credentials-source' = 'USER_INFO'", sql)

    def test_invalid_state_and_watermark_configuration_fails_fast(self) -> None:
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            MODULE.render({"FLINK_DEDUP_RETENTION_HOURS": "0"})
        with self.assertRaisesRegex(ValueError, "whole number of seconds"):
            MODULE.render({"FLINK_MAX_OUT_OF_ORDERNESS_MS": "1500"})


if __name__ == "__main__":
    unittest.main()
