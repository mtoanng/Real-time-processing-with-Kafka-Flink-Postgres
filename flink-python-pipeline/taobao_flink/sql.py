from __future__ import annotations

import re
from pathlib import Path

from taobao_flink.config import PipelineConfig

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"
PLACEHOLDER = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


def _escape(value: str) -> str:
    if not value.strip():
        raise ValueError("SQL configuration values must not be blank")
    return value.replace("'", "''")


def _kafka_security_options(config: PipelineConfig, *, schema_registry: bool) -> str:
    options = []
    if config.kafka_security_protocol == "SASL_SSL":
        username = config.kafka_sasl_username.replace("\\", "\\\\").replace('"', '\\"')
        password = config.kafka_sasl_password.replace("\\", "\\\\").replace('"', '\\"')
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{username}" password="{password}";'
        )
        options.extend(
            [
                ("properties.security.protocol", "SASL_SSL"),
                ("properties.sasl.mechanism", config.kafka_sasl_mechanism),
                ("properties.sasl.jaas.config", jaas),
            ]
        )
    if schema_registry and config.schema_registry_basic_auth:
        options.extend(
            [
                ("value.avro-confluent.basic-auth.credentials-source", "USER_INFO"),
                (
                    "value.avro-confluent.basic-auth.user-info",
                    config.schema_registry_basic_auth,
                ),
            ]
        )
    return "".join(f",\n    '{key}' = '{_escape(value)}'" for key, value in options)


def render_tables(config: PipelineConfig) -> str:
    bounded = "'scan.bounded.mode' = 'latest-offset'," if config.bounded else ""
    values = {
        "KAFKA_BOOTSTRAP_SERVERS": config.kafka_bootstrap_servers,
        "SCHEMA_REGISTRY_URL": config.schema_registry_url,
        "BEHAVIOR_TOPIC": config.behavior_topic,
        "RAW_TOPIC": config.raw_topic,
        "METRICS_TOPIC": config.metrics_topic,
        "QUALITY_TOPIC": config.quality_topic,
        "CART_MUTATION_TOPIC": config.cart_mutation_topic,
        "CONSUMER_GROUP": config.consumer_group,
        "BEHAVIOR_BOUNDED_OPTION": bounded,
        "SOURCE_SECURITY_OPTIONS": _kafka_security_options(config, schema_registry=True),
        "KAFKA_SECURITY_OPTIONS": _kafka_security_options(config, schema_registry=False),
    }
    template = (SQL_DIR / "tables.sql.template").read_text(encoding="utf-8")
    raw_fragments = {
        "BEHAVIOR_BOUNDED_OPTION",
        "SOURCE_SECURITY_OPTIONS",
        "KAFKA_SECURITY_OPTIONS",
    }
    rendered = PLACEHOLDER.sub(
        lambda match: (
            values[match.group(1)]
            if match.group(1) in raw_fragments
            else _escape(values[match.group(1)])
        ),
        template,
    )
    unresolved = PLACEHOLDER.findall(rendered)
    if unresolved:
        raise ValueError(f"unresolved SQL placeholders: {', '.join(sorted(set(unresolved)))}")
    return rendered


def split_statements(sql: str) -> list[str]:
    statements = []
    buffer = []
    for line in sql.splitlines():
        if line.strip().startswith("--"):
            continue
        buffer.append(line)
        if line.rstrip().endswith(";"):
            statements.append("\n".join(buffer).strip())
            buffer = []
    if any(line.strip() for line in buffer):
        raise ValueError("SQL file has an unterminated statement")
    return statements


def table_ddl(config: PipelineConfig) -> list[str]:
    return split_statements(render_tables(config))


def transformation_ddl() -> list[str]:
    return split_statements((SQL_DIR / "transformations.sql").read_text(encoding="utf-8"))


def insert_statements() -> list[str]:
    return split_statements((SQL_DIR / "inserts.sql").read_text(encoding="utf-8"))
