from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Mapping
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.runtime_env import load_environment  # noqa: E402

TEMPLATE = Path(__file__).with_name("pipeline.sql.template")
DEFAULT_OUTPUT = ROOT / "build/flink/pipeline.sql"
PLACEHOLDER = re.compile(r"\{\{([A-Z0-9_]+)\}\}")


def _positive_int(values: Mapping[str, str], key: str, default: int) -> int:
    raw = values.get(key, str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{key} must be greater than zero")
    return value


def _sql(value: str, key: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{key} must not be blank")
    return value.replace("'", "''")


def _security_options(values: Mapping[str, str], *, source: bool) -> str:
    protocol = values.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    if protocol == "PLAINTEXT":
        kafka_options: list[tuple[str, str]] = []
    elif protocol == "SASL_SSL":
        mechanism = _sql(values.get("KAFKA_SASL_MECHANISM", ""), "KAFKA_SASL_MECHANISM")
        username = _sql(values.get("KAFKA_SASL_USERNAME", ""), "KAFKA_SASL_USERNAME")
        password = _sql(values.get("KAFKA_SASL_PASSWORD", ""), "KAFKA_SASL_PASSWORD")
        jaas = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{username}" password="{password}";'
        )
        kafka_options = [
            ("properties.security.protocol", protocol),
            ("properties.sasl.mechanism", mechanism),
            ("properties.sasl.jaas.config", jaas),
        ]
    else:
        raise ValueError("KAFKA_SECURITY_PROTOCOL must be PLAINTEXT or SASL_SSL")

    if source:
        registry_auth = values.get("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "").strip()
        if registry_auth:
            kafka_options.extend(
                [
                    ("value.avro-confluent.basic-auth.credentials-source", "USER_INFO"),
                    (
                        "value.avro-confluent.basic-auth.user-info",
                        _sql(registry_auth, "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO"),
                    ),
                ]
            )
    return "".join(f",\n    '{key}' = '{value}'" for key, value in kafka_options)


def render(values: Mapping[str, str]) -> str:
    dedup_hours = _positive_int(values, "FLINK_DEDUP_RETENTION_HOURS", 168)
    checkpoint_ms = _positive_int(values, "FLINK_CHECKPOINT_INTERVAL_MS", 60_000)
    out_of_orderness_ms = _positive_int(values, "FLINK_MAX_OUT_OF_ORDERNESS_MS", 5_000)
    if out_of_orderness_ms % 1000:
        raise ValueError("FLINK_MAX_OUT_OF_ORDERNESS_MS must be a whole number of seconds")

    replacements = {
        "CHECKPOINT_INTERVAL": f"{checkpoint_ms} ms",
        "DEDUP_RETENTION": f"{dedup_hours} h",
        "MAX_OUT_OF_ORDERNESS_SECONDS": str(out_of_orderness_ms // 1000),
        "INPUT_TOPIC": _sql(values.get("KAFKA_TOPIC", "user-behavior-events"), "KAFKA_TOPIC"),
        "RAW_OUTPUT_TOPIC": _sql(
            values.get("KAFKA_RAW_OUTPUT_TOPIC", "taobao-raw-events"), "KAFKA_RAW_OUTPUT_TOPIC"
        ),
        "METRICS_OUTPUT_TOPIC": _sql(
            values.get("KAFKA_METRICS_OUTPUT_TOPIC", "taobao-item-metrics-1m"),
            "KAFKA_METRICS_OUTPUT_TOPIC",
        ),
        "QUALITY_OUTPUT_TOPIC": _sql(
            values.get("KAFKA_QUALITY_OUTPUT_TOPIC", "taobao-quality-events"),
            "KAFKA_QUALITY_OUTPUT_TOPIC",
        ),
        "KAFKA_BOOTSTRAP_SERVERS": _sql(
            values.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            "KAFKA_BOOTSTRAP_SERVERS",
        ),
        "KAFKA_CONSUMER_GROUP": _sql(
            values.get("KAFKA_CONSUMER_GROUP", "taobao-sql-pipeline"),
            "KAFKA_CONSUMER_GROUP",
        ),
        "SCHEMA_REGISTRY_URL": _sql(
            values.get(
                "SCHEMA_REGISTRY_URL",
                "http://schema-registry:8080/apis/ccompat/v7",
            ),
            "SCHEMA_REGISTRY_URL",
        ),
        "SOURCE_SECURITY_OPTIONS": _security_options(values, source=True),
        "SINK_SECURITY_OPTIONS": _security_options(values, source=False),
    }
    template = TEMPLATE.read_text(encoding="utf-8")
    rendered = PLACEHOLDER.sub(lambda match: replacements[match.group(1)], template)
    unresolved = sorted(set(PLACEHOLDER.findall(rendered)))
    if unresolved:
        raise ValueError(f"unresolved SQL placeholders: {', '.join(unresolved)}")
    return rendered


def main() -> int:
    parser = argparse.ArgumentParser(description="Render the environment-specific Flink SQL job")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    rendered = render(load_environment(process_environment=os.environ))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8", newline="\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
