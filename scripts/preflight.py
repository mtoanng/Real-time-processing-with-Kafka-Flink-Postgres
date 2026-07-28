from __future__ import annotations

import argparse
import os
from collections.abc import Mapping
from pathlib import Path

from scripts.runtime_env import load_environment

PROFILES = {"local-smoke", "cloud-demo"}


def configuration_errors(profile: str, environment: Mapping[str, str]) -> list[str]:
    if profile not in PROFILES:
        return [f"RUNTIME_PROFILE must be one of {sorted(PROFILES)}"]

    errors: list[str] = []
    kafka = environment.get("KAFKA_BOOTSTRAP_SERVERS", "")
    registry = environment.get("SCHEMA_REGISTRY_URL", "")
    if profile == "local-smoke":
        if kafka != "kafka:29092":
            errors.append("local-smoke KAFKA_BOOTSTRAP_SERVERS must be kafka:29092")
        if "schema-registry:8080" not in registry:
            errors.append("local-smoke SCHEMA_REGISTRY_URL must use schema-registry:8080")
        return errors

    required = (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
        "SCHEMA_REGISTRY_URL",
        "SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO",
        "POSTGRES_PASSWORD",
        "CLICKHOUSE_PASSWORD",
    )
    for key in required:
        if not environment.get(key, "").strip():
            errors.append(f"{key} is required for cloud-demo")

    if environment.get("KAFKA_SECURITY_PROTOCOL") != "SASL_SSL":
        errors.append("KAFKA_SECURITY_PROTOCOL must be SASL_SSL for cloud-demo")
    if environment.get("KAFKA_SASL_MECHANISM") != "PLAIN":
        errors.append("KAFKA_SASL_MECHANISM must be PLAIN for cloud-demo")
    if kafka.startswith(("kafka:", "localhost", "127.0.0.1")):
        errors.append("cloud-demo Kafka must use an external Confluent Cloud endpoint")
    if not registry.startswith("https://"):
        errors.append("cloud-demo Schema Registry URL must use HTTPS")
    if environment.get("POSTGRES_PASSWORD") == "local-catalog":
        errors.append("cloud-demo must replace the local PostgreSQL password")
    if environment.get("CLICKHOUSE_PASSWORD") == "local-clickhouse":
        errors.append("cloud-demo must replace the local ClickHouse password")
    try:
        replication = int(environment.get("KAFKA_CONNECT_REPLICATION_FACTOR", "0"))
        if replication < 3:
            errors.append("KAFKA_CONNECT_REPLICATION_FACTOR must be at least 3 for cloud-demo")
    except ValueError:
        errors.append("KAFKA_CONNECT_REPLICATION_FACTOR must be an integer")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", choices=sorted(PROFILES))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    environment = load_environment(args.env_file)
    profile = args.profile or environment.get("RUNTIME_PROFILE", "local-smoke")
    errors = configuration_errors(profile, environment)
    if profile == "cloud-demo" and os.name != "nt" and args.env_file.exists():
        if args.env_file.stat().st_mode & 0o077:
            errors.append(f"{args.env_file} must have mode 600")
    if errors:
        for error in errors:
            print(f"ERROR: {error}")
        return 2
    print(f"Preflight passed for {profile}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
