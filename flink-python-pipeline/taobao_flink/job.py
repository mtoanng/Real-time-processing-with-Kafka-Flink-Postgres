"""Submit the native Flink Table/SQL streaming plan.

Python owns configuration and deployment only. Every per-record relational,
stateful, and event-time operation is expressed in SQL and executed inside the
Flink JVM runtime; no Python callback is present in the data path.
"""

from __future__ import annotations

from pyflink.table import EnvironmentSettings, TableEnvironment

from taobao_flink.config import PipelineConfig
from taobao_flink.sql import insert_statements, table_ddl, transformation_ddl


def _configure(config: PipelineConfig) -> TableEnvironment:
    """Create a streaming Table environment with explicit recovery contracts."""
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    flink = table_env.get_config().get_configuration()

    flink.set_string("pipeline.name", "Taobao SQL Streaming Platform")
    flink.set_integer("parallelism.default", 1)
    flink.set_string("table.local-time-zone", "UTC")
    flink.set_string("table.exec.state.ttl", f"{config.dedup_retention_hours} h")

    flink.set_string("state.backend.type", "rocksdb")
    flink.set_boolean("execution.checkpointing.incremental", True)
    flink.set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
    flink.set_string("execution.checkpointing.interval", f"{config.checkpoint_interval_ms} ms")
    flink.set_string("execution.checkpointing.storage", "filesystem")
    flink.set_string("execution.checkpointing.dir", config.checkpoint_dir)
    flink.set_string(
        "execution.checkpointing.externalized-checkpoint-retention",
        "RETAIN_ON_CANCELLATION",
    )

    flink.set_string("restart-strategy.type", "fixed-delay")
    flink.set_integer("restart-strategy.fixed-delay.attempts", config.restart_attempts)
    flink.set_string("restart-strategy.fixed-delay.delay", f"{config.restart_delay_ms} ms")

    # Stable planner-generated UIDs allow checkpoint restoration when the SQL
    # plan itself has not changed. SQL-plan upgrades are not claimed here.
    flink.set_string("table.exec.uid.generation", "ALWAYS")
    flink.set_string("table.exec.uid.format", "taobao_<id>_<type>_<transformation>")
    if config.connector_jar:
        flink.set_string("pipeline.jars", config.connector_jar.resolve().as_uri())
    return table_env


def build_job(config: PipelineConfig):
    """Build one StatementSet so every sink shares a single optimized job graph."""
    table_env = _configure(config)
    for statement in table_ddl(config):
        table_env.execute_sql(statement)
    for statement in transformation_ddl():
        table_env.execute_sql(statement)

    statement_set = table_env.create_statement_set()
    for statement in insert_statements():
        statement_set.add_insert_sql(statement)
    return statement_set


def main() -> None:
    """Load validated environment configuration and submit the SQL job."""
    build_job(PipelineConfig.from_environment()).execute()


if __name__ == "__main__":
    main()
