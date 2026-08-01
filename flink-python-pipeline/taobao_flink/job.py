"""Assemble the single recoverable SQL/PyFlink streaming topology.

The job reads behavior events once, validates and deduplicates them before raw
storage, routes late records away from one-minute metrics, and publishes Kafka
materialization contracts for ClickHouse and Redis.  It never writes either
serving store directly.
"""

from __future__ import annotations

from pyflink.common import Duration
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import ExternalizedCheckpointCleanup
from pyflink.datastream.checkpoint_storage import FileSystemCheckpointStorage
from pyflink.table import DataTypes, EnvironmentSettings, Schema, StreamTableEnvironment

from taobao_flink.config import PipelineConfig
from taobao_flink.operators import (
    CART_MUTATION_TYPE,
    DUPLICATES,
    EVENT_TYPE,
    INVALID_EVENTS,
    LATE_EVENTS,
    ActiveCartProjector,
    DeduplicateEventId,
    RouteClassifiedEvent,
    RouteLateEvent,
)
from taobao_flink.sql import insert_statements, table_ddl, transformation_ddl

EVENT_COLUMNS = """
event_id, user_id, item_id, category_id, behavior_type,
event_time_ms, source_sequence, replay_run_id
"""


class EventTimeAssigner(TimestampAssigner):
    """Use producer event time, never ingestion or replay time, for watermarks."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        del record_timestamp
        return value.event_time_ms


def _watermark_strategy(config: PipelineConfig) -> WatermarkStrategy:
    """Allow bounded disorder while making event-time lateness explicit."""
    return (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_millis(config.max_out_of_orderness_ms)
        )
        .with_timestamp_assigner(EventTimeAssigner())
        .with_idleness(Duration.of_seconds(30))
    )


def _configure(config: PipelineConfig):
    """Configure checkpoint-consistent Flink state, restart policy and SQL runtime."""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(config.checkpoint_interval_ms, CheckpointingMode.EXACTLY_ONCE)
    checkpoint = env.get_checkpoint_config()
    checkpoint.set_checkpoint_storage(FileSystemCheckpointStorage(config.checkpoint_dir))
    checkpoint.set_externalized_checkpoint_cleanup(
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(config.restart_attempts, config.restart_delay_ms)
    )
    if config.connector_jar:
        env.add_jars(config.connector_jar.resolve().as_uri())

    table_env = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().build(),
    )
    table_env.get_config().set("table.local-time-zone", "UTC")
    table_env.get_config().set("python.execution-mode", "process")
    # The job is constructed in one deterministic order. Explicit Table UIDs
    # complement the hand-authored DataStream UIDs for checkpoint restoration.
    table_env.get_config().set("table.exec.uid.generation", "ALWAYS")
    table_env.get_config().set("table.exec.uid.format", "taobao_<id>_<type>_<transformation>")
    return env, table_env


def build_job(config: PipelineConfig):
    """Build the topology without submitting it.

    Flow: decoded → validation → event-id dedup → canonical raw; then
    watermarks → late quality or one-minute metrics.  Cart state branches from
    accepted unique events because it is a serving projection, not a metric.
    """
    env, table_env = _configure(config)
    for statement in table_ddl(config):
        table_env.execute_sql(statement)
    for statement in transformation_ddl():
        table_env.execute_sql(statement)

    classified = table_env.sql_query(
        f"SELECT {EVENT_COLUMNS}, validation_reason FROM classified_events"
    )
    classified_stream = table_env.to_data_stream(classified)
    valid_stream = (
        classified_stream.process(RouteClassifiedEvent(), output_type=EVENT_TYPE)
        .name("RouteClassifiedEvent")
        .uid("route-classified-event")
    )
    invalid_quality = valid_stream.get_side_output(INVALID_EVENTS)
    accepted = (
        valid_stream.key_by(lambda row: row.event_id)
        .process(DeduplicateEventId(config.dedup_retention_hours), output_type=EVENT_TYPE)
        .name("DeduplicateEventId")
        .uid("deduplicate-event-id")
    )
    duplicate_quality = accepted.get_side_output(DUPLICATES)

    with_watermarks = (
        accepted.assign_timestamps_and_watermarks(_watermark_strategy(config))
        .name("AssignEventTimeAndWatermarks")
        .uid("assign-event-time-watermarks")
    )
    on_time = (
        with_watermarks.key_by(lambda row: row.event_id)
        .process(RouteLateEvent(), output_type=EVENT_TYPE)
        .name("RouteLateEvents")
        .uid("route-late-events")
    )
    late_quality = on_time.get_side_output(LATE_EVENTS)
    quality = invalid_quality.union(duplicate_quality, late_quality)

    cart_mutations = (
        accepted.key_by(lambda row: f"{row.user_id}:{row.item_id}")
        .process(
            ActiveCartProjector(config.cart_state_ttl_hours),
            output_type=CART_MUTATION_TYPE,
        )
        .name("ProjectUserActiveCart")
        .uid("project-user-active-cart")
    )

    table_env.create_temporary_view("accepted_unique_events", table_env.from_data_stream(accepted))
    on_time_schema = (
        Schema.new_builder()
        .column_by_metadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
        .watermark("rowtime", "SOURCE_WATERMARK()")
        .build()
    )
    table_env.create_temporary_view(
        "on_time_events",
        table_env.from_data_stream(on_time, on_time_schema),
    )
    table_env.create_temporary_view("quality_events", table_env.from_data_stream(quality))
    table_env.create_temporary_view("cart_mutations", table_env.from_data_stream(cart_mutations))

    statement_set = table_env.create_statement_set()
    for statement in insert_statements():
        statement_set.add_insert_sql(statement)
    statement_set.attach_as_datastream()
    return env


def main() -> None:
    """Load deployment configuration and submit the named Flink job."""
    config = PipelineConfig.from_environment()
    build_job(config).execute("Taobao Python-SQL Streaming Platform")


if __name__ == "__main__":
    main()
