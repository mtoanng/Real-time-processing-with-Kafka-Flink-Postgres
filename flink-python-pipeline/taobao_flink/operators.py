"""Small PyFlink escape hatches for stateful semantics not expressed in SQL.

SQL owns relational validation and one-minute aggregation.  These operators
only provide bounded event-id deduplication, durable side outputs for quality
events, event-time late routing, and the active-cart projection.
"""

from __future__ import annotations

import time

from pyflink.common import Row, Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import OutputTag
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction, RuntimeContext
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor

from taobao_flink.logic import apply_cart_event, quality_event_id

EVENT_TYPE = Types.ROW_NAMED(
    [
        "event_id",
        "user_id",
        "item_id",
        "category_id",
        "behavior_type",
        "event_time_ms",
        "source_sequence",
        "replay_run_id",
    ],
    [
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.STRING(),
    ],
)

QUALITY_TYPE = Types.ROW_NAMED(
    [
        "quality_event_id",
        "quality_type",
        "event_id",
        "user_id",
        "item_id",
        "category_id",
        "behavior_type",
        "event_time_ms",
        "replay_run_id",
        "source_sequence",
        "reason_code",
        "reason_message",
        "observed_at_ms",
        "record_version",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
    ],
)

CART_MUTATION_TYPE = Types.ROW_NAMED(
    [
        "operation",
        "user_id",
        "item_id",
        "category_id",
        "added_at_ms",
        "last_updated_at_ms",
    ],
    [
        Types.STRING(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
        Types.LONG(),
    ],
)

DUPLICATES = OutputTag("duplicate-quality", QUALITY_TYPE)
INVALID_EVENTS = OutputTag("invalid-quality", QUALITY_TYPE)
LATE_EVENTS = OutputTag("late-quality", QUALITY_TYPE)


def _quality(row: Row, quality_type: str, reason: str, message: str) -> Row:
    """Build one replay-safe quality record from a behavior event."""
    observed = int(time.time() * 1000)
    return Row(
        quality_event_id(
            quality_type, row.event_id, row.replay_run_id, row.source_sequence, reason
        ),
        quality_type,
        row.event_id,
        row.user_id,
        row.item_id,
        row.category_id,
        row.behavior_type,
        row.event_time_ms,
        row.replay_run_id,
        row.source_sequence,
        reason,
        message,
        observed,
        observed,
    )


class DeduplicateEventId(KeyedProcessFunction):
    """Keep the first event ID within a configured TTL and audit later copies."""

    def __init__(self, retention_hours: int) -> None:
        self._retention_hours = retention_hours
        self._seen = None

    def open(self, runtime_context: RuntimeContext) -> None:
        """Create checkpointed keyed state whose TTL bounds the dedup guarantee."""
        descriptor = ValueStateDescriptor("seen-event-id", Types.BOOLEAN())
        ttl = (
            StateTtlConfig.new_builder(Time.hours(self._retention_hours))
            .update_ttl_on_create_and_write()
            .never_return_expired()
            .cleanup_full_snapshot()
            .build()
        )
        descriptor.enable_time_to_live(ttl)
        self._seen = runtime_context.get_state(descriptor)

    def process_element(self, value: Row, ctx: KeyedProcessFunction.Context):
        del ctx
        if self._seen.value():
            yield (
                DUPLICATES,
                _quality(
                    value,
                    "DUPLICATE",
                    "DUPLICATE_WITHIN_RETENTION",
                    "event_id was already observed within the configured state TTL",
                ),
            )
            return
        self._seen.update(True)
        yield value


class RouteClassifiedEvent(ProcessFunction):
    """Split one SQL-classified source stream without creating a second Kafka scan."""

    def process_element(self, value: Row, ctx: ProcessFunction.Context):
        del ctx
        if value.validation_reason is not None:
            yield (
                INVALID_EVENTS,
                _quality(
                    value,
                    "INVALID",
                    value.validation_reason,
                    "event failed the source-faithful semantic contract",
                ),
            )
            return
        yield Row(
            value.event_id,
            value.user_id,
            value.item_id,
            value.category_id,
            value.behavior_type,
            value.event_time_ms,
            value.source_sequence,
            value.replay_run_id,
        )


class RouteLateEvent(KeyedProcessFunction):
    """Exclude events at or behind the current watermark from closed metrics."""

    def process_element(self, value: Row, ctx: KeyedProcessFunction.Context):
        if value.event_time_ms <= ctx.timer_service().current_watermark():
            yield (
                LATE_EVENTS,
                _quality(
                    value,
                    "LATE",
                    "LATE_FOR_AGGREGATION",
                    "event_time is at or behind the current watermark",
                ),
            )
            return
        yield value


class ActiveCartProjector(KeyedProcessFunction):
    """Derive ordered cart UPSERT/DELETE mutations; Redis is applied downstream."""

    def __init__(self, retention_hours: int) -> None:
        self._retention_hours = retention_hours

    def open(self, runtime_context: RuntimeContext) -> None:
        """Create bounded checkpointed state for each user-item projection key."""
        descriptor = ValueStateDescriptor("active-cart-item-state", Types.PICKLED_BYTE_ARRAY())
        descriptor.enable_time_to_live(
            StateTtlConfig.new_builder(Time.hours(self._retention_hours))
            .update_ttl_on_create_and_write()
            .never_return_expired()
            .cleanup_full_snapshot()
            .build()
        )
        self._state = runtime_context.get_state(descriptor)

    def process_element(self, value: Row, _ctx: KeyedProcessFunction.Context):
        current = self._state.value()
        state, mutation = apply_cart_event(
            current,
            user_id=value.user_id,
            item_id=value.item_id,
            category_id=value.category_id,
            behavior_type=value.behavior_type,
            event_time_ms=value.event_time_ms,
            source_sequence=value.source_sequence,
        )
        if state != current:
            self._state.update(state)
        if mutation:
            yield Row(
                mutation.operation,
                mutation.user_id,
                mutation.item_id,
                mutation.category_id,
                mutation.added_at_ms,
                mutation.last_updated_at_ms,
            )
