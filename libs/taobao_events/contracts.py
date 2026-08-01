"""Source-row contract used by the external replay client and optional HTTP ingress.

This package simulates an upstream producer; it is not the Flink processing
pipeline.  ``event_id`` is stable across replay runs while ``replay_run_id``
records which delivery attempt carried it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass
from hashlib import sha256

BEHAVIOR_TYPES = frozenset({"pv", "cart", "fav", "buy"})
FIELD_NAMES = ("user_id", "item_id", "category_id", "behavior_type", "timestamp")


class RowValidationError(ValueError):
    """Raised when a source row does not satisfy the five-column contract."""


@dataclass(frozen=True, slots=True)
class UserBehaviorEvent:
    """Validated Kafka payload produced from one Taobao source-row occurrence."""

    event_id: str
    user_id: int
    item_id: int
    category_id: int
    behavior_type: str
    event_time_ms: int
    source_sequence: int
    replay_run_id: str

    def to_dict(self) -> dict[str, str | int]:
        """Return the portable event payload used by Avro and HTTP publishers."""
        return asdict(self)


def deterministic_event_id(
    *,
    user_id: int,
    item_id: int,
    category_id: int,
    behavior_type: str,
    timestamp: int,
    source_sequence: int,
) -> str:
    """Hash stable source fields plus sequence; intentionally exclude replay run ID."""
    canonical = "\x1f".join(
        str(value)
        for value in (
            user_id,
            item_id,
            category_id,
            behavior_type,
            timestamp,
            source_sequence,
        )
    )
    return sha256(canonical.encode("utf-8")).hexdigest()


def parse_event(
    values: Sequence[str], *, source_sequence: int, replay_run_id: str
) -> UserBehaviorEvent:
    """Validate one five-column CSV row and attach deterministic delivery identity."""
    if len(values) != len(FIELD_NAMES):
        raise RowValidationError(f"expected 5 columns, got {len(values)}")
    if source_sequence < 0:
        raise RowValidationError("source_sequence must be non-negative")
    if not replay_run_id.strip():
        raise RowValidationError("replay_run_id must not be blank")

    user_raw, item_raw, category_raw, behavior_raw, timestamp_raw = (
        value.strip() for value in values
    )
    try:
        user_id = int(user_raw)
        item_id = int(item_raw)
        category_id = int(category_raw)
        timestamp = int(timestamp_raw)
    except ValueError as exc:
        raise RowValidationError("IDs and timestamp must be integers") from exc

    if behavior_raw not in BEHAVIOR_TYPES:
        raise RowValidationError(
            f"behavior_type must be one of {sorted(BEHAVIOR_TYPES)}, got {behavior_raw!r}"
        )

    return UserBehaviorEvent(
        event_id=deterministic_event_id(
            user_id=user_id,
            item_id=item_id,
            category_id=category_id,
            behavior_type=behavior_raw,
            timestamp=timestamp,
            source_sequence=source_sequence,
        ),
        user_id=user_id,
        item_id=item_id,
        category_id=category_id,
        behavior_type=behavior_raw,
        event_time_ms=timestamp * 1_000,
        source_sequence=source_sequence,
        replay_run_id=replay_run_id,
    )
