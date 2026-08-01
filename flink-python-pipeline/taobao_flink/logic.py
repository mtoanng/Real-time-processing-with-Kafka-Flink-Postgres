"""Pure business rules shared by stateful PyFlink operators and unit tests.

Keeping these functions free of Flink types makes event and cart semantics easy
to test without a cluster.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

VALID_BEHAVIORS = frozenset({"pv", "cart", "fav", "buy"})


def validation_reason(
    event_id: str | None,
    user_id: int,
    item_id: int,
    category_id: int,
    behavior_type: str | None,
    event_time_ms: int,
) -> str | None:
    """Return a stable reason code, or ``None`` when an event is valid."""
    if not event_id or not event_id.strip():
        return "INVALID_EVENT_ID"
    if user_id <= 0 or item_id <= 0 or category_id <= 0:
        return "INVALID_IDENTIFIER"
    if event_time_ms <= 0:
        return "INVALID_EVENT_TIME"
    if behavior_type not in VALID_BEHAVIORS:
        return "INVALID_BEHAVIOR_TYPE"
    return None


def quality_event_id(
    quality_type: str,
    event_id: str | None,
    replay_run_id: str,
    source_sequence: int,
    reason_code: str,
) -> str:
    """Create deterministic audit identity for an invalid, duplicate or late event."""
    canonical = "\x1f".join(
        (quality_type, event_id or "", replay_run_id, str(source_sequence), reason_code)
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class CartState:
    """Last known state for one ``(user_id, item_id)`` cart projection key."""

    category_id: int
    added_at_ms: int
    last_event_time_ms: int
    last_source_sequence: int
    active: bool


@dataclass(frozen=True)
class CartMutation:
    """Idempotent change to publish to the compacted cart-mutation topic."""

    operation: str
    user_id: int
    item_id: int
    category_id: int
    added_at_ms: int
    last_updated_at_ms: int


def apply_cart_event(
    current: CartState | None,
    *,
    user_id: int,
    item_id: int,
    category_id: int,
    behavior_type: str,
    event_time_ms: int,
    source_sequence: int,
) -> tuple[CartState | None, CartMutation | None]:
    """Apply cart/buy semantics while rejecting stale event-time transitions.

    ``source_sequence`` is the deterministic tie-breaker for equal event times;
    it is not an instruction to reorder the source replay.
    """
    if behavior_type not in {"cart", "buy"}:
        return current, None
    event_order = (event_time_ms, source_sequence)
    if current and event_order <= (current.last_event_time_ms, current.last_source_sequence):
        return current, None
    if behavior_type == "buy":
        state = CartState(category_id, 0, event_time_ms, source_sequence, False)
        return state, CartMutation("DELETE", user_id, item_id, category_id, 0, event_time_ms)

    added_at = current.added_at_ms if current is not None and current.active else event_time_ms
    state = CartState(category_id, added_at, event_time_ms, source_sequence, True)
    return state, CartMutation("UPSERT", user_id, item_id, category_id, added_at, event_time_ms)
