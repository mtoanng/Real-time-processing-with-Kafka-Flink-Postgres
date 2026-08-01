"""Bounded deterministic replay for fixtures and external-client simulation."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from taobao_events.contracts import UserBehaviorEvent

from taobao_replay.reader import ParseIssue, iter_event_batches


@dataclass(frozen=True, slots=True)
class ReplayStats:
    """Counts emitted and producer-rejected rows without claiming Flink outcomes."""

    source_rows: int
    emitted_events: int
    invalid_rows: int
    batches: int


def replay_file(
    path: Path,
    *,
    replay_run_id: str,
    emit: Callable[[UserBehaviorEvent], None],
    on_invalid: Callable[[ParseIssue], None] | None = None,
    batch_size: int = 1_000,
    speed: float = 0,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayStats:
    """Replay in source order; ``speed=0`` disables pacing.

    Source order is preserved by iteration.  ``source_sequence`` distinguishes
    repeated source rows and event-time ties; it does not make event time sorted.
    """
    if speed < 0:
        raise ValueError("speed must be non-negative")

    source_rows = emitted_events = invalid_rows = batches = 0
    first_event_time_ms: int | None = None
    replay_started = 0.0

    for batch in iter_event_batches(path, replay_run_id=replay_run_id, batch_size=batch_size):
        batches += 1
        source_rows += batch.source_rows
        invalid_rows += len(batch.issues)
        if on_invalid is not None:
            for issue in batch.issues:
                on_invalid(issue)

        for event in batch.events:
            if first_event_time_ms is None:
                first_event_time_ms = event.event_time_ms
                replay_started = monotonic()
            elif speed > 0:
                event_offset = max(0, event.event_time_ms - first_event_time_ms) / 1_000
                delay = replay_started + (event_offset / speed) - monotonic()
                if delay > 0:
                    sleep(delay)
            emit(event)
            emitted_events += 1

    return ReplayStats(source_rows, emitted_events, invalid_rows, batches)
