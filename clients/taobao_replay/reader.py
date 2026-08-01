"""Bounded-memory CSV reader for the simulated external producer."""

from __future__ import annotations

import csv
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from taobao_events.contracts import FIELD_NAMES, RowValidationError, UserBehaviorEvent, parse_event


@dataclass(frozen=True, slots=True)
class ParseIssue:
    """Producer-side rejection; it never reaches Kafka or Flink."""

    source_sequence: int
    raw_values: tuple[str, ...]
    reason: str

    def to_dict(self) -> dict[str, int | str | list[str]]:
        """Return JSON-safe evidence for replay reports."""
        return {
            "source_sequence": self.source_sequence,
            "raw_values": list(self.raw_values),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class ReplayBatch:
    """One bounded batch preserving the source order of valid and rejected rows."""

    events: tuple[UserBehaviorEvent, ...]
    issues: tuple[ParseIssue, ...]
    source_rows: int


def iter_source_rows(path: Path) -> Iterator[tuple[int, tuple[str, ...]]]:
    """Yield zero-based data rows, accepting headerless CSV or one canonical header."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        source_sequence = 0
        first_row = True
        for row in reader:
            values = tuple(row)
            if first_row and tuple(value.strip() for value in values) == FIELD_NAMES:
                first_row = False
                continue
            first_row = False
            yield source_sequence, values
            source_sequence += 1


def iter_event_batches(
    path: Path, *, replay_run_id: str, batch_size: int = 1_000
) -> Iterator[ReplayBatch]:
    """Parse at most batch_size source rows at a time without reordering input."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    events: list[UserBehaviorEvent] = []
    issues: list[ParseIssue] = []
    source_rows = 0

    for source_sequence, values in iter_source_rows(path):
        try:
            events.append(
                parse_event(
                    values,
                    source_sequence=source_sequence,
                    replay_run_id=replay_run_id,
                )
            )
        except RowValidationError as exc:
            issues.append(ParseIssue(source_sequence, values, str(exc)))
        source_rows += 1

        if source_rows == batch_size:
            yield ReplayBatch(tuple(events), tuple(issues), source_rows)
            events.clear()
            issues.clear()
            source_rows = 0

    if source_rows:
        yield ReplayBatch(tuple(events), tuple(issues), source_rows)
