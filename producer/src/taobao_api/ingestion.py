from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict
from taobao_replay.contracts import UserBehaviorEvent, deterministic_event_id


class BehaviorEventRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_id: str | None = None
    user_id: int
    item_id: int
    category_id: int
    behavior_type: str
    event_time_ms: int
    source_sequence: int
    replay_run_id: str

    def validated_event(self) -> UserBehaviorEvent:
        if self.event_time_ms < 0 or self.event_time_ms % 1_000:
            raise ValueError("event_time_ms must be a non-negative whole second")
        if self.source_sequence < 0:
            raise ValueError("source_sequence must be non-negative")
        if not self.replay_run_id.strip():
            raise ValueError("replay_run_id must not be blank")
        expected = deterministic_event_id(
            user_id=self.user_id,
            item_id=self.item_id,
            category_id=self.category_id,
            behavior_type=self.behavior_type,
            timestamp=self.event_time_ms // 1_000,
            source_sequence=self.source_sequence,
        )
        if self.event_id is not None and self.event_id != expected:
            raise ValueError("event_id does not match the deterministic source identity")
        return UserBehaviorEvent(
            event_id=expected,
            user_id=self.user_id,
            item_id=self.item_id,
            category_id=self.category_id,
            behavior_type=self.behavior_type,
            event_time_ms=self.event_time_ms,
            source_sequence=self.source_sequence,
            replay_run_id=self.replay_run_id,
        )


class ConfirmedPublisher(Protocol):
    def publish_confirmed(self, event: UserBehaviorEvent) -> None: ...
