from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from taobao_replay.replay import replay_file


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class ReplayTests(unittest.TestCase):
    def test_replay_preserves_order_paces_forward_and_rejects_invalid(self) -> None:
        content = "\n".join(
            [
                "1,10,100,pv,100",
                "1,11,100,cart,102",
                "bad,12,100,pv,103",
                "1,13,100,fav,101",
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.csv"
            path.write_text(content + "\n", encoding="utf-8")
            emitted = []
            rejected = []
            clock = FakeClock()
            stats = replay_file(
                path,
                replay_run_id="test-run",
                emit=emitted.append,
                on_invalid=rejected.append,
                batch_size=2,
                speed=2,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
            )

        self.assertEqual([event.item_id for event in emitted], [10, 11, 13])
        self.assertEqual(clock.sleeps, [1.0])
        self.assertEqual(stats.source_rows, 4)
        self.assertEqual(stats.emitted_events, 3)
        self.assertEqual(stats.invalid_rows, 1)
        self.assertEqual(len(rejected), 1)


if __name__ == "__main__":
    unittest.main()
