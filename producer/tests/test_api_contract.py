from __future__ import annotations

import unittest

from fastapi.testclient import TestClient
from taobao_api.app import create_app
from taobao_replay.contracts import parse_event
from taobao_replay.kafka import event_to_avro, kafka_key


class FakePublisher:
    def __init__(self) -> None:
        self.events = []

    def publish_confirmed(self, event: object) -> None:
        self.events.append(event)


class FakeServing:
    def cart(self, user_id: int) -> dict[str, object]:
        return {"user_id": user_id, "items": [], "ttl_seconds": -2}

    def trending(self, minutes: int, as_of_ms: int | None) -> dict[str, object]:
        return {"window_minutes": minutes, "as_of_ms": as_of_ms, "products": []}


class ApiContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.direct = parse_event(
            ("42", "500", "50", "cart", "1511658020"),
            source_sequence=7,
            replay_run_id="http-run",
        )
        self.publisher = FakePublisher()
        self.client = TestClient(create_app(self.publisher, FakeServing()))

    def test_http_transport_preserves_identity_key_and_avro_business_payload(self) -> None:
        response = self.client.post("/v1/events", json=self.direct.to_dict())

        self.assertEqual(202, response.status_code)
        self.assertEqual({"accepted": True, "event_id": self.direct.event_id}, response.json())
        published = self.publisher.events[0]
        self.assertEqual(self.direct, published)
        self.assertEqual(kafka_key(self.direct), kafka_key(published))
        self.assertEqual(event_to_avro(self.direct), event_to_avro(published))

    def test_http_boundary_rejects_a_forged_event_id(self) -> None:
        payload = self.direct.to_dict()
        payload["event_id"] = "0" * 64

        response = self.client.post("/v1/events", json=payload)

        self.assertEqual(422, response.status_code)
        self.assertEqual(
            "event_id does not match the deterministic source identity",
            response.json()["detail"],
        )

    def test_fixture_timestamp_must_remain_whole_seconds(self) -> None:
        payload = self.direct.to_dict()
        payload["event_time_ms"] = int(payload["event_time_ms"]) + 1

        response = self.client.post("/v1/events", json=payload)

        self.assertEqual(422, response.status_code)

    def test_serving_routes_return_logical_backing_state(self) -> None:
        self.assertEqual(
            {"user_id": 42, "items": [], "ttl_seconds": -2},
            self.client.get("/v1/users/42/cart").json(),
        )
        self.assertEqual(
            {"window_minutes": 15, "as_of_ms": 1511658120000, "products": []},
            self.client.get(
                "/v1/products/trending",
                params={"minutes": 15, "as_of_ms": 1511658120000},
            ).json(),
        )


if __name__ == "__main__":
    unittest.main()
