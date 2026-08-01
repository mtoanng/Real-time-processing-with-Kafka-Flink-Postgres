"""Optional thin HTTP boundary for external event ingress and serving reads.

The API re-computes event identity before publishing to the same Kafka contract
as the replay client. It reads Redis cart state and ClickHouse analytics; it
does not contain Flink business logic.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, status
from taobao_events.kafka import KafkaEventPublisher, build_schema_registry_producer

from taobao_api.ingestion import BehaviorEventRequest, ConfirmedPublisher
from taobao_api.serving import ServingStore


def create_app(
    publisher: ConfirmedPublisher | None = None,
    serving: ServingStore | None = None,
) -> FastAPI:
    """Create a testable API with optional injected Kafka and serving adapters."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if publisher is None:
            producer = build_schema_registry_producer(
                bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                schema_path=Path("schemas/user-behavior-event.avsc"),
            )
            app.state.publisher = KafkaEventPublisher(
                producer, topic=os.getenv("KAFKA_TOPIC", "user-behavior-events")
            )
        if serving is None:
            app.state.serving = ServingStore.from_environment()
        yield
        if publisher is None:
            app.state.publisher.close()
        if serving is None:
            app.state.serving.close()

    app = FastAPI(title="Taobao event and serving API", lifespan=lifespan)

    @app.post("/v1/events", status_code=status.HTTP_202_ACCEPTED)
    def ingest(request: BehaviorEventRequest) -> dict[str, object]:
        try:
            event = request.validated_event()
            active_publisher = publisher or app.state.publisher
            active_publisher.publish_confirmed(event)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=503, detail="Kafka publish failed") from exc
        return {"accepted": True, "event_id": event.event_id}

    @app.get("/v1/users/{user_id}/cart")
    def cart(user_id: int) -> dict[str, object]:
        if user_id <= 0:
            raise HTTPException(status_code=422, detail="user_id must be positive")
        return (serving or app.state.serving).cart(user_id)

    @app.get("/v1/users/{user_id}/features")
    def features(user_id: int) -> dict[str, object]:
        if user_id <= 0:
            raise HTTPException(status_code=422, detail="user_id must be positive")
        return (serving or app.state.serving).features(user_id)

    @app.get("/v1/products/trending")
    def trending(
        minutes: int = Query(15, ge=1, le=1_440),
        as_of_ms: int | None = Query(None, ge=0),
    ) -> dict[str, object]:
        return (serving or app.state.serving).trending(minutes, as_of_ms)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
