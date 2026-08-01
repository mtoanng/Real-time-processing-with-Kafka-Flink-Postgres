from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen


def _wait_for_connect(endpoint: str, attempts: int = 60) -> None:
    last_error = "not ready"
    for _ in range(attempts):
        try:
            with urlopen(endpoint, timeout=5) as response:  # noqa: S310
                if response.status == 200:
                    return
        except (OSError, URLError) as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(f"Kafka Connect did not become ready: {last_error}")


def main() -> None:
    path = Path("infra/debezium/product-catalog-connector.json")
    config = json.loads(path.read_text(encoding="utf-8"))
    config["database.password"] = os.environ["POSTGRES_PASSWORD"]
    payload = json.dumps(config).encode()
    endpoint = os.getenv("KAFKA_CONNECT_URL", "http://localhost:8083").rstrip("/")
    _wait_for_connect(endpoint)
    request = Request(
        f"{endpoint}/connectors/product-catalog/config",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urlopen(request, timeout=15) as response:  # noqa: S310
        if response.status not in {200, 201}:
            raise RuntimeError(f"Kafka Connect returned HTTP {response.status}")
    status_url = f"{endpoint}/connectors/product-catalog/status"
    last_state = "not reported"
    for _ in range(60):
        try:
            with urlopen(status_url, timeout=5) as response:  # noqa: S310
                status = json.load(response)
        except (OSError, URLError) as exc:
            last_state = str(exc)
            time.sleep(2)
            continue
        connector_state = status.get("connector", {}).get("state")
        task_states = [task.get("state") for task in status.get("tasks", [])]
        last_state = f"connector={connector_state}, tasks={task_states}"
        if (
            connector_state == "RUNNING"
            and task_states
            and all(state == "RUNNING" for state in task_states)
        ):
            return
        time.sleep(2)
    raise RuntimeError(f"product-catalog connector did not reach RUNNING: {last_state}")


if __name__ == "__main__":
    main()
