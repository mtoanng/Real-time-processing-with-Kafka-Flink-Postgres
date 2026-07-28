from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from scripts.runtime_env import load_environment


def request(url: str, method: str, payload: object | None, user_info: str) -> None:
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    if user_info:
        token = base64.b64encode(user_info.encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    body = None if payload is None else json.dumps(payload).encode()
    with urlopen(Request(url, data=body, headers=headers, method=method), timeout=15):  # noqa: S310
        return


def main() -> int:
    environment = load_environment()
    profile = environment.get("RUNTIME_PROFILE", "core")
    registry = (
        "http://localhost:8081/apis/ccompat/v7"
        if profile == "core"
        else environment["SCHEMA_REGISTRY_URL"]
    ).rstrip("/")
    user_info = environment.get("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", "")
    deadline = time.monotonic() + 120
    while True:
        try:
            request(f"{registry}/subjects", "GET", None, user_info)
            break
        except HTTPError as exc:
            raise RuntimeError(f"Schema Registry returned HTTP {exc.code}") from exc
        except URLError as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Schema Registry did not become ready within 120 seconds"
                ) from exc
            time.sleep(2)

    topic = environment.get("KAFKA_TOPIC", "user-behavior-events")
    subject = f"{topic}-value"
    schema = Path("schemas/user-behavior-event.avsc").read_text(encoding="utf-8")
    request(
        f"{registry}/config/{subject}",
        "PUT",
        {"compatibility": "BACKWARD"},
        user_info,
    )
    request(
        f"{registry}/subjects/{subject}/versions",
        "POST",
        {"schemaType": "AVRO", "schema": schema},
        user_info,
    )
    print(f"Registered {subject} with BACKWARD compatibility.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
