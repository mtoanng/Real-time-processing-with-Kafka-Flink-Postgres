from __future__ import annotations

import json
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from taobao_replay.contracts import UserBehaviorEvent


class HttpEventPublisher:
    def __init__(self, endpoint: str) -> None:
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("endpoint must use http:// or https://")
        self._endpoint = endpoint

    def publish(self, event: UserBehaviorEvent) -> None:
        request = Request(
            self._endpoint,
            data=json.dumps(event.to_dict(), separators=(",", ":")).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=15) as response:  # noqa: S310
                if response.status != 202:
                    raise RuntimeError(f"event API returned HTTP {response.status}")
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"event API rejected the event: HTTP {exc.code}: {detail}") from exc
        except URLError as exc:
            raise RuntimeError(f"event API is unavailable: {exc.reason}") from exc

    def close(self, timeout: float = 30.0) -> None:
        del timeout
