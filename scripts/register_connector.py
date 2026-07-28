from __future__ import annotations

import json
from pathlib import Path
from urllib.request import Request, urlopen

from scripts.runtime_env import load_environment


def main() -> int:
    environment = load_environment()
    connector = json.loads(
        Path("infra/debezium/product-catalog-connector.json").read_text(encoding="utf-8")
    )
    connector["database.dbname"] = environment.get("POSTGRES_DB", "catalog")
    connector["database.user"] = environment.get("POSTGRES_USER", "catalog")
    connector["database.password"] = environment.get("POSTGRES_PASSWORD", "local-catalog")
    endpoint = environment.get("KAFKA_CONNECT_URL", "http://localhost:8083")
    request = Request(
        f"{endpoint}/connectors/product-catalog-postgres/config",
        data=json.dumps(connector).encode(),
        headers={"Content-Type": "application/json"},
        method="PUT",
    )
    with urlopen(request, timeout=30) as response:  # noqa: S310
        if response.status not in {200, 201}:
            raise RuntimeError(f"Kafka Connect returned HTTP {response.status}")
    print("Registered product-catalog-postgres connector.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
