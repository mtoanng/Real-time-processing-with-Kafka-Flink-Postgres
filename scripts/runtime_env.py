from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path


def load_environment(
    env_file: Path = Path(".env"), process_environment: Mapping[str, str] | None = None
) -> dict[str, str]:
    values: dict[str, str] = {}
    if env_file.is_file():
        for number, raw_line in enumerate(env_file.read_text(encoding="utf-8").splitlines(), 1):
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                raise ValueError(f"{env_file}:{number}: expected KEY=VALUE")
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key.replace("_", "").isalnum() or not key[0].isalpha():
                raise ValueError(f"{env_file}:{number}: invalid environment key")
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            values[key] = value
    values.update(os.environ if process_environment is None else process_environment)
    return values
