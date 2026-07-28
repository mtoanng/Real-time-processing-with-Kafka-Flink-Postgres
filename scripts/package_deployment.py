from __future__ import annotations

import argparse
import subprocess
import tarfile
from pathlib import Path


def deployment_files(root: Path) -> list[Path]:
    result = subprocess.run(
        [
            "git",
            "-c",
            f"safe.directory={root.as_posix()}",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    files = []
    for name in result.stdout.splitlines():
        path = Path(name)
        if path.name == ".env" or "target" in path.parts or "__pycache__" in path.parts:
            continue
        if path.as_posix() == "data/UserBehavior.csv":
            continue
        if (root / path).is_file():
            files.append(path)
    return sorted(files)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("artifacts/taobao-aws-demo.tar.gz"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(args.output, "w:gz") as archive:
        for relative in deployment_files(root):
            archive.add(root / relative, arcname=Path("taobao-aws-demo") / relative)
    print(f"Created {args.output} without .env, raw data, Git metadata, or build outputs.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
