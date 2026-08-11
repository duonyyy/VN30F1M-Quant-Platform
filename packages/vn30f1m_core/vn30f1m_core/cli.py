"""Command-line entry point for the VN30F1M platform."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .settings import Settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="vn30f1m", description="VN30F1M platform CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    status = subparsers.add_parser("status", help="show project paths and runtime settings")
    status.add_argument("--root", type=Path, help="override the platform repository root")
    status.add_argument("--json", action="store_true", help="print machine-readable JSON")
    return parser


def _print_status(settings: Settings, as_json: bool) -> None:
    payload = settings.as_dict()
    if as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return

    print(f"project: {payload['project_name']}")
    print(f"root: {payload['project_root']}")
    print(f"environment: {payload['environment']}")
    print(f"symbol: {payload['default_symbol']}")
    print(f"source_timeframe: {payload['source_timeframe']}")
    print(f"canonical_timeframe: {payload['canonical_timeframe']}")
    print(f"timezone: {payload['timezone']}")
    print(f"storage_backend: {payload['storage_backend']}")
    print(f"kafka_enabled: {str(payload['kafka_enabled']).lower()}")
    print(f"kafka_bootstrap_servers: {payload['kafka_bootstrap_servers']}")
    print(f"kafka_raw_topic: {payload['kafka_raw_topic']}")
    print(f"clickhouse_enabled: {str(payload['clickhouse_enabled']).lower()}")
    print("paths:")
    for name, path in payload["paths"].items():
        state = "present" if Path(path).exists() else "missing"
        print(f"  {name}: {path} ({state})")


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            settings = Settings.from_env(args.root)
            _print_status(settings, args.json)
            return 0
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
