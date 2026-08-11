from __future__ import annotations

import json
from pathlib import Path

from vn30f1m_core.cli import main


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_status_json(capsys):
    assert main(["status", "--root", str(REPO_ROOT), "--json"]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["canonical_timeframe"] == "15m"
    assert output["kafka_enabled"] is True
    assert output["paths"]["root"] == str(REPO_ROOT)


def test_status_human_output(capsys):
    assert main(["status", "--root", str(REPO_ROOT)]) == 0

    output = capsys.readouterr().out
    assert "project: VN30F1M Quant Platform" in output
    assert "canonical_timeframe: 15m" in output
    assert "kafka_enabled: true" in output
