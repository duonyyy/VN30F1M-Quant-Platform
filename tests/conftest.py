from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
CORE_SRC = REPO_ROOT / "packages" / "vn30f1m_core"
DATASET_SRC = REPO_ROOT / "packages" / "vn30f1m_dataset"
STREAMING_SRC = REPO_ROOT / "packages" / "vn30f1m_streaming"
BATCH_SRC = REPO_ROOT / "packages" / "vn30f1m_batch"
sys.path.insert(0, str(CORE_SRC))
sys.path.insert(0, str(DATASET_SRC))
sys.path.insert(0, str(STREAMING_SRC))
sys.path.insert(0, str(BATCH_SRC))
