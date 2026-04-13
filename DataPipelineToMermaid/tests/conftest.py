"""Shared fixtures for DataPipelineToMermaid tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# Resolve paths relative to this file
_TESTS_DIR = Path(__file__).resolve().parent
_PACKAGE_DIR = _TESTS_DIR.parent
_FIXTURES_DIR = _PACKAGE_DIR / "test_fixtures"


@pytest.fixture()
def fixtures_dir() -> Path:
    """Return the test_fixtures directory."""
    return _FIXTURES_DIR


@pytest.fixture()
def sample_lineage_path(fixtures_dir: Path) -> Path:
    """Return path to the sample_lineage.json fixture."""
    return fixtures_dir / "sample_lineage.json"


@pytest.fixture()
def sample_lineage_dict(sample_lineage_path: Path) -> dict:
    """Load sample_lineage.json as a Python dict."""
    return json.loads(sample_lineage_path.read_text(encoding="utf-8"))


@pytest.fixture()
def sample_lineage(sample_lineage_path: Path):
    """Load sample_lineage.json as a PipelineLineage model."""
    from DataPipelineToMermaid.models import PipelineLineage

    return PipelineLineage.from_json(sample_lineage_path)


@pytest.fixture()
def tmp_output(tmp_path: Path) -> Path:
    """Return a temporary directory for test outputs."""
    out = tmp_path / "output"
    out.mkdir()
    return out
