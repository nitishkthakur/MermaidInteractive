"""Shared fixtures for cross-file lineage tests.

All tests that invoke the LLM use the cheap model configured in .env
(anthropic/claude-haiku-4.5 by default).  Tests are marked with
pytest.mark.llm so they can be skipped offline:

    pytest -m "not llm"        # fast, no API calls
    pytest -m llm              # integration tests (requires OPENROUTER_API_KEY)
"""

from __future__ import annotations

from pathlib import Path

import pytest


FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Original 7-file DAG: SQL + Ab-Initio only
ALL_FIXTURE_FILES = [
    str(FIXTURES_DIR / "01_raw_orders.sql"),
    str(FIXTURES_DIR / "02_raw_customers.sql"),
    str(FIXTURES_DIR / "03_raw_payments.sql"),
    str(FIXTURES_DIR / "04_enrich_orders.mp"),
    str(FIXTURES_DIR / "05_reconcile_payments.sql"),
    str(FIXTURES_DIR / "06_customer_risk.mp"),
    str(FIXTURES_DIR / "07_final_metrics.sql"),
]

# Extended 9-file DAG: adds two stored-procedure files (SP branch)
ALL_FIXTURE_FILES_WITH_SP = ALL_FIXTURE_FILES + [
    str(FIXTURES_DIR / "08_sp_loyalty_tiers.sql"),
    str(FIXTURES_DIR / "09_sp_segment_rewards.sql"),
]


@pytest.fixture(scope="session")
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture(scope="session")
def all_files() -> list[str]:
    return ALL_FIXTURE_FILES


@pytest.fixture(scope="session")
def all_files_with_sp() -> list[str]:
    return ALL_FIXTURE_FILES_WITH_SP


@pytest.fixture(scope="session")
def llm_model():
    """A shared cheap LLM model for the session (avoids re-authing per test)."""
    from dotenv import load_dotenv
    from pathlib import Path as P
    import os

    pkg_dir = P(__file__).resolve().parent.parent.parent
    load_dotenv(pkg_dir / ".env")

    from langchain_openrouter import ChatOpenRouter
    return ChatOpenRouter(
        model=os.getenv("LLM_MODEL", "anthropic/claude-haiku-4.5"),
        temperature=0,
        max_tokens=16384,
    )
