"""Load runtime configuration from config.yaml.

Usage::

    from DataPipelineToMermaid.config import get_config

    cfg = get_config()
    mode = cfg.get("artifact_mode", "regular")
"""

from __future__ import annotations

from pathlib import Path

_CONFIG_PATH = Path(__file__).resolve().parent / "config.yaml"

_ALLOWED_ARTIFACT_MODES = {"regular", "long", "wide"}


def get_config() -> dict:
    """Return the parsed config.yaml as a plain dict.

    If the file does not exist or is empty, returns ``{}``.
    Unknown keys are passed through untouched so callers can handle them.
    """
    if not _CONFIG_PATH.exists():
        return {}
    try:
        import yaml  # PyYAML — listed in requirements.txt
        data = yaml.safe_load(_CONFIG_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_artifact_mode() -> str:
    """Return the configured artifact_mode, defaulting to ``'regular'``."""
    mode = get_config().get("artifact_mode", "regular")
    if mode not in _ALLOWED_ARTIFACT_MODES:
        raise ValueError(
            f"config.yaml: artifact_mode '{mode}' is not valid. "
            f"Choose one of: {sorted(_ALLOWED_ARTIFACT_MODES)}"
        )
    return mode
