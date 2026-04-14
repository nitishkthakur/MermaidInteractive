"""Deep Agent harness for lineage extraction.

Uses ``deepagents.create_deep_agent`` with an OpenRouter-backed LLM to
read source code and produce a ``PipelineLineage`` JSON.

Configuration is read from a ``.env`` file (see ``.env.template``).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

from .models import PipelineLineage
from .prompt_template import LINEAGE_EXTRACTION_PROMPT


# ── Helpers ─────────────────────────────────────────────────────────


def _load_env() -> None:
    """Load .env from the DataPipelineToMermaid directory (or cwd)."""
    pkg_dir = Path(__file__).resolve().parent
    for candidate in [pkg_dir / ".env", Path.cwd() / ".env"]:
        if candidate.exists():
            load_dotenv(candidate)
            return
    load_dotenv()  # fall back to default search


def _build_model() -> Any:
    """Construct a ChatOpenRouter model."""
    _load_env()
    if not os.getenv("OPENROUTER_API_KEY", "").strip():
        print(
            "ERROR: OPENROUTER_API_KEY is not set.\n"
            "  1. Copy .env.example → .env\n"
            "  2. Paste your OpenRouter API key\n"
            "  3. Re-run.",
            file=sys.stderr,
        )
        sys.exit(1)

    model_name = os.getenv("LLM_MODEL", "anthropic/claude-sonnet-4").strip()
    max_tokens = int(os.getenv("LLM_MAX_TOKENS", "16384"))
    temperature = float(os.getenv("LLM_TEMPERATURE", "0"))

    from langchain_openrouter import ChatOpenRouter  # lazy import

    return ChatOpenRouter(
        model=model_name,
        temperature=temperature,
        max_tokens=max_tokens,
    )


# ── Agent construction ──────────────────────────────────────────────


def create_lineage_agent() -> Any:
    """Build a Deep Agent configured for lineage extraction.

    Returns a compiled LangGraph ``CompiledStateGraph`` that accepts
    ``{"messages": [...]}`` and returns ``{"messages": [...], ...}``.
    """
    from deepagents import create_deep_agent  # lazy import

    model = _build_model()
    agent = create_deep_agent(
        model=model,
        system_prompt=LINEAGE_EXTRACTION_PROMPT,
    )
    return agent


# ── Extraction workflow ─────────────────────────────────────────────


def _extract_json_from_text(text: str) -> str:
    """Pull a JSON object out of the agent's response text.

    Handles:
      • Raw JSON (starts with ``{``)
      • Markdown-fenced JSON (```json ... ```)
      • Text before/after the JSON block
    """
    # Handle list-of-blocks (Anthropic content format)
    if isinstance(text, list):
        parts = []
        for block in text:
            if isinstance(block, str):
                parts.append(block)
            elif isinstance(block, dict) and "text" in block:
                parts.append(block["text"])
            elif hasattr(block, "text"):
                parts.append(block.text)
        text = "\n".join(parts)
    text = str(text).strip()

    # Markdown fences
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start)
        return text[start:end].strip()
    if "```" in text:
        start = text.index("```") + 3
        # skip optional language tag on same line
        nl = text.index("\n", start)
        end = text.index("```", nl)
        return text[nl:end].strip()

    # Raw JSON — find first { and last }
    try:
        brace_start = text.index("{")
        brace_end = text.rindex("}") + 1
        return text[brace_start:brace_end]
    except ValueError:
        raise ValueError(
            f"No JSON object found in LLM response "
            f"({len(text)} chars). Response begins with: "
            f"{text[:200]!r}"
        )


def extract_lineage(
    source_path: str,
    *,
    agent: Any | None = None,
    verbose: bool = False,
) -> PipelineLineage:
    """Read *source_path*, send to the Deep Agent, return parsed lineage.

    Parameters
    ----------
    source_path : str
        Path to the SQL / Python / XML file to analyse.
    agent : optional
        Pre-built agent (for reuse across calls).  Built automatically if
        ``None``.
    verbose : bool
        If ``True``, print progress messages to stderr.
    """
    code = Path(source_path).read_text(encoding="utf-8")
    if agent is None:
        if verbose:
            print("  → Building lineage agent …", file=sys.stderr)
        agent = create_lineage_agent()

    suffix = Path(source_path).suffix.lstrip(".")
    user_msg = (
        f"Analyze the following {suffix.upper()} code and extract complete "
        f"column-level data lineage.\n\n"
        f"Source file: {source_path}\n\n"
        f"```{suffix}\n{code}\n```\n\n"
        f"Produce the lineage JSON following the exact schema from your "
        f"instructions.  Output ONLY the JSON — no commentary."
    )

    if verbose:
        print(
            f"  → Sending {len(code):,} chars to LLM …", file=sys.stderr
        )

    result = agent.invoke(
        {"messages": [{"role": "user", "content": user_msg}]}
    )

    # The agent response is in the last AI message
    response_content = result["messages"][-1].content

    # Handle list-of-blocks format (Anthropic models return this)
    if isinstance(response_content, list):
        text_parts = []
        for block in response_content:
            if isinstance(block, str):
                text_parts.append(block)
            elif isinstance(block, dict) and "text" in block:
                text_parts.append(block["text"])
            elif hasattr(block, "text"):
                text_parts.append(block.text)
        response_text: str = "\n".join(text_parts)
    else:
        response_text: str = str(response_content)

    # Check if the agent wrote to a file instead
    if "WRITTEN_TO_FILE:" in response_text:
        # Deep Agent virtual FS — extract from state
        file_path = response_text.split("WRITTEN_TO_FILE:")[-1].strip()
        files = result.get("files", {})
        if file_path in files:
            response_text = (
                files[file_path].get("content", "")
                if isinstance(files[file_path], dict)
                else str(files[file_path])
            )
        else:
            # Fallback: try to find JSON in the message anyway
            pass

    if verbose:
        print(
            f"  → Received {len(response_text):,} chars, parsing …",
            file=sys.stderr,
        )

    json_str = _extract_json_from_text(response_text)
    data = json.loads(json_str)
    lineage = PipelineLineage(**data)

    if verbose:
        print(
            f"  ✓ Extracted {len(lineage.sources)} sources, "
            f"{len(lineage.targets)} targets, "
            f"{len(lineage.column_lineage)} lineage mappings, "
            f"{len(lineage.components)} components",
            file=sys.stderr,
        )

    return lineage


# ── Convenience: extract from raw text (no file) ───────────────────


def extract_lineage_from_text(
    code: str,
    *,
    code_type: str = "sql",
    name: str = "inline",
    agent: Any | None = None,
    verbose: bool = False,
) -> PipelineLineage:
    """Like ``extract_lineage`` but accepts code as a string."""
    if agent is None:
        agent = create_lineage_agent()

    user_msg = (
        f"Analyze the following {code_type.upper()} code and extract "
        f"complete column-level data lineage.\n\n"
        f"Pipeline name: {name}\n\n"
        f"```{code_type}\n{code}\n```\n\n"
        f"Produce the lineage JSON following the exact schema from your "
        f"instructions.  Output ONLY the JSON — no commentary."
    )

    result = agent.invoke(
        {"messages": [{"role": "user", "content": user_msg}]}
    )

    response_text = result["messages"][-1].content
    json_str = _extract_json_from_text(response_text)
    data = json.loads(json_str)
    return PipelineLineage(**data)
