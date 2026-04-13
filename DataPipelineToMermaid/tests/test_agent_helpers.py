"""Unit tests for DataPipelineToMermaid.agent helpers (no LLM calls)."""

from __future__ import annotations

import json

import pytest

from DataPipelineToMermaid.agent import _extract_json_from_text


class TestExtractJsonFromText:
    """Test the JSON-extraction helper without needing an API key."""

    def test_raw_json(self):
        raw = '{"pipeline_name": "test", "pipeline_type": "sql_query"}'
        result = _extract_json_from_text(raw)
        data = json.loads(result)
        assert data["pipeline_name"] == "test"

    def test_markdown_fenced_json(self):
        text = 'Here is the result:\n```json\n{"a": 1}\n```\nDone.'
        result = _extract_json_from_text(text)
        data = json.loads(result)
        assert data["a"] == 1

    def test_markdown_fenced_no_lang(self):
        text = 'Result:\n```\n{"b": 2}\n```'
        result = _extract_json_from_text(text)
        data = json.loads(result)
        assert data["b"] == 2

    def test_json_embedded_in_text(self):
        text = 'Sure, here is the lineage:\n\n{"x": 42}\n\nEnd of response.'
        result = _extract_json_from_text(text)
        data = json.loads(result)
        assert data["x"] == 42

    def test_nested_json(self):
        obj = {"a": {"b": [1, 2, {"c": 3}]}}
        text = f"```json\n{json.dumps(obj)}\n```"
        result = _extract_json_from_text(text)
        data = json.loads(result)
        assert data["a"]["b"][2]["c"] == 3

    def test_raises_on_no_json(self):
        with pytest.raises(ValueError):
            _extract_json_from_text("no json here at all")

    def test_whitespace_padding(self):
        text = "   \n  {\"key\": \"val\"}  \n  "
        result = _extract_json_from_text(text)
        data = json.loads(result)
        assert data["key"] == "val"

    def test_large_fixture_in_markdown(self, sample_lineage_path):
        """Wrap the full fixture in markdown fences and extract."""
        fixture_text = sample_lineage_path.read_text()
        wrapped = f"Here is the lineage:\n```json\n{fixture_text}\n```\nEnd."
        result = _extract_json_from_text(wrapped)
        data = json.loads(result)
        assert data["pipeline_name"] == "Customer Analytics ETL"
        assert len(data["sources"]) == 4
