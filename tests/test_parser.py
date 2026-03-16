"""
tests/test_parser.py
====================
Unit tests for the Mermaid parser in mermaid_interactive.py.
"""
import sys
import os
import pytest

# Allow importing from the project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mermaid_interactive import parse_mermaid, generate_html


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def edges_set(edges):
    """Return a set of (src, dst) tuples for order-independent comparison."""
    return set(edges)


# ---------------------------------------------------------------------------
# Basic arrow types
# ---------------------------------------------------------------------------

class TestBasicArrows:
    def test_simple_arrow(self):
        nodes, edges = parse_mermaid("flowchart TD\n  A --> B")
        assert edges_set(edges) == {("A", "B")}
        assert "A" in nodes
        assert "B" in nodes

    def test_open_line(self):
        _, edges = parse_mermaid("flowchart TD\n  A --- B")
        assert edges_set(edges) == {("A", "B")}

    def test_dotted_arrow(self):
        _, edges = parse_mermaid("flowchart TD\n  A -.-> B")
        assert edges_set(edges) == {("A", "B")}

    def test_thick_arrow(self):
        _, edges = parse_mermaid("flowchart TD\n  A ==> B")
        assert edges_set(edges) == {("A", "B")}

    def test_bidirectional(self):
        _, edges = parse_mermaid("flowchart LR\n  A <--> B")
        assert edges_set(edges) == {("A", "B")}

    def test_pipe_label(self):
        _, edges = parse_mermaid("flowchart TD\n  A -->|yes| B")
        assert edges_set(edges) == {("A", "B")}

    def test_text_label_arrow(self):
        _, edges = parse_mermaid("flowchart TD\n  A -- yes --> B")
        assert edges_set(edges) == {("A", "B")}

    def test_thick_text_label(self):
        _, edges = parse_mermaid("flowchart TD\n  A == yes ==> B")
        assert edges_set(edges) == {("A", "B")}


# ---------------------------------------------------------------------------
# Node shapes / labels
# ---------------------------------------------------------------------------

class TestNodeShapes:
    def test_rectangle(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A[My Label] --> B")
        assert nodes["A"] == "My Label"

    def test_rounded_rect(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A(Rounded) --> B")
        assert nodes["A"] == "Rounded"

    def test_diamond(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A{Decision} --> B")
        assert nodes["A"] == "Decision"

    def test_circle(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A((Circle)) --> B")
        assert nodes["A"] == "Circle"

    def test_flag(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A>Flag] --> B")
        assert nodes["A"] == "Flag"

    def test_cylinder(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A[(Database)] --> B")
        assert nodes["A"] == "Database"

    def test_subroutine(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A[[Subroutine]] --> B")
        assert nodes["A"] == "Subroutine"

    def test_hexagon(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A{{Hex}} --> B")
        assert nodes["A"] == "Hex"

    def test_multi_word_label(self):
        nodes, _ = parse_mermaid(
            "flowchart TD\n  ReadMe[ReadMe Documentation] --> Guides[Guides]"
        )
        assert nodes["ReadMe"] == "ReadMe Documentation"
        assert nodes["Guides"] == "Guides"

    def test_bare_node_id_falls_back_to_id(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A --> B")
        assert nodes["A"] == "A"
        assert nodes["B"] == "B"


# ---------------------------------------------------------------------------
# Multi-hop edges
# ---------------------------------------------------------------------------

class TestMultiHop:
    def test_three_hop(self):
        _, edges = parse_mermaid("flowchart TD\n  A --> B --> C")
        assert edges_set(edges) == {("A", "B"), ("B", "C")}

    def test_four_hop(self):
        _, edges = parse_mermaid("flowchart TD\n  A --> B --> C --> D")
        assert edges_set(edges) == {("A", "B"), ("B", "C"), ("C", "D")}


# ---------------------------------------------------------------------------
# Directives that must be skipped
# ---------------------------------------------------------------------------

class TestSkippedDirectives:
    def test_style_line_ignored(self):
        nodes, edges = parse_mermaid(
            "flowchart TD\n  A --> B\n  style A fill:#f9f,stroke:#333"
        )
        assert edges_set(edges) == {("A", "B")}

    def test_classdef_ignored(self):
        nodes, edges = parse_mermaid(
            "flowchart TD\n  A --> B\n  classDef myClass fill:#f9f"
        )
        assert edges_set(edges) == {("A", "B")}

    def test_class_statement_ignored(self):
        nodes, edges = parse_mermaid(
            "flowchart TD\n  A --> B\n  class A myClass"
        )
        assert edges_set(edges) == {("A", "B")}

    def test_link_style_ignored(self):
        nodes, edges = parse_mermaid(
            "flowchart TD\n  A --> B\n  linkStyle 0 stroke:#ff3"
        )
        assert edges_set(edges) == {("A", "B")}

    def test_comment_ignored(self):
        nodes, edges = parse_mermaid(
            "flowchart TD\n  %% this is a comment\n  A --> B"
        )
        assert edges_set(edges) == {("A", "B")}

    def test_click_ignored(self):
        nodes, edges = parse_mermaid(
            "flowchart TD\n  A --> B\n  click A callback"
        )
        assert edges_set(edges) == {("A", "B")}


# ---------------------------------------------------------------------------
# Subgraphs
# ---------------------------------------------------------------------------

class TestSubgraphs:
    def test_edges_inside_subgraph_parsed(self):
        diagram = (
            "flowchart TD\n"
            "  A --> B\n"
            "  subgraph Group1\n"
            "    B --> C\n"
            "  end\n"
        )
        _, edges = parse_mermaid(diagram)
        assert ("A", "B") in edges
        assert ("B", "C") in edges


# ---------------------------------------------------------------------------
# Full example from the problem statement
# ---------------------------------------------------------------------------

class TestProblemStatementExample:
    DIAGRAM = """
flowchart TD
    ReadMe[ReadMe Documentation] --> Guides[Guides]
    ReadMe --> APIRef[API Reference]

    Guides --> Editor[Editor UI]
    Editor --> Slash[Slash Commands]
    Slash --> Mermaid[Mermaid Diagrams]
    Slash --> Other[Other Blocks]

    APIRef --> OpenAPI[OpenAPI Spec]
    APIRef --> Manual[Manual Editor]

    style ReadMe fill:#f9f,stroke:#333,stroke-width:4px
    style Mermaid fill:#bbf,stroke:#333,stroke-width:2px
"""

    def test_edges_complete(self):
        _, edges = parse_mermaid(self.DIAGRAM)
        expected = {
            ("ReadMe", "Guides"),
            ("ReadMe", "APIRef"),
            ("Guides", "Editor"),
            ("Editor", "Slash"),
            ("Slash", "Mermaid"),
            ("Slash", "Other"),
            ("APIRef", "OpenAPI"),
            ("APIRef", "Manual"),
        }
        assert edges_set(edges) == expected

    def test_labels_complete(self):
        nodes, _ = parse_mermaid(self.DIAGRAM)
        assert nodes["ReadMe"] == "ReadMe Documentation"
        assert nodes["Guides"] == "Guides"
        assert nodes["APIRef"] == "API Reference"
        assert nodes["Editor"] == "Editor UI"
        assert nodes["Slash"] == "Slash Commands"
        assert nodes["Mermaid"] == "Mermaid Diagrams"
        assert nodes["Other"] == "Other Blocks"
        assert nodes["OpenAPI"] == "OpenAPI Spec"
        assert nodes["Manual"] == "Manual Editor"

    def test_style_lines_do_not_create_nodes(self):
        nodes, _ = parse_mermaid(self.DIAGRAM)
        # 'style' and 'fill' etc. must not appear as node IDs
        for bad in ("style", "fill", "stroke"):
            assert bad not in nodes


# ---------------------------------------------------------------------------
# Graph JSON (children / parents)
# ---------------------------------------------------------------------------

class TestGraphJson:
    def test_children_and_parents_correct(self):
        import json
        html = generate_html("flowchart TD\n  A --> B\n  A --> C\n  B --> D")
        # extract GRAPH JSON from the generated HTML
        import re
        m = re.search(r"const GRAPH = (\{.*?\});", html, re.S)
        assert m, "GRAPH JSON not found in output"
        graph = json.loads(m.group(1))

        assert set(graph["children"]["A"]) == {"B", "C"}
        assert graph["children"]["B"] == ["D"]
        assert graph["parents"]["B"] == ["A"]
        assert graph["parents"]["D"] == ["B"]

    def test_isolated_node_has_no_edges(self):
        import json, re
        diagram = "flowchart TD\n  A --> B\n  C"
        html = generate_html(diagram)
        m = re.search(r"const GRAPH = (\{.*?\});", html, re.S)
        graph = json.loads(m.group(1))
        assert "C" in graph["nodes"]
        assert "C" not in graph["children"]
        assert "C" not in graph["parents"]


# ---------------------------------------------------------------------------
# HTML output sanity checks
# ---------------------------------------------------------------------------

class TestHtmlOutput:
    def test_contains_mermaid_script(self):
        html = generate_html("flowchart TD\n  A --> B")
        assert "mermaid" in html.lower()
        assert "cdn.jsdelivr.net" in html

    def test_contains_diagram_source(self):
        diagram = "flowchart TD\n  A --> B"
        html = generate_html(diagram)
        # The diagram text is HTML-escaped inside the page
        assert "flowchart TD" in html

    def test_contains_graph_data(self):
        html = generate_html("flowchart TD\n  A --> B")
        assert '"A"' in html
        assert '"B"' in html

    def test_is_valid_html(self):
        html = generate_html("flowchart TD\n  A --> B")
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html

    def test_xss_in_node_label(self):
        """Node labels with HTML special chars must not appear raw in output."""
        diagram = 'flowchart TD\n  A["<script>alert(1)</script>"] --> B'
        html = generate_html(diagram)
        # The literal unescaped string must not appear anywhere in the HTML
        assert "<script>alert(1)</script>" not in html
        # The Mermaid source block must HTML-escape angle brackets
        assert "&lt;script&gt;" in html or r"\u003cscript\u003e" in html


# ---------------------------------------------------------------------------
# graph LR alias
# ---------------------------------------------------------------------------

class TestGraphAlias:
    def test_graph_lr_parsed(self):
        _, edges = parse_mermaid("graph LR\n  X --> Y")
        assert edges_set(edges) == {("X", "Y")}
