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
        assert ("<script>" in html and "mermaid.initialize" in html) or "cdn.jsdelivr.net" in html

    def test_steelblue_theme_no_lavender(self):
        html = generate_html("flowchart TD\n  A --> B")
        assert "#d6e4f0" in html
        assert "#4682b4" in html
        assert "#ececff" not in html

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

    def test_no_header_title_or_hint(self):
        """The old decorative h1 / hint paragraph must not appear in the output."""
        html = generate_html("flowchart TD\n  A --> B")
        assert "<h1>" not in html
        assert "Interactive Mermaid Diagram" not in html
        assert "Click a node to highlight" not in html

    def test_download_button_present(self):
        """A Download PNG button must be present in the generated HTML."""
        html = generate_html("flowchart TD\n  A --> B")
        assert "download-btn" in html
        assert "downloadPNG" in html

    def test_professional_colors_no_orange_or_pink(self):
        """Old orange (#e65c00) and blue (#0077cc) colors must be gone;
        professional palette (steelblue, salmon) must be used instead."""
        html = generate_html("flowchart TD\n  A --> B")
        assert "#e65c00" not in html
        assert "#0077cc" not in html
        assert "steelblue" in html
        assert "salmon" in html

    def test_container_fills_viewport(self):
        """The diagram container should use min-height to fill the viewport."""
        html = generate_html("flowchart TD\n  A --> B")
        assert "min-height" in html
        assert "100vh" in html

    def test_subgraph_source_preserved_for_mermaid(self):
        """Subgraph syntax in the source must be present in the HTML output
        so that Mermaid.js renders the subgraph visually."""
        diagram = (
            "flowchart TD\n"
            "  A --> B\n"
            "  subgraph Group1\n"
            "    B --> C\n"
            "  end\n"
        )
        html = generate_html(diagram)
        assert "subgraph" in html
        assert "Group1" in html


# ---------------------------------------------------------------------------
# Subgraph ID registration
# ---------------------------------------------------------------------------

class TestSubgraphIdRegistration:
    def test_subgraph_id_registered_as_node(self):
        """A named subgraph ID should be available in the nodes dict so that
        edges referencing it (A --> subgraphID) are resolved correctly."""
        diagram = (
            "flowchart TD\n"
            "  subgraph sg1\n"
            "    A --> B\n"
            "  end\n"
            "  C --> sg1\n"
        )
        nodes, edges = parse_mermaid(diagram)
        assert "sg1" in nodes
        assert ("C", "sg1") in edges

    def test_subgraph_label_not_used_as_id(self):
        """A subgraph with label syntax `subgraph id[Label]` should register
        the id only, not the bracketed label."""
        diagram = (
            "flowchart TD\n"
            "  subgraph myGroup[My Group Label]\n"
            "    X --> Y\n"
            "  end\n"
        )
        nodes, _ = parse_mermaid(diagram)
        assert "myGroup" in nodes


# ---------------------------------------------------------------------------
# graph LR alias
# ---------------------------------------------------------------------------

class TestGraphAlias:
    def test_graph_lr_parsed(self):
        _, edges = parse_mermaid("graph LR\n  X --> Y")
        assert edges_set(edges) == {("X", "Y")}


# ---------------------------------------------------------------------------
# Non-flowchart diagram types (should return empty graph)
# ---------------------------------------------------------------------------

class TestNonFlowchartDiagrams:
    def test_sequence_diagram_returns_empty(self):
        nodes, edges = parse_mermaid("sequenceDiagram\n  Alice->>Bob: Hello")
        assert nodes == {}
        assert edges == []

    def test_class_diagram_returns_empty(self):
        nodes, edges = parse_mermaid("classDiagram\n  Animal <|-- Duck")
        assert nodes == {}
        assert edges == []

    def test_state_diagram_returns_empty(self):
        nodes, edges = parse_mermaid("stateDiagram-v2\n  s1 --> s2")
        assert nodes == {}
        assert edges == []

    def test_er_diagram_returns_empty(self):
        nodes, edges = parse_mermaid("erDiagram\n  CUSTOMER ||--o{ ORDER : places")
        assert nodes == {}
        assert edges == []

    def test_gantt_returns_empty(self):
        nodes, edges = parse_mermaid("gantt\n  title A Gantt Diagram")
        assert nodes == {}
        assert edges == []

    def test_pie_returns_empty(self):
        nodes, edges = parse_mermaid('pie\n  title Pets\n  "Dogs" : 386')
        assert nodes == {}
        assert edges == []


# ---------------------------------------------------------------------------
# YAML front-matter
# ---------------------------------------------------------------------------

class TestYamlFrontMatter:
    def test_yaml_frontmatter_skipped(self):
        diagram = (
            "---\n"
            "title: My Diagram\n"
            "config:\n"
            "  theme: default\n"
            "---\n"
            "flowchart TD\n"
            "  A --> B\n"
        )
        nodes, edges = parse_mermaid(diagram)
        assert edges_set(edges) == {("A", "B")}
        assert "title" not in nodes
        assert "config" not in nodes

    def test_no_frontmatter_still_works(self):
        nodes, edges = parse_mermaid("flowchart TD\n  X --> Y")
        assert edges_set(edges) == {("X", "Y")}


# ---------------------------------------------------------------------------
# Quoted node labels
# ---------------------------------------------------------------------------

class TestQuotedNodeLabels:
    def test_double_quoted_label(self):
        nodes, _ = parse_mermaid('flowchart TD\n  A["My Label"] --> B')
        assert nodes["A"] == "My Label"

    def test_single_quoted_label(self):
        nodes, _ = parse_mermaid("flowchart TD\n  A['My Label'] --> B")
        assert nodes["A"] == "My Label"

    def test_double_quoted_label_with_brackets(self):
        nodes, _ = parse_mermaid('flowchart TD\n  A["text with [brackets]"] --> B')
        assert nodes["A"] == "text with [brackets]"


# ---------------------------------------------------------------------------
# Large-scale graphs
# ---------------------------------------------------------------------------

class TestLargeGraphs:
    @staticmethod
    def _make_chain(n: int) -> str:
        """Linear chain: node_0 --> node_1 --> ... --> node_{n-1}."""
        header = "flowchart TD\n"
        edges = "\n".join(f"  n{i} --> n{i+1}" for i in range(n - 1))
        return header + edges

    @staticmethod
    def _make_star(n: int) -> str:
        """Star: center --> leaf_0, center --> leaf_1, ..."""
        header = "flowchart TD\n"
        edges = "\n".join(f"  center --> leaf{i}" for i in range(n))
        return header + edges

    @staticmethod
    def _make_binary_tree(depth: int) -> str:
        """Binary tree via multi-hop chains."""
        header = "flowchart TD\n"
        lines = []
        node_id = [0]

        def add_children(parent: int, d: int):
            if d == 0:
                return
            left, right = node_id[0] + 1, node_id[0] + 2
            node_id[0] += 2
            lines.append(f"  n{parent} --> n{left}")
            lines.append(f"  n{parent} --> n{right}")
            add_children(left, d - 1)
            add_children(right, d - 1)

        add_children(0, depth)
        return header + "\n".join(lines)

    def test_linear_chain_100(self):
        diagram = self._make_chain(100)
        nodes, edges = parse_mermaid(diagram)
        assert len(nodes) == 100
        assert len(edges) == 99
        assert ("n0", "n1") in edges
        assert ("n98", "n99") in edges

    def test_linear_chain_500(self):
        diagram = self._make_chain(500)
        nodes, edges = parse_mermaid(diagram)
        assert len(nodes) == 500
        assert len(edges) == 499

    def test_star_graph_200_leaves(self):
        diagram = self._make_star(200)
        nodes, edges = parse_mermaid(diagram)
        assert len(nodes) == 201        # center + 200 leaves
        assert len(edges) == 200
        assert all(dst.startswith("leaf") for _, dst in edges)

    def test_binary_tree_depth_7(self):
        # depth 7 → 127 edges, 128 leaves, 255 total nodes (with root = 256 nodes)
        diagram = self._make_binary_tree(7)
        nodes, edges = parse_mermaid(diagram)
        assert len(nodes) > 100
        assert len(edges) > 100

    def test_large_graph_html_is_valid(self):
        diagram = self._make_chain(300)
        html = generate_html(diagram)
        assert html.startswith("<!DOCTYPE html>")
        assert "</html>" in html
        assert '"n0"' in html
        assert '"n299"' in html

    def test_large_graph_json_correct(self):
        import json, re
        diagram = self._make_star(150)
        html = generate_html(diagram)
        m = re.search(r"const GRAPH = (\{.*?\});", html, re.S)
        assert m
        graph = json.loads(m.group(1))
        assert len(graph["children"]["center"]) == 150
        assert len(graph["parents"]) == 150   # each leaf has center as parent
