# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this repository.

## Commands

```bash
# Run the tool
python mermaid_interactive.py diagram.mmd -o output.html
python mermaid_interactive.py -t "flowchart TD\n  A --> B" -o out.html
cat diagram.mmd | python mermaid_interactive.py -o out.html

# Run all tests
python -m pytest tests/ -v

# Run a single test class or method
python -m pytest tests/test_parser.py::TestBasicArrows -v
python -m pytest tests/test_parser.py::TestBasicArrows::test_simple_arrow -v

# With coverage
pytest tests/ --cov=mermaid_interactive
```

No installation required — zero third-party runtime dependencies (pytest needed only for tests).

## Architecture

Everything lives in a single module: `mermaid_interactive.py`.

**Data flow:** `parse_mermaid(text)` → `_build_graph_json(nodes, edges)` → `generate_html(mermaid_text)` → write `.html`

### Parser (`parse_mermaid` → lines 30–306)

Regex-based, line-by-line. Returns `(nodes: dict[str, str], edges: list[tuple[str, str]])`.

- `_NID`, `_SHAPE`, `_ARROW`, `_EDGE_ANY` are the core regex building blocks (lines 41–92)
- `_tokenize_line()` tokenizes a single line into `(kind, value)` tuples
- `_expand_edge_line()` handles multi-hop (`A --> B --> C`) and `&` grouping (`A & B --> C`)
- `_extract_node_defs()` registers nodes with their display labels
- Permissive: unknown/styling directives are silently skipped via `_SKIP_RE`
- `subgraph` IDs are registered as nodes so cross-subgraph edges resolve correctly

### HTML Generator (`generate_html` → lines 671–689)

Populates `_HTML_TEMPLATE` (a large string constant, lines 313–643) with:
- HTML-escaped Mermaid source (rendered by Mermaid.js v10 from jsDelivr CDN)
- JSON graph data with `children` and `parents` adjacency maps (HTML-special-char–escaped for safe `<script>` embedding)

The embedded JavaScript does BFS traversal (`getRelated`) and maps Mermaid SVG element IDs back to logical node IDs via `svgIdToNodeId` (pattern: `flowchart-{nodeId}-{n}`) and `svgEdgeToNodeIds` (pattern: `L-{src}-{dst}-{n}`).

### UI conventions
- Colors: `salmon` for selected node, `steelblue` for ancestors/descendants, `opacity: .15` for dimmed
- Toolbar: top-right, minimalist — Download PNG + Reset view buttons only (no decorative title or hint text)
- Canvas: `min-height: calc(100vh - 6rem)` to fill viewport

## Adding new syntax

- **New arrow type**: extend `_ARROW` regex (line ~60), add test in `TestBasicArrows`
- **New node shape**: extend `_SHAPE` regex (line ~44) and `_node_label()` patterns (line ~128), add test in `TestNodeShapes`
- **New skipped directive**: add to `_SKIP_RE` (line 236)
- **Highlighting logic**: modify `applyHighlight` / `getRelated` in `_HTML_TEMPLATE`

## Known constraints

- Output HTML requires internet access (Mermaid.js loaded from CDN)
- Interactive click-to-highlight only works for `flowchart`/`graph` diagram types
- `<-->` is treated as a single undirected edge, not two directed edges
- Python 3.10+ required (uses `list[...]` and `tuple[...]` type hints in function signatures)
