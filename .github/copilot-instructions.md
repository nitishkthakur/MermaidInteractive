# MermaidInteractive — Copilot Instructions

## Project Overview

**MermaidInteractive** converts Mermaid diagram syntax (flowcharts, graphs) into self-contained, interactive HTML files with **zero dependencies**. When a user clicks a node in the diagram, all ancestors (predecessors) and descendants are highlighted; everything else dims. Clicking again or the background resets the view.

### Key Characteristics
- **Language**: Pure Python 3.10+ (no third-party dependencies)
- **Scope**: Single-module parser + HTML generator  
- **Input**: `.mmd` files, stdin, or inline text via CLI  
- **Output**: Self-contained HTML (Mermaid.js loaded via CDN + custom JavaScript)
- **Current Status**: Functional; examples in `examples/` folder demonstrate flowcharts and graphs

---

## Architecture & Core Concepts

### Main Components

1. **Parser** (`mermaid_interactive.py` — lines 30–150+)
   - Regex-based pattern matching for Mermaid syntax
   - Extracts nodes (with shapes: `[rect]`, `(rounded)`, `{diamond}`, `((circle))`, etc.)
   - Extracts edges (arrow types: `-->`, `---`, `-.->`, `==>`, `<-->`, `~~>`, etc.)
   - Handles labeled edges (pipe syntax: `-->|label|` and text syntax: `-- text -->`)
   - Handles multi-hop chains (e.g., `A --> B --> C`)
   - **Safety**: Safely ignores `style`, `classDef`, `class`, `linkStyle`, `click`
   - **Subgraphs**: Registers subgraph IDs as potential nodes; processes all edges inside subgraphs
   - Returns: `(nodes_dict, edges_list)` tuple

2. **HTML Generator** (`generate_html()`)
   - Wraps parsed nodes and edges into a self-contained HTML file
   - Embeds custom JavaScript for click-based highlighting logic
   - Loads Mermaid.js from CDN; no server required
   - **UI**: Minimalist, professional layout — no decorative title or hint text
   - **Toolbar**: Top-right toolbar with "Download PNG" and "Reset view" buttons
   - **Colors**: Professional palette — `steelblue` for related nodes, `salmon` for selected
   - **Canvas**: Large container (`min-height: calc(100vh - 6rem)`) fills the viewport
   - Output is ready to open in any modern browser

3. **CLI Interface** (`main()`)
   - Argparse-based command-line tool
   - Supports three input modes:
     - From `.mmd` file: `python mermaid_interactive.py diagram.mmd`
     - From stdin: `cat diagram.mmd | python mermaid_interactive.py -o out.html`
     - Inline text: `python mermaid_interactive.py -t "flowchart TD\n  A --> B" -o out.html`
   - Default output: `interactive_diagram.html`

### Supported Diagram Types
- `flowchart` (directions: TD, LR, RL, BT)
- `graph` (directions: TD, LR, RL, BT)

### Regex Patterns (Key References)
- `_NID`: Node identifiers (alphanumeric + underscore/hyphen)
- `_SHAPE`: Node shape syntax (brackets, parentheses, braces, etc.)
- `_ARROW`: Edge types (normal, dotted, thick, bidirectional, wavy)
- `_EDGE_PIPE`, `_EDGE_TEXT_FW`, `_EDGE_TEXT_OP`: Edge label patterns

---

## Development Workflow

### Running the Tool

```bash
# From a file
python mermaid_interactive.py diagram.mmd -o output.html

# From stdin
cat diagram.mmd | python mermaid_interactive.py -o output.html

# Inline
python mermaid_interactive.py -t "flowchart TD\n  A --> B" -o output.html
```

### Running Tests

```bash
pytest tests/test_parser.py -v

# Or with coverage
pytest tests/ --cov=mermaid_interactive
```

### Testing Patterns
- Test structure: `tests/test_parser.py` uses pytest classes (e.g., `TestBasicArrows`)
- Helper: `edges_set()` normalizes edges for order-independent comparison
- Each test calls `parse_mermaid()` directly and asserts on returned `(nodes, edges)`

---

## Code Patterns & Conventions

### Parser Functions
- **`parse_mermaid(diagram_text) → (nodes_dict, edges_list)`**
  - Input: Raw Mermaid diagram text (string)
  - Output: `(dict, list)` where dict keys are node IDs, list is `[(src, dst), ...]`
  - Handles multiline input; strips/normalizes whitespace
  - Returns empty structures if no diagram is detected

### HTML Generation
- **`generate_html(diagram_text, nodes, edges) → str`**
  - Input: Original diagram text, parsed nodes/edges
  - Output: Complete HTML document (string) ready to write to `.html`
  - Includes inline JavaScript for click event handling and ancestor/descendant traversal

### Error Handling
- Parser is **permissive**: Ignores unsupported directives rather than raising errors
- CLI validates input file existence before parsing
- HTML escaping used throughout for security (no XSS attacks)

### Code Style
- Module docstrings at top (27 lines)
- Function docstrings present
- Comments mark major sections (e.g., "# Mermaid structural parser")
- Variable naming: descriptive (`_NID`, `_SHAPE`, `_ARROW` for regex patterns)
- No type hints in main file (Python 3.10+ supports them; consider adding if expanding)

---

## Common Tasks & Quick References

### Add a New Arrow Type
1. Locate `_ARROW` pattern in `mermaid_interactive.py` (line ~45)
2. Add the new arrow syntax to the regex (use alternation `|`)
3. Add a test in `tests/test_parser.py` under `TestBasicArrows` or new class

### Add a New Node Shape
1. Update `_SHAPE` pattern in `mermaid_interactive.py`
2. Test with existing parser (shapes are extracted by the same regex)
3. Add test case to `TestNodeShapes` class

### Improve Highlighting Logic
1. Modify the JavaScript in `generate_html()` function (search for `function applyHighlight`)
2. Test in browser: open generated `.html` and click nodes
3. Consider adding tests for graph traversal if logic becomes complex

### Modify the Download PNG Feature
1. Locate `function downloadPNG()` inside `_HTML_TEMPLATE` in `mermaid_interactive.py`
2. Adjust the Canvas scale or background color as needed
3. The function uses native browser APIs (no extra dependencies)

### Handle a New Directive (e.g., custom styling)
- Current approach: Ignore unknown directives (regex lines skip them)
- Option 1: Preserve directives in parsed output (extend return tuple)
- Option 2: Add specific handling if directive affects graph structure

---

## Known Constraints & Gotchas

1. **Mermaid.js Dependency**: Output HTML requires internet access to load Mermaid.js from CDN
2. **Subgraph Support**: `subgraph` directives are rendered by Mermaid.js natively; subgraph IDs are registered as nodes in the graph data for interactive highlighting (e.g., `A --> subgraphID` edges work)
3. **No Styling Preservation**: Style directives (`style`, `classDef`, etc.) are intentionally stripped
4. **Python 3.10+ Only**: Uses match/case or modern string features (verify exact version requirement)
5. **Single-Direction Edges**: Parser treats `<-->` as a single undirected edge (not two directed edges)

---

## File Layout

```
MermaidInteractive/
├── mermaid_interactive.py    # Main module (parser + HTML gen + CLI)
├── test_1.mmd                # Example diagram for manual testing
├── tests/
│   ├── __init__.py
│   └── test_parser.py        # Unit tests (pytest)
├── examples/
│   ├── app_navigation.mmd
│   ├── app_navigation.html
│   ├── ci_pipeline.mmd
│   ├── ci_pipeline.html
│   ├── docs_tree.mmd
│   └── docs_tree.html
├── README.md
├── LICENSE
└── .github/
    └── copilot-instructions.md (this file)
```

---

## Next Steps for AI Agents

When working on this project, prioritize in this order:
1. **Run tests first**: `pytest tests/ -v` to ensure baseline functionality
2. **Use examples**: Reference diagrams in `examples/` to understand supported syntax
3. **Test manually**: Generate an HTML and open in browser to verify click behavior
4. **Read the parser**: The regex logic in lines 30–150 is the project's core
5. **Check edge cases**: Labeled edges, multi-hop chains, mixed arrow types, and subgraph references

---

## Resources

- **Mermaid.js Documentation**: https://mermaid.js.org/
- **Supported Diagram Types**: https://mermaid.js.org/ecosystem/integrations.html
- **Project README**: `README.md` (includes usage examples and feature list)
