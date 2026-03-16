# MermaidInteractive

Convert any [Mermaid](https://mermaid.js.org/) flowchart or graph into a **self-contained interactive HTML file** — no server required.

When you click a node in the rendered diagram, that node's **ancestors** (predecessors) and **descendants** are highlighted; every other node and its edges are dimmed. Clicking the same node again (or clicking the diagram background) resets the view.

---

## Features

- ✅ Pure Python — zero third-party dependencies  
- ✅ Single, self-contained HTML output (Mermaid.js loaded via CDN)  
- ✅ Supports `flowchart` and `graph` diagrams (TD, LR, RL, BT)  
- ✅ Handles all common arrow types: `-->`, `---`, `-.->`, `==>`, `<-->`, `~~>` and more  
- ✅ Handles labeled edges: `-->|label|` and `-- text -->`  
- ✅ Handles multi-hop chains: `A --> B --> C`  
- ✅ Node shapes: `[rect]`, `(rounded)`, `{diamond}`, `((circle))`, `[(cylinder)]`, `[[subroutine]]`, `{{hexagon}}`, `>flag]`  
- ✅ Safely ignores `style`, `classDef`, `class`, `linkStyle`, `click`, `subgraph` directives  
- ✅ HTML/script injection–safe output  

---

## Requirements

- Python 3.10 or newer  
- Internet access in the browser when opening the HTML file (to load Mermaid.js from CDN)

---

## Installation

No installation needed — just clone or download the repo:

```bash
git clone https://github.com/nitishkthakur/MermaidInteractive.git
cd MermaidInteractive
```

---

## Usage

### 1. From a `.mmd` file

```bash
python mermaid_interactive.py diagram.mmd
# → writes interactive_diagram.html (default)

python mermaid_interactive.py diagram.mmd -o my_diagram.html
# → writes my_diagram.html
```

### 2. From standard input (pipe)

```bash
cat diagram.mmd | python mermaid_interactive.py -o out.html

echo "flowchart TD
  A --> B
  B --> C" | python mermaid_interactive.py -o out.html
```

### 3. Inline text with `-t`

```bash
python mermaid_interactive.py -t "flowchart TD
  A --> B
  B --> C" -o out.html
```

### Open the result

```bash
# macOS
open interactive_diagram.html

# Linux
xdg-open interactive_diagram.html

# Windows
start interactive_diagram.html
```

---

## Example diagram

Save the following as `docs.mmd`:

```
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
```

Generate and open:

```bash
python mermaid_interactive.py docs.mmd -o docs.html
open docs.html
```

**Click any node** to see it highlighted (orange border) along with all its ancestors and descendants (blue border). All unrelated nodes are dimmed. Click again or click the background to reset.

More examples are in the [`examples/`](examples/) folder.

---

## How it works

1. **Parse** – `mermaid_interactive.py` reads the Mermaid source and extracts:
   - every node ID and its display label
   - every directed edge `(source → destination)`

2. **Build graph** – constructs `children` and `parents` adjacency maps and serialises them as JSON.

3. **Generate HTML** – produces a single `.html` file containing:
   - the original Mermaid source (HTML-escaped, inside a `<pre class="mermaid">`)
   - the adjacency JSON (embedded in a `<script>` block, with HTML special chars escaped as JSON Unicode escapes)
   - Mermaid.js v10 loaded from jsDelivr CDN
   - JavaScript that attaches click handlers after render and does BFS to find ancestors/descendants
   - CSS transitions for the highlight/dim effect

---

## Supported Mermaid syntax elements

| Syntax element | Parsed? | Notes |
|---|---|---|
| `flowchart TD/LR/RL/BT` | ✅ | Diagram type header is skipped; direction ignored for interaction |
| `graph TD/LR/…` | ✅ | Alias for `flowchart` |
| `A --> B` | ✅ | Normal arrow |
| `A --- B` | ✅ | Open line |
| `A -.-> B` | ✅ | Dotted arrow |
| `A ==> B` | ✅ | Thick arrow |
| `A <--> B` | ✅ | Bidirectional |
| `A -->|label| B` | ✅ | Pipe-labelled edge |
| `A -- text --> B` | ✅ | Text-labelled edge |
| `A --> B --> C` | ✅ | Multi-hop chain |
| `A[label]` | ✅ | Rectangle |
| `A(label)` | ✅ | Rounded rectangle |
| `A{label}` | ✅ | Diamond |
| `A((label))` | ✅ | Circle |
| `A>label]` | ✅ | Flag / ribbon |
| `A[(label)]` | ✅ | Cylinder / database |
| `A[[label]]` | ✅ | Subroutine |
| `A{{label}}` | ✅ | Hexagon |
| `style …` | ⏩ | Passed to Mermaid.js as-is; not parsed structurally |
| `classDef / class` | ⏩ | Passed to Mermaid.js as-is |
| `linkStyle` | ⏩ | Passed to Mermaid.js as-is |
| `subgraph … end` | ⏩ | Boundary skipped; internal edges still parsed |
| `click …` | ⏩ | Skipped (interaction is handled separately) |
| `%% comment` | ⏩ | Ignored |

> **Limitation:** sequence diagrams, class diagrams, Gantt charts, and other non-flowchart/graph Mermaid diagram types are rendered by Mermaid.js correctly but the interactive click-to-highlight feature only works for `flowchart`/`graph` diagrams.

---

## Running tests

```bash
pip install pytest
python -m pytest tests/ -v
```

---

## Command-line reference

```
usage: mermaid_interactive [-h] [-t DIAGRAM_TEXT] [-o OUTPUT_FILE] [INPUT_FILE]

positional arguments:
  INPUT_FILE       Path to a .mmd file. Omit to read from stdin.

options:
  -h, --help       show this help message and exit
  -t DIAGRAM_TEXT  Mermaid diagram text passed directly as a string.
  -o OUTPUT_FILE   Output HTML file path (default: interactive_diagram.html).
```
