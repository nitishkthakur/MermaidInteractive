#!/usr/bin/env python3
"""
mermaid_interactive.py
======================
Convert Mermaid diagram syntax into a self-contained interactive HTML file.

When a node is clicked the diagram highlights that node together with all of
its ancestors (predecessors) and all of its descendants.  Every other node
(and its connecting edges) is dimmed.  Clicking the same node again, or
clicking the diagram background, resets the view.

Usage
-----
    # Read from a file
    python mermaid_interactive.py diagram.mmd

    # Specify output file (default: interactive_diagram.html)
    python mermaid_interactive.py diagram.mmd -o my_diagram.html

    # Read from stdin
    cat diagram.mmd | python mermaid_interactive.py -o out.html
    echo "flowchart TD\n  A --> B" | python mermaid_interactive.py

    # Pass diagram text inline
    python mermaid_interactive.py -t "flowchart TD
        A --> B
        B --> C" -o out.html
"""

import sys
import re
import json
import html as html_module
import argparse
import os
import urllib.request

# ---------------------------------------------------------------------------
# Mermaid structural parser
# ---------------------------------------------------------------------------

_NID = r"[A-Za-z_][A-Za-z0-9_-]*"

# Optional shape suffix attached to a node token
_SHAPE = (
    r"(?:"
    r"\[\[[^\[\]]*\]\]"        # [[label]]  subroutine
    r"|\[\([^()]*\)\]"         # [(label)]  cylinder / database
    r"|\(\([^()]*\)\)"         # ((label))  circle
    r'|\["[^"]*"\]'             # ["label"]  double-quoted rectangle
    r"|\['[^']*'\]"             # ['label']  single-quoted rectangle
    r"|\[/?[^\[\]]*[/\\]?\]"   # [label] [/label/] [\label\] …
    r"|\([^()]*\)"             # (label)    rounded rect
    r"|\{\{[^{}]*\}\}"         # {{label}}  hexagon
    r"|\{[^{}]*\}"             # {label}    diamond
    r"|>[^\]]*\]"              # >label]    flag / ribbon
    r")?"
)

_NODE_TOK = r"(" + _NID + _SHAPE + r")"

# Arrow / edge-type patterns (most specific first)
_ARROW = (
    r"(?:"
    r"<--+>"          # <-->  bidirectional
    r"|--+>"          # -->   normal
    r"|-\.+->"        # -.->  dotted arrow
    r"|-\.-"          # -.-   dotted line
    r"|=+>"           # ==>   thick arrow
    r"|={2,}"         # ===   thick open line
    r"|---+"          # ---   open line
    r"|~~>"           # ~~>   wavy
    r"|o--o|x--x"
    r"|<--o|o-->|<--x|x-->"
    r")"
)

# edge -->|label|
_EDGE_PIPE = r"(?:" + _ARROW + r"\s*\|[^|]*\|)"
# edge -- text --> or -- text ---
_EDGE_TEXT_FW = r"(?:--[^-|&>\n]+--+>)"
_EDGE_TEXT_OP = r"(?:--[^-|&>\n]+---+)"
# edge == text ==>
_EDGE_TEXT_TH = r"(?:==[^=|&>\n]+==+>?)"
_EDGE_SIMPLE = _ARROW

_EDGE_ANY = (
    r"(?:"
    + _EDGE_PIPE
    + r"|" + _EDGE_TEXT_FW
    + r"|" + _EDGE_TEXT_OP
    + r"|" + _EDGE_TEXT_TH
    + r"|" + _EDGE_SIMPLE
    + r")"
)

# Compiled patterns
_EDGE_RE = re.compile(
    r"^\s*" + _NODE_TOK + r"\s*" + _EDGE_ANY + r"\s*" + _NODE_TOK + r"\s*$"
)
_NODE_ONLY_RE = re.compile(r"^\s*" + _NODE_TOK + r"\s*$")

# Detects any arrow/edge marker in a line (used for multi-hop detection)
_HAS_ARROW_RE = re.compile(
    r"--+>|<--+>|-\.+->|=+>|---+|~~>|o--o|x--x|<--o|o-->|<--x|x-->"
)

# Tokeniser used for multi-hop / & expansion
_TOKEN_RE = re.compile(
    # pipe-labelled edge
    r"(?P<edge_pipe>" + _ARROW + r"\s*\|[^|]*\|)"
    # text-labelled edge
    r"|(?P<edge_text>--[^-|&>\n]+--+>|--[^-|&>\n]+---+|==[^=|&>\n]+==+>)"
    # plain arrow
    r"|(?P<edge_simple>" + _ARROW + r")"
    # & separator between sibling nodes
    r"|(?P<amp>&)"
    # node token
    r"|(?P<node>" + _NID + _SHAPE + r")"
)


def _node_id(token: str) -> str:
    """Return the bare node ID from a token such as 'ReadMe[label]'."""
    m = re.match(r"^(" + _NID + r")", token.strip())
    return m.group(1) if m else ""


def _node_label(token: str) -> str:
    """Return the visible display label from a node token."""
    for pattern in [
        r"\[\[([^\[\]]*)\]\]",             # [[label]]
        r"\[\(([^()]*)\)\]",               # [(label)]
        r"\(\(([^()]*)\)\)",               # ((label))
        r"\[/([^\[\]/\\]*)[/\\]?\]",       # [/label/]
        r"\[\\([^\[\]/\\]*)/?]",           # [\label\]
        r'\["([^"]*)"\]',                  # ["label"]
        r"\['([^']*)'\]",                  # ['label']
        r"\[([^\[\]]*)\]",                 # [label]
        r"\(([^()]*)\)",                   # (label)
        r"\{\{([^{}]*)\}\}",              # {{label}}
        r"\{([^{}]*)\}",                   # {label}
        r">([^\]]*)\]",                    # >label]
    ]:
        m = re.search(pattern, token)
        if m:
            return m.group(1).strip()
    return _node_id(token)


def _tokenize_line(line: str) -> list[tuple[str, str]] | None:
    """
    Tokenize a single Mermaid edge/node line into (kind, value) tuples.

    Kinds: 'node', 'edge_pipe', 'edge_text', 'edge_simple', 'amp'.
    Returns None if any part of the line cannot be tokenised.
    """
    tokens: list[tuple[str, str]] = []
    pos = 0
    line = line.strip()
    while pos < len(line):
        # skip whitespace
        ws = re.match(r"\s+", line[pos:])
        if ws:
            pos += ws.end()
            continue
        m = _TOKEN_RE.match(line, pos)
        if not m:
            return None
        kind = m.lastgroup
        tokens.append((kind, m.group()))
        pos = m.end()
    return tokens if tokens else None


def _expand_edge_line(line: str) -> list[tuple[str, str]]:
    """
    Expand one Mermaid edge line into a list of (src_id, dst_id) pairs.

    Handles:
      • Basic:       A --> B
      • Labeled:     A -->|text| B   /  A -- text --> B
      • Multi-hop:   A --> B --> C
      • & grouping:  A & B --> C   /  A --> B & C
    """
    tokens = _tokenize_line(line)
    if not tokens:
        return []

    # Check that we have at least one edge token
    has_edge = any(k in ("edge_pipe", "edge_text", "edge_simple") for k, _ in tokens)
    if not has_edge:
        return []

    # Split token stream into groups separated by edge tokens.
    # Each group is a list of node/amp tokens; groups[i] --> groups[i+1].
    groups: list[list[str]] = []
    current_group: list[str] = []

    for kind, val in tokens:
        if kind in ("edge_pipe", "edge_text", "edge_simple"):
            groups.append(current_group)
            current_group = []
        elif kind == "node":
            current_group.append(val)
        elif kind == "amp":
            pass  # node tokens in same group are already collected
    groups.append(current_group)

    # Expand & within groups: 'A & B' both appear as consecutive 'node' tokens
    # because _tokenize_line skips 'amp' as a separator
    # Re-tokenise to capture & properly
    def group_node_ids(group: list[str]) -> list[str]:
        return [_node_id(tok) for tok in group if _node_id(tok)]

    edges: list[tuple[str, str]] = []
    for i in range(len(groups) - 1):
        srcs = group_node_ids(groups[i])
        dsts = group_node_ids(groups[i + 1])
        for src in srcs:
            for dst in dsts:
                if src and dst:
                    edges.append((src, dst))

    return edges


def _extract_node_defs(line: str, nodes: dict[str, str]) -> None:
    """Register any node tokens with their labels from a tokenized line."""
    tokens = _tokenize_line(line)
    if not tokens:
        return
    for kind, val in tokens:
        if kind == "node":
            nid = _node_id(val)
            if nid:
                nodes.setdefault(nid, _node_label(val))


# Directives that carry no structural information
_SKIP_RE = re.compile(
    r"^(style|classDef|class|linkStyle|click|accTitle|accDescr|direction)\b",
    re.I,
)

# Diagram types that are not flowcharts — return empty graph immediately
_NON_FLOWCHART_RE = re.compile(
    r"^(?:sequenceDiagram|classDiagram|stateDiagram(?:-v2)?|erDiagram"
    r"|gitGraph|gantt|pie|journey|quadrantChart|xychart-beta"
    r"|block-beta|mindmap|timeline)\b",
    re.I,
)


def parse_mermaid(text: str) -> tuple[dict[str, str], list[tuple[str, str]]]:
    """
    Parse Mermaid flowchart / graph syntax.

    Parameters
    ----------
    text : str
        Raw Mermaid diagram source.

    Returns
    -------
    nodes : dict[str, str]
        ``{node_id: display_label}``
    edges : list[tuple[str, str]]
        Directed edges as ``(source_id, destination_id)`` pairs.
    """
    nodes: dict[str, str] = {}
    edges: list[tuple[str, str]] = []

    diagram_type_seen = False
    in_frontmatter = False

    for raw in text.splitlines():
        line = raw.strip()

        # blank lines and comments
        if not line or line.startswith("%%"):
            continue

        # YAML front-matter (--- ... ---)
        if not diagram_type_seen and line == "---":
            in_frontmatter = not in_frontmatter
            continue
        if in_frontmatter:
            continue

        if line in ("...",) or line.startswith("%%{"):
            continue

        # non-flowchart early exit on first real line
        if not diagram_type_seen:
            if _NON_FLOWCHART_RE.match(line):
                return {}, []
            diagram_type_seen = True

        # diagram-type header: 'flowchart TD', 'graph LR', …
        if re.match(r"^(flowchart|graph)\b", line, re.I):
            # Only skip if the line has no edge content
            if not _HAS_ARROW_RE.search(line):
                continue

        # non-structural directives
        if _SKIP_RE.match(line):
            continue

        # subgraph boundaries — skip structural keywords but register the
        # subgraph ID as a potential node so that edges like `A --> myGroup`
        # are resolved correctly when a subgraph is referenced by ID.
        sg_m = re.match(r"^subgraph\s+([A-Za-z_][A-Za-z0-9_-]*)", line, re.I)
        if sg_m:
            nodes.setdefault(sg_m.group(1), sg_m.group(1))
            continue
        if line.lower() == "end":
            continue

        # Does this line contain an arrow? → try as edge line
        if _HAS_ARROW_RE.search(line):
            new_edges = _expand_edge_line(line)
            if new_edges:
                _extract_node_defs(line, nodes)
                for src, dst in new_edges:
                    nodes.setdefault(src, src)
                    nodes.setdefault(dst, dst)
                    edges.append((src, dst))
                continue

        # Otherwise treat as a standalone node declaration
        _extract_node_defs(line, nodes)

    return nodes, edges


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Mermaid Diagram</title>
  <style>
    /* ── layout ──────────────────────────────────────────────────────── */
    *, *::before, *::after { box-sizing: border-box; }

    html, body {
      margin: 0;
      padding: 0;
      height: 100%;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto,
                   Arial, sans-serif;
      background: #f8f9fa;
    }

    body {
      display: flex;
      flex-direction: column;
      min-height: 100vh;
    }

    /* ── toolbar ──────────────────────────────────────────────────────── */
    #toolbar {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: .5rem;
      padding: .55rem 1.25rem;
      background: #fff;
      border-bottom: 1px solid #e8e8e8;
      box-shadow: 0 1px 4px rgba(0,0,0,.06);
    }

    .tb-btn {
      display: inline-flex;
      align-items: center;
      gap: .3rem;
      padding: .42rem 1rem;
      font-size: .78rem;
      font-weight: 600;
      border: 1.5px solid transparent;
      border-radius: 6px;
      cursor: pointer;
      letter-spacing: .03em;
      line-height: 1;
      white-space: nowrap;
      transition: background .14s, border-color .14s, box-shadow .14s,
                  color .14s, transform .08s;
    }
    .tb-btn:active { transform: scale(.95); }

    #download-btn {
      background: steelblue;
      color: #fff;
      border-color: steelblue;
      box-shadow: 0 1px 3px rgba(70,130,180,.4);
    }
    #download-btn:hover {
      background: #3a72a0;
      border-color: #3a72a0;
      box-shadow: 0 2px 7px rgba(70,130,180,.45);
    }

    #reset-btn {
      background: #fff;
      color: #555;
      border-color: #d4d4d4;
    }
    #reset-btn:hover {
      background: #f0f5fb;
      border-color: steelblue;
      color: steelblue;
    }

    /* ── diagram area ─────────────────────────────────────────────────── */
    #diagram-container {
      flex: 1;
      background: #fff;
      margin: 1.25rem;
      border-radius: 8px;
      box-shadow: 0 1px 8px rgba(0,0,0,.08);
      min-height: calc(100vh - 6rem);
      overflow: hidden;
      position: relative;
      cursor: grab;
      user-select: none;
    }
    #diagram-container.panning { cursor: grabbing; }

    /* ── node highlight states ───────────────────────────────────────── */
    .node { transition: opacity .22s, filter .22s; cursor: pointer; }

    /* selected node — salmon outline */
    .node.mi-selected > rect,
    .node.mi-selected > circle,
    .node.mi-selected > ellipse,
    .node.mi-selected > polygon,
    .node.mi-selected > path {
      stroke: salmon !important;
      stroke-width: 3.5px !important;
      filter: drop-shadow(0 0 6px rgba(250,128,114,.6));
    }

    /* ancestor / descendant — steelblue outline */
    .node.mi-related > rect,
    .node.mi-related > circle,
    .node.mi-related > ellipse,
    .node.mi-related > polygon,
    .node.mi-related > path {
      stroke: steelblue !important;
      stroke-width: 2.5px !important;
      filter: drop-shadow(0 0 4px rgba(70,130,180,.5));
    }

    /* unrelated nodes — heavily faded */
    .node.mi-dim { opacity: .15; }

    /* edge dimming */
    .edgePath.mi-dim,
    .edgeLabel.mi-dim { opacity: .08; transition: opacity .22s; }
  </style>
</head>
<body>
  <div id="toolbar">
    <button id="download-btn" class="tb-btn" onclick="downloadPNG()"><span>&#x2B07;</span>Download PNG</button>
    <button id="reset-btn" class="tb-btn" onclick="resetView()"><span>&#x21BA;</span>Reset view</button>
  </div>
  <div id="diagram-container">
    <pre class="mermaid">
MERMAID_SOURCE
    </pre>
  </div>

  MERMAID_JS_TAG
  <script>
    // ── graph data injected by Python ────────────────────────────────────
    const GRAPH = GRAPH_JSON;

    // ── Mermaid initialisation ───────────────────────────────────────────
    mermaid.initialize({
      startOnLoad: false,
      securityLevel: "loose",
      theme: "default",
      themeVariables: {
        primaryColor: "#d6e4f0",
        primaryBorderColor: "#4682b4",
        primaryTextColor: "#1a1a2e",
        secondaryColor: "#dce8f5",
        tertiaryColor: "#e8f2fa"
      }
    });

    // ── graph traversal (BFS) ────────────────────────────────────────────
    /**
     * Return the Set of all node IDs related to `nodeId`:
     * the node itself + all ancestors + all descendants.
     */
    function getRelated(nodeId) {
      const related = new Set([nodeId]);

      // forward BFS → descendants
      let queue = [nodeId];
      while (queue.length) {
        const id = queue.shift();
        for (const child of (GRAPH.children[id] || [])) {
          if (!related.has(child)) { related.add(child); queue.push(child); }
        }
      }

      // reverse BFS → ancestors
      queue = [nodeId];
      while (queue.length) {
        const id = queue.shift();
        for (const parent of (GRAPH.parents[id] || [])) {
          if (!related.has(parent)) { related.add(parent); queue.push(parent); }
        }
      }

      return related;
    }

    // ── SVG ↔ node-ID mapping ────────────────────────────────────────────
    const knownIds = Object.keys(GRAPH.nodes);

    /**
     * Map a Mermaid SVG element id (e.g. "flowchart-ReadMe-0") back to
     * the logical node ID (e.g. "ReadMe").
     * Uses longest-prefix matching to handle IDs that share prefixes.
     */
    function svgIdToNodeId(svgId) {
      if (!svgId) return null;

      // Mermaid v10 pattern: "flowchart-{nodeId}-{n}"
      const prefix = "flowchart-";
      if (!svgId.startsWith(prefix)) return null;
      const rest = svgId.slice(prefix.length);

      let best = null;
      for (const id of knownIds) {
        if (!rest.startsWith(id)) continue;
        const suffix = rest.slice(id.length);
        if (suffix === "" || /^-\\d+$/.test(suffix)) {
          if (!best || id.length > best.length) best = id;
        }
      }
      return best;
    }

    /**
     * Map a Mermaid edge SVG id (e.g. "L-ReadMe-Guides-0") back to
     * { src, dst } logical node IDs.
     */
    function svgEdgeToNodeIds(svgId) {
      if (!svgId) return null;
      const prefix = "L-";
      if (!svgId.startsWith(prefix)) return null;
      const rest = svgId.slice(prefix.length);

      for (const src of knownIds) {
        if (!rest.startsWith(src + "-")) continue;
        const after = rest.slice(src.length + 1);
        for (const dst of knownIds) {
          if (!after.startsWith(dst)) continue;
          const suffix = after.slice(dst.length);
          if (suffix === "" || /^-\\d+$/.test(suffix)) {
            return { src, dst };
          }
        }
      }
      return null;
    }

    // ── highlight / reset ────────────────────────────────────────────────
    let selectedId = null;

    function resetHighlight() {
      document.querySelectorAll(".node, .edgePath, .edgeLabel").forEach(el => {
        el.classList.remove("mi-selected", "mi-related", "mi-dim");
      });
      selectedId = null;
    }

    function resetView() {
      resetHighlight();
      if (window._fitDiagram) window._fitDiagram();
    }

    function applyHighlight(nodeId) {
      // toggle off if same node clicked again
      if (selectedId === nodeId) { resetHighlight(); return; }
      selectedId = nodeId;

      const related = getRelated(nodeId);

      // style nodes
      document.querySelectorAll(".node").forEach(el => {
        const id = svgIdToNodeId(el.id || "");
        el.classList.remove("mi-selected", "mi-related", "mi-dim");
        if (!id) return;
        if (id === nodeId)        el.classList.add("mi-selected");
        else if (related.has(id)) el.classList.add("mi-related");
        else                      el.classList.add("mi-dim");
      });

      // style edges
      document.querySelectorAll(".edgePath, .edgeLabel").forEach(el => {
        el.classList.remove("mi-related", "mi-dim");
        const ids = svgEdgeToNodeIds(el.id || "");
        if (!ids) return;
        if (!(related.has(ids.src) && related.has(ids.dst))) {
          el.classList.add("mi-dim");
        }
      });

    }

    // ── download PNG ─────────────────────────────────────────────────────
    function downloadPNG() {
      const svgEl = document.querySelector(".mermaid svg");
      if (!svgEl) return;

      const rect = svgEl.getBoundingClientRect();
      const w = Math.max(Math.round(rect.width), 400);
      const h = Math.max(Math.round(rect.height), 300);

      // Clone SVG and set explicit dimensions so the canvas renders correctly
      const clone = svgEl.cloneNode(true);
      clone.setAttribute("width", w);
      clone.setAttribute("height", h);
      // Ensure a white background is embedded in the SVG
      const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
      bg.setAttribute("width", "100%");
      bg.setAttribute("height", "100%");
      bg.setAttribute("fill", "#ffffff");
      clone.insertBefore(bg, clone.firstChild);

      const svgStr = new XMLSerializer().serializeToString(clone);
      const blob = new Blob([svgStr], { type: "image/svg+xml;charset=utf-8" });
      const url = URL.createObjectURL(blob);

      const img = new Image();
      img.onload = () => {
        // Render at 2× for crisp high-DPI output
        const canvas = document.createElement("canvas");
        canvas.width  = w * 2;
        canvas.height = h * 2;
        const ctx = canvas.getContext("2d");
        ctx.fillStyle = "#ffffff";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        ctx.scale(2, 2);
        ctx.drawImage(img, 0, 0, w, h);
        URL.revokeObjectURL(url);

        const a = document.createElement("a");
        a.download = "diagram.png";
        a.href = canvas.toDataURL("image/png");
        a.click();
      };
      img.onerror = () => {
        URL.revokeObjectURL(url);
        console.error("Failed to load SVG for PNG export.");
      };
      img.src = url;
    }

    // ── attach handlers after Mermaid has rendered ───────────────────────
    async function init() {
      await mermaid.run();

      const svg = document.querySelector(".mermaid svg");
      if (!svg) {
        console.warn("Mermaid SVG not found — interactive features disabled.");
        return;
      }

      const container = document.getElementById("diagram-container");

      // -- Read natural SVG size from viewBox (set by Mermaid) --------------
      let naturalW, naturalH;
      const vb = svg.viewBox && svg.viewBox.baseVal;
      if (vb && vb.width > 0) {
        naturalW = vb.width;
        naturalH = vb.height;
      } else {
        const r = svg.getBoundingClientRect();
        naturalW = r.width  || 800;
        naturalH = r.height || 600;
      }
      svg.setAttribute("width",  naturalW);
      svg.setAttribute("height", naturalH);
      svg.style.display = "block";

      // -- Wrap SVG in a transform layer ------------------------------------
      const zoomLayer = document.createElement("div");
      zoomLayer.style.cssText =
        "display:inline-block;transform-origin:0 0;will-change:transform;";
      svg.parentNode.insertBefore(zoomLayer, svg);
      zoomLayer.appendChild(svg);

      // -- Pan / zoom state -------------------------------------------------
      let scale = 1, tx = 0, ty = 0;

      function applyTransform() {
        zoomLayer.style.transform =
          `translate(${tx}px,${ty}px) scale(${scale})`;
      }

      function fitDiagram() {
        const cw = container.clientWidth;
        const ch = container.clientHeight;
        scale = Math.min((cw - 40) / naturalW, (ch - 40) / naturalH);
        tx = Math.max((cw - naturalW * scale) / 2, 0);
        ty = Math.max((ch - naturalH * scale) / 2, 0);
        applyTransform();
      }

      fitDiagram();
      window._fitDiagram = fitDiagram;

      // -- Wheel → zoom toward cursor ---------------------------------------
      container.addEventListener("wheel", e => {
        e.preventDefault();
        const factor = e.deltaY < 0 ? 1.15 : 1 / 1.15;
        const r  = container.getBoundingClientRect();
        const mx = e.clientX - r.left;
        const my = e.clientY - r.top;
        tx = mx - (mx - tx) * factor;
        ty = my - (my - ty) * factor;
        scale = Math.max(0.05, Math.min(20, scale * factor));
        applyTransform();
      }, { passive: false });

      // -- Drag → pan -------------------------------------------------------
      let isPanning = false, moved = false;
      let px0 = 0, py0 = 0, tx0 = 0, ty0 = 0;

      container.addEventListener("mousedown", e => {
        if (e.button !== 0) return;
        isPanning = true;
        moved     = false;
        px0 = e.clientX; py0 = e.clientY;
        tx0 = tx;        ty0 = ty;
        container.classList.add("panning");
        e.preventDefault();
      });

      window.addEventListener("mousemove", e => {
        if (!isPanning) return;
        const dx = e.clientX - px0;
        const dy = e.clientY - py0;
        if (Math.abs(dx) > 3 || Math.abs(dy) > 3) moved = true;
        tx = tx0 + dx;
        ty = ty0 + dy;
        applyTransform();
      });

      window.addEventListener("mouseup", () => {
        isPanning = false;
        container.classList.remove("panning");
      });

      // -- Node click (highlight) -------------------------------------------
      svg.querySelectorAll("g.node").forEach(el => {
        el.addEventListener("click", e => {
          if (moved) return;
          e.stopPropagation();
          const id = svgIdToNodeId(el.id || "");
          if (id) applyHighlight(id);
        });
      });

      // -- Background click → reset highlight --------------------------------
      svg.addEventListener("click", () => {
        if (moved) return;
        resetHighlight();
      });
    }

    init().catch(err => console.error("Mermaid init error:", err));
  </script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Mermaid JS bundling
# ---------------------------------------------------------------------------

_MERMAID_CDN_URL = "https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"
_MERMAID_CACHE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "mermaid.min.js"
)


def _get_mermaid_js() -> str | None:
    """Return mermaid.min.js content from local cache, downloading on first use."""
    if os.path.isfile(_MERMAID_CACHE_PATH):
        try:
            with open(_MERMAID_CACHE_PATH, encoding="utf-8") as fh:
                content = fh.read()
            if content.strip():
                return content
        except OSError:
            pass

    print("Downloading mermaid.min.js from CDN (one-time cache)...", file=sys.stderr)
    try:
        with urllib.request.urlopen(_MERMAID_CDN_URL, timeout=30) as resp:
            content = resp.read().decode("utf-8")
    except Exception as exc:
        print(f"Warning: could not download mermaid.min.js: {exc}", file=sys.stderr)
        return None

    try:
        with open(_MERMAID_CACHE_PATH, "w", encoding="utf-8") as fh:
            fh.write(content)
    except OSError:
        pass
    return content


# ---------------------------------------------------------------------------
# HTML generator
# ---------------------------------------------------------------------------

def _build_graph_json(nodes: dict[str, str], edges: list[tuple[str, str]]) -> str:
    children: dict[str, list[str]] = {}
    parents: dict[str, list[str]] = {}
    for src, dst in edges:
        children.setdefault(src, []).append(dst)
        parents.setdefault(dst, []).append(src)
    raw = json.dumps(
        {"nodes": nodes, "children": children, "parents": parents},
        ensure_ascii=False,
        indent=2,
    )
    # Escape HTML special characters so the JSON is safe to embed inside a
    # <script> block.  JSON \uXXXX escapes are invisible to JavaScript but
    # prevent the browser HTML parser from misinterpreting tag-like strings.
    return (
        raw.replace("&", r"\u0026")
           .replace("<", r"\u003c")
           .replace(">", r"\u003e")
    )


_CDN_SCRIPT_TAG = (
    '<!-- Mermaid.js v10 -->\n'
    '  <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>'
)


def generate_html(mermaid_text: str) -> str:
    """
    Parse *mermaid_text* and return a complete, self-contained interactive
    HTML document as a string.
    """
    nodes, edges = parse_mermaid(mermaid_text)
    graph_json = _build_graph_json(nodes, edges)

    # Embed the Mermaid source safely inside a <pre> tag.
    # html.escape encodes <, >, & so they don't break the HTML parser;
    # the browser decodes them back to plain text before Mermaid reads
    # the element's textContent.
    safe_mermaid = html_module.escape(mermaid_text)

    js_content = _get_mermaid_js()
    mermaid_js_tag = f"<script>{js_content}</script>" if js_content else _CDN_SCRIPT_TAG

    return (
        _HTML_TEMPLATE
        .replace("MERMAID_JS_TAG", mermaid_js_tag)
        .replace("MERMAID_SOURCE", safe_mermaid)
        .replace("GRAPH_JSON", graph_json)
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="mermaid_interactive",
        description=(
            "Convert Mermaid diagram syntax into a self-contained "
            "interactive HTML file."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    src = p.add_mutually_exclusive_group()
    src.add_argument(
        "input_file",
        nargs="?",
        metavar="INPUT_FILE",
        help="Path to a .mmd file (or any text file with Mermaid syntax). "
             "Omit to read from stdin.",
    )
    src.add_argument(
        "-t", "--text",
        metavar="DIAGRAM_TEXT",
        help="Mermaid diagram text passed directly as a string.",
    )
    p.add_argument(
        "-o", "--output",
        metavar="OUTPUT_FILE",
        default="interactive_diagram.html",
        help="Output HTML file path (default: interactive_diagram.html).",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    # Read Mermaid source
    if args.text:
        # Support Python-style \n escapes when passed from shell
        mermaid_text = args.text.replace("\\n", "\n")
    elif args.input_file:
        try:
            with open(args.input_file, encoding="utf-8") as fh:
                mermaid_text = fh.read()
        except OSError as exc:
            print(f"Error reading {args.input_file!r}: {exc}", file=sys.stderr)
            return 1
    else:
        if sys.stdin.isatty():
            print(
                "No input provided.  Pass a file, use -t, or pipe Mermaid "
                "syntax via stdin.\nRun with --help for usage.",
                file=sys.stderr,
            )
            return 1
        mermaid_text = sys.stdin.read()

    if not mermaid_text.strip():
        print("Error: empty diagram input.", file=sys.stderr)
        return 1

    html_content = generate_html(mermaid_text)

    try:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(html_content)
    except OSError as exc:
        print(f"Error writing {args.output!r}: {exc}", file=sys.stderr)
        return 1

    print(f"Interactive diagram written to: {os.path.abspath(args.output)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
