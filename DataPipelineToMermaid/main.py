"""CLI entry point for DataPipelineToMermaid.

Usage examples
--------------
  # Full pipeline: extract lineage from SQL → JSON + Excel + Mermaid
  python -m DataPipelineToMermaid.main full  input.sql  -o output_dir/

  # Extract only (requires OpenRouter API key)
  python -m DataPipelineToMermaid.main extract  input.sql  -o lineage.json

  # Convert existing JSON to Excel + Mermaid (no LLM call)
  python -m DataPipelineToMermaid.main convert  lineage.json  -o output_dir/
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="DataPipelineToMermaid",
        description=(
            "Extract column-level data lineage from SQL / Informatica / "
            "Ab-Initio / Pandas code using an LLM, then convert to Excel "
            "and Mermaid."
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    # ── extract ─────────────────────────────────────────────────────
    p_ext = sub.add_parser(
        "extract",
        help="Run the LLM agent to extract lineage → JSON",
    )
    p_ext.add_argument("input", help="Source file (SQL, Python, XML, …)")
    p_ext.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output JSON path (default: <input>.lineage.json)",
    )
    p_ext.add_argument(
        "--skip-lineage-fields",
        nargs="*",
        default=[],
        metavar="FIELD",
        dest="skip_lineage_fields",
        help=(
            "ColumnLineage fields to omit from the output JSON "
            "(saves tokens / reduces verbosity). "
            "Choices: notes filename intermediate_steps transformation_type"
        ),
    )
    p_ext.add_argument("-v", "--verbose", action="store_true")

    # ── convert ─────────────────────────────────────────────────────
    p_cvt = sub.add_parser(
        "convert",
        help="Convert existing lineage JSON → Excel + Mermaid (no LLM)",
    )
    p_cvt.add_argument("input", help="Lineage JSON file")
    p_cvt.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Output directory (default: same as input)",
    )
    p_cvt.add_argument(
        "--detail",
        choices=["table", "column"],
        default="table",
        help="Mermaid detail level (default: table)",
    )
    p_cvt.add_argument(
        "--no-excel", action="store_true", help="Skip Excel generation"
    )
    p_cvt.add_argument(
        "--no-mermaid", action="store_true", help="Skip Mermaid generation"
    )
    p_cvt.add_argument(
        "--no-html", action="store_true", help="Skip HTML generation"
    )
    p_cvt.add_argument(
        "--skip-lineage-fields",
        nargs="*",
        default=[],
        metavar="FIELD",
        dest="skip_lineage_fields",
        help=(
            "ColumnLineage fields to omit when re-exporting to JSON. "
            "Choices: notes filename intermediate_steps transformation_type"
        ),
    )
    p_cvt.add_argument("-v", "--verbose", action="store_true")

    # ── full ────────────────────────────────────────────────────────
    p_full = sub.add_parser(
        "full",
        help="Extract lineage then convert → JSON + Excel + Mermaid",
    )
    p_full.add_argument("input", help="Source file (SQL, Python, XML, …)")
    p_full.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="Output directory (default: ./output/)",
    )
    p_full.add_argument(
        "--detail",
        choices=["table", "column"],
        default="table",
        help="Mermaid detail level (default: table)",
    )
    p_full.add_argument(
        "--skip-lineage-fields",
        nargs="*",
        default=[],
        metavar="FIELD",
        dest="skip_lineage_fields",
        help=(
            "ColumnLineage fields to omit from the output JSON "
            "(saves tokens / reduces verbosity). "
            "Choices: notes filename intermediate_steps transformation_type"
        ),
    )
    p_full.add_argument("-v", "--verbose", action="store_true")

    return p.parse_args(argv)


def cmd_extract(args: argparse.Namespace) -> None:
    from .agent import extract_lineage

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    output_path = args.output or str(input_path.with_suffix(".lineage.json"))

    print(f"📂 Input:  {input_path}")
    print(f"📄 Output: {output_path}")
    print()

    skip = args.skip_lineage_fields or []
    lineage = extract_lineage(str(input_path), verbose=args.verbose)
    lineage.to_json(output_path, skip_lineage_fields=skip or None)

    if skip:
        print(f"   (fields omitted from column_lineage: {', '.join(skip)})")
    print(f"\n✅ Lineage JSON written to {output_path}")
    print(f"   {len(lineage.sources)} sources, "
          f"{len(lineage.targets)} targets, "
          f"{len(lineage.column_lineage)} column mappings")


def cmd_convert(args: argparse.Namespace) -> None:
    from .excel_export import lineage_to_excel
    from .mermaid_export import lineage_to_html, lineage_to_mermaid_file
    from .models import PipelineLineage

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    skip = args.skip_lineage_fields or []
    lineage = PipelineLineage.from_json(input_path)
    out_dir = Path(args.output_dir) if args.output_dir else input_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    stem = input_path.stem.replace(".lineage", "")

    print(f"📂 Input:      {input_path}")
    print(f"📁 Output dir: {out_dir}")
    print(f"   Pipeline:   {lineage.pipeline_name} ({lineage.pipeline_type})")
    if skip:
        print(f"   Skipping column_lineage fields: {', '.join(skip)}")
    print()

    if not args.no_excel:
        xlsx = lineage_to_excel(lineage, out_dir / f"{stem}.xlsx")
        print(f"  📊 Excel:   {xlsx}")

    if not args.no_mermaid:
        mmd = lineage_to_mermaid_file(
            lineage, out_dir / f"{stem}.mmd", detail_level=args.detail
        )
        print(f"  📐 Mermaid:  {mmd}")

    if not args.no_html:
        html = lineage_to_html(
            lineage, out_dir / f"{stem}.html", detail_level=args.detail
        )
        print(f"  🌐 HTML:     {html}")

    print("\n✅ Done!")


def cmd_full(args: argparse.Namespace) -> None:
    from .agent import extract_lineage
    from .excel_export import lineage_to_excel
    from .mermaid_export import lineage_to_html, lineage_to_mermaid_file

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"ERROR: File not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.output_dir) if args.output_dir else Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = input_path.stem

    print(f"📂 Input:      {input_path}")
    print(f"📁 Output dir: {out_dir}")
    print()

    skip = args.skip_lineage_fields or []

    # Step 1: Extract
    print("─── Step 1/2: Extracting lineage via LLM ───")
    lineage = extract_lineage(str(input_path), verbose=args.verbose)
    json_path = out_dir / f"{stem}.lineage.json"
    lineage.to_json(json_path, skip_lineage_fields=skip or None)
    print(f"  📄 JSON:     {json_path}")
    if skip:
        print(f"   (fields omitted from column_lineage: {', '.join(skip)})")

    # Step 2: Convert
    print("\n─── Step 2/2: Generating artifacts ───")
    xlsx = lineage_to_excel(lineage, out_dir / f"{stem}.xlsx")
    print(f"  📊 Excel:    {xlsx}")

    mmd = lineage_to_mermaid_file(
        lineage, out_dir / f"{stem}.mmd", detail_level=args.detail
    )
    print(f"  📐 Mermaid:  {mmd}")

    html = lineage_to_html(
        lineage, out_dir / f"{stem}.html", detail_level=args.detail
    )
    print(f"  🌐 HTML:     {html}")

    print(f"\n✅ Full pipeline complete!")
    print(f"   {len(lineage.sources)} sources, "
          f"{len(lineage.targets)} targets, "
          f"{len(lineage.column_lineage)} column mappings, "
          f"{len(lineage.components)} components")


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.command == "extract":
        cmd_extract(args)
    elif args.command == "convert":
        cmd_convert(args)
    elif args.command == "full":
        cmd_full(args)
    else:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
