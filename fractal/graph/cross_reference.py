"""
cross_reference.py — Detect intra-document references and build reference edges.

PURPOSE:
  Scans paragraph text for mentions of tables and figures within the
  same paper, and creates 'references' edges linking the mentioning
  chunk to the referenced caption chunk.

PATTERNS DETECTED:
  - "Table 1", "Table 2a", "Table S1" → matched to table_caption chunks
  - "Figure 1", "Fig. 1", "Figure S2" → matched to caption chunks
  - "Supplementary Table 1" / "Supplementary Figure 1"

HOW IT WORKS:
  For each paper:
  1. Index all table_caption and caption chunks by their number
     (extracted via regex from the caption text).
  2. Scan all paragraph/other text chunks for mentions of "Table N"
     or "Figure N".
  3. When a mention matches an indexed caption, emit a 'references'
     edge from the mentioning chunk to the caption chunk.

HOW IT FITS:
  Called after document_graph.py builds containment/sequence edges.
  Appends reference edges to the same document_edges table.

Usage:
    uv run python -m fractal.graph.cross_reference [--db path]
"""

import argparse
import logging
import os
import re
import time

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'fractal.duckdb')

# Patterns to extract table/figure numbers from caption text.
# These match the START of a caption chunk's text.
RE_TABLE_NUM = re.compile(
    r'^(?:Supplementary\s+)?Table\s+([A-Z]?-?\d+[a-z]?)',
    re.IGNORECASE
)
RE_FIGURE_NUM = re.compile(
    r'^(?:Supplementary\s+)?(?:Figure|Fig\.?)\s+([A-Z]?-?\d+[a-z]?)',
    re.IGNORECASE
)

# Patterns to find references IN paragraph text.
# These match anywhere in the text (not just start).
RE_TABLE_REF = re.compile(
    r'(?:Supplementary\s+)?Table\s+([A-Z]?-?\d+[a-z]?)',
    re.IGNORECASE
)
RE_FIGURE_REF = re.compile(
    r'(?:Supplementary\s+)?(?:Figure|Fig\.?)\s+([A-Z]?-?\d+[a-z]?)',
    re.IGNORECASE
)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_cross_references(db_path: str = DB_PATH):
    """
    Scan all parsed papers for intra-document table/figure references
    and add 'references' edges to document_edges.
    """
    db_path = os.path.abspath(db_path)
    con = duckdb.connect(db_path)

    # Get all papers that have parsed chunks
    papers = con.execute("""
        SELECT DISTINCT paper_id FROM document_chunks
    """).fetchall()
    total = len(papers)
    print(f"Scanning {total} papers for cross-references...")

    t0 = time.time()
    total_edges = 0
    papers_with_refs = 0

    for i, (paper_id,) in enumerate(papers):
        edges = _find_references_for_paper(con, paper_id)
        if edges:
            con.executemany(
                """
                INSERT INTO document_edges
                    (paper_id, source_type, source_id, target_type, target_id, edge_type)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                edges,
            )
            total_edges += len(edges)
            papers_with_refs += 1

        if (i + 1) % 200 == 0 or (i + 1) == total:
            elapsed = time.time() - t0
            print(f"  [{i+1}/{total}] ref_edges={total_edges:,} papers_with_refs={papers_with_refs}")

    con.close()
    elapsed = time.time() - t0
    print(f"\nDone. {total_edges:,} reference edges across {papers_with_refs} papers ({elapsed:.1f}s)")
    return total_edges


def _find_references_for_paper(con, paper_id: str) -> list[tuple]:
    """
    For a single paper, find all table/figure cross-references.

    Returns list of edge tuples ready for insertion.
    """
    # Step 1: Index captions by their table/figure number
    captions = con.execute("""
        SELECT chunk_id, chunk_type, text
        FROM document_chunks
        WHERE paper_id = ? AND chunk_type IN ('table_caption', 'caption')
    """, [paper_id]).fetchall()

    # Build lookup: normalized reference key → caption chunk_id
    # Key format: "table_1", "figure_3", etc.
    caption_index: dict[str, int] = {}
    for chunk_id, chunk_type, text in captions:
        if chunk_type == 'table_caption':
            m = RE_TABLE_NUM.match(text)
            if m:
                key = f"table_{m.group(1).lower()}"
                caption_index[key] = chunk_id
        elif chunk_type == 'caption':
            m = RE_FIGURE_NUM.match(text)
            if m:
                key = f"figure_{m.group(1).lower()}"
                caption_index[key] = chunk_id
            # Also check if it's a table caption classified as 'caption'
            m = RE_TABLE_NUM.match(text)
            if m:
                key = f"table_{m.group(1).lower()}"
                caption_index[key] = chunk_id

    if not caption_index:
        return []

    # Step 2: Scan text chunks for references to indexed captions
    text_chunks = con.execute("""
        SELECT chunk_id, text
        FROM document_chunks
        WHERE paper_id = ? AND chunk_type IN ('paragraph', 'heading', 'list_item')
    """, [paper_id]).fetchall()

    edges = []
    seen = set()  # avoid duplicate edges

    for chunk_id, text in text_chunks:
        # Find table references
        for m in RE_TABLE_REF.finditer(text):
            key = f"table_{m.group(1).lower()}"
            if key in caption_index:
                target_id = caption_index[key]
                edge_key = (chunk_id, target_id)
                if edge_key not in seen:
                    edges.append((
                        paper_id, 'chunk', chunk_id,
                        'chunk', target_id, 'references'
                    ))
                    seen.add(edge_key)

        # Find figure references
        for m in RE_FIGURE_REF.finditer(text):
            key = f"figure_{m.group(1).lower()}"
            if key in caption_index:
                target_id = caption_index[key]
                edge_key = (chunk_id, target_id)
                if edge_key not in seen:
                    edges.append((
                        paper_id, 'chunk', chunk_id,
                        'chunk', target_id, 'references'
                    ))
                    seen.add(edge_key)

    return edges


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Build cross-reference edges')
    parser.add_argument('--db', default=DB_PATH, help='Path to fractal.duckdb')
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    build_cross_references(args.db)


if __name__ == '__main__':
    main()
