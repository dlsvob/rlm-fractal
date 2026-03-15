"""
document_graph.py — Build containment and sequence edges from parsed document chunks.

PURPOSE:
  Creates the intra-document graph (Level 2 of the fractal). Builds
  two types of edges using bulk SQL (no per-paper loops):

  1. SEQUENCE: chunk → next_chunk ('follows')
     Each chunk follows the previous one within the same paper,
     preserving reading order.

  2. CONTAINMENT: heading → chunk ('contains')
     A heading "contains" all subsequent non-heading chunks until the
     next heading. This lets you query "all paragraphs in the Methods
     section" by traversing containment edges.

HOW IT FITS:
  Called after batch_parse.py populates document_chunks. Uses DuckDB
  window functions for bulk edge generation — no Python loops needed.
  Generates ~2M edges in under a minute.

Usage:
    uv run python -m fractal.graph.document_graph [--db path]
"""

import argparse
import logging
import os
import time

import duckdb

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'fractal.duckdb')

DOCUMENT_EDGES_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_edges (
    paper_id      VARCHAR,
    source_type   VARCHAR,
    source_id     INTEGER,
    target_type   VARCHAR,
    target_id     INTEGER,
    edge_type     VARCHAR
);
"""


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def build_edges(db_path: str = DB_PATH):
    """
    Build containment and sequence edges for all parsed papers using
    bulk SQL with window functions.

    Much faster than per-paper loops: DuckDB processes the entire
    document_chunks table in one pass per edge type.
    """
    db_path = os.path.abspath(db_path)
    con = duckdb.connect(db_path)

    con.execute("DROP TABLE IF EXISTS document_edges")
    con.execute(DOCUMENT_EDGES_SCHEMA)

    t0 = time.time()

    # --- Sequence edges (follows) ---
    # Use LEAD window function to get the next chunk_id within each paper.
    # This generates one edge per consecutive chunk pair.
    print("Building sequence edges...")
    con.execute("""
        INSERT INTO document_edges (paper_id, source_type, source_id, target_type, target_id, edge_type)
        SELECT
            paper_id,
            'chunk',
            chunk_id,
            'chunk',
            LEAD(chunk_id) OVER (PARTITION BY paper_id ORDER BY page_num, chunk_order),
            'follows'
        FROM document_chunks
    """)
    # The LEAD for the last chunk in each paper produces NULL — delete those.
    con.execute("DELETE FROM document_edges WHERE target_id IS NULL")
    seq_count = con.execute(
        "SELECT COUNT(*) FROM document_edges WHERE edge_type = 'follows'"
    ).fetchone()[0]
    print(f"  Sequence edges: {seq_count:,}")

    # --- Containment edges (heading contains chunk) ---
    # Strategy: for each chunk, find the most recent heading chunk in the
    # same paper (by page_num, chunk_order). Use a window function to
    # carry forward the last heading's chunk_id.
    print("Building containment edges...")
    con.execute("""
        INSERT INTO document_edges (paper_id, source_type, source_id, target_type, target_id, edge_type)
        SELECT
            paper_id,
            'heading',
            heading_id,
            'chunk',
            chunk_id,
            'contains'
        FROM (
            SELECT
                paper_id,
                chunk_id,
                chunk_type,
                -- Carry forward the last heading's chunk_id using a conditional
                -- window aggregate: MAX of chunk_id where chunk_type='heading',
                -- but only looking at rows up to the current one.
                -- We use a trick: assign heading_id only to heading rows,
                -- then fill forward with MAX OVER (rows unbounded preceding).
                MAX(CASE WHEN chunk_type = 'heading' THEN chunk_id END)
                    OVER (
                        PARTITION BY paper_id
                        ORDER BY page_num, chunk_order
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS heading_id
            FROM document_chunks
        ) sub
        WHERE heading_id IS NOT NULL
          AND chunk_type != 'heading'
    """)
    cont_count = con.execute(
        "SELECT COUNT(*) FROM document_edges WHERE edge_type = 'contains'"
    ).fetchone()[0]
    print(f"  Containment edges: {cont_count:,}")

    total = seq_count + cont_count
    elapsed = time.time() - t0
    con.close()

    print(f"\nDone. {total:,} total edges in {elapsed:.1f}s")
    return total


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Build document graph edges')
    parser.add_argument('--db', default=DB_PATH, help='Path to fractal.duckdb')
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    build_edges(args.db)


if __name__ == '__main__':
    main()
