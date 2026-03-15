"""
batch_parse.py — Parallel batch runner for parsing all PDFs into fractal.duckdb.

PURPOSE:
  Reads paper_pdfs WHERE parse_status = 'pending', parses each PDF through
  the hardened parser + structure classifier, and stores the results in
  document_chunks. Updates parse_status per paper.

HOW IT WORKS:
  1. Queries fractal.duckdb for all pending PDFs
  2. Parses each PDF using multiprocessing.Pool (CPU-bound work)
  3. Classifies chunks via structure_classifier for untagged PDFs
  4. Inserts chunks into document_chunks table
  5. Updates paper_pdfs.parse_status: 'parsed', 'partial', or 'failed'

RESUMABLE:
  Re-running picks up where it left off — only 'pending' papers are processed.
  To re-parse a paper, set its parse_status back to 'pending'.

QUALITY TIERS:
  - 'parsed':  text extracted with structure (tagged or heuristic sections)
  - 'partial': text extracted but no structure (raw text only)
  - 'failed':  parsing produced no output (corrupt PDF, missing catalog, etc.)

Usage:
    uv run python fractal/parser/batch_parse.py [--workers N] [--limit N]
"""

import argparse
import logging
import os
import sys
import time
from multiprocessing import Pool
from typing import Optional

import duckdb

from .pdf_parser import parse_pdf_raw, TextChunk
from .structure_classifier import classify_chunks

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Default path to fractal.duckdb (relative to project root)
DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'fractal.duckdb')

# Default path to PDFs directory
PDF_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'pdfs')

# Schema for document chunks — every text chunk extracted from a PDF.
# This is the Level 2 data in the fractal graph: the document structure
# within each paper.
DOCUMENT_CHUNKS_SCHEMA = """
CREATE TABLE IF NOT EXISTS document_chunks (
    chunk_id      INTEGER,
    paper_id      VARCHAR,
    page_num      INTEGER,
    chunk_order   INTEGER,
    chunk_type    VARCHAR,
    section_name  VARCHAR,
    text          VARCHAR,
    font_name     VARCHAR,
    font_size     DOUBLE,
    x_pos         DOUBLE,
    y_pos         DOUBLE,
    parse_quality VARCHAR
);
"""

# Auto-incrementing chunk IDs are handled via a sequence.
CHUNK_SEQUENCE = """
CREATE SEQUENCE IF NOT EXISTS chunk_id_seq START 1;
"""


# ---------------------------------------------------------------------------
# Worker function (runs in subprocess)
# ---------------------------------------------------------------------------

def _parse_one_pdf(args: tuple) -> dict:
    """
    Parse a single PDF file. Runs in a worker subprocess.

    Args:
        args: tuple of (paper_id, pdf_path)

    Returns:
        dict with keys: paper_id, status, quality, chunks (list of dicts)

    Why a top-level function: multiprocessing.Pool requires picklable
    functions, so this can't be a lambda or nested function.
    """
    paper_id, pdf_path = args

    try:
        # Step 1: Parse the PDF into raw TextChunks
        chunks, is_tagged = parse_pdf_raw(pdf_path)

        if not chunks:
            return {
                'paper_id': paper_id,
                'status': 'failed',
                'quality': None,
                'chunks': [],
            }

        # Step 2: Classify chunks (handles both tagged and untagged PDFs)
        chunks, quality_tier = classify_chunks(chunks)

        # Step 3: Convert to serializable dicts for DB insertion
        chunk_dicts = []
        for order, chunk in enumerate(chunks):
            section = chunk.properties.get('section_name')
            chunk_dicts.append({
                'paper_id': paper_id,
                'page_num': chunk.page,
                'chunk_order': order,
                'chunk_type': chunk.chunk_type,
                'section_name': section,
                'text': chunk.text,
                'font_name': chunk.font if chunk.font else None,
                'font_size': chunk.font_size if chunk.font_size else None,
                'x_pos': chunk.x if chunk.x else None,
                'y_pos': chunk.y if chunk.y else None,
                'parse_quality': quality_tier,
            })

        # Determine status based on quality
        # 'parsed' = got structured output, 'partial' = raw text only
        has_structure = any(
            c['chunk_type'] in ('heading', 'paragraph', 'table_cell', 'table_caption', 'reference')
            for c in chunk_dicts
        )
        status = 'parsed' if has_structure else 'partial'

        return {
            'paper_id': paper_id,
            'status': status,
            'quality': quality_tier,
            'chunks': chunk_dicts,
        }

    except Exception as e:
        return {
            'paper_id': paper_id,
            'status': 'failed',
            'quality': None,
            'chunks': [],
            'error': str(e)[:200],
        }


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

def run_batch(
    db_path: str = DB_PATH,
    pdf_dir: str = PDF_DIR,
    workers: int = 4,
    limit: Optional[int] = None,
    log_every: int = 50,
):
    """
    Parse all pending PDFs and store results in fractal.duckdb.

    Args:
        db_path:   Path to fractal.duckdb
        pdf_dir:   Directory containing PDF files
        workers:   Number of parallel worker processes
        limit:     Max papers to process (None = all pending)
        log_every: Print progress every N papers
    """
    db_path = os.path.abspath(db_path)
    pdf_dir = os.path.abspath(pdf_dir)

    print(f"Opening {db_path}")
    con = duckdb.connect(db_path)

    # Create schema if needed
    con.execute(DOCUMENT_CHUNKS_SCHEMA)
    con.execute(CHUNK_SEQUENCE)

    # Get pending papers
    query = "SELECT paper_id, filename FROM paper_pdfs WHERE parse_status = 'pending'"
    if limit:
        query += f" LIMIT {limit}"
    pending = con.execute(query).fetchall()

    if not pending:
        print("No pending papers to parse.")
        con.close()
        return

    print(f"Papers to parse: {len(pending)}")
    print(f"Workers: {workers}")

    # Build work items: (paper_id, full_pdf_path)
    work_items = []
    for paper_id, filename in pending:
        pdf_path = os.path.join(pdf_dir, filename)
        if os.path.exists(pdf_path):
            work_items.append((paper_id, pdf_path))
        else:
            # PDF missing on disk — mark as failed immediately
            con.execute(
                "UPDATE paper_pdfs SET parse_status = 'failed' WHERE paper_id = ?",
                [paper_id],
            )

    print(f"PDFs found on disk: {len(work_items)}")

    # Process in parallel
    t0 = time.time()
    parsed = 0
    partial = 0
    failed = 0
    total_chunks = 0

    # Use imap_unordered for streaming results as they complete
    # (rather than waiting for all to finish with map)
    with Pool(processes=workers) as pool:
        for i, result in enumerate(pool.imap_unordered(_parse_one_pdf, work_items, chunksize=4)):
            paper_id = result['paper_id']
            status = result['status']
            chunks = result['chunks']

            # Insert chunks into document_chunks table
            if chunks:
                # Batch insert for efficiency
                con.executemany(
                    """
                    INSERT INTO document_chunks
                        (chunk_id, paper_id, page_num, chunk_order, chunk_type,
                         section_name, text, font_name, font_size, x_pos, y_pos,
                         parse_quality)
                    VALUES (nextval('chunk_id_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (c['paper_id'], c['page_num'], c['chunk_order'],
                         c['chunk_type'], c['section_name'], c['text'],
                         c['font_name'], c['font_size'], c['x_pos'], c['y_pos'],
                         c['parse_quality'])
                        for c in chunks
                    ],
                )
                total_chunks += len(chunks)

            # Update parse_status
            con.execute(
                "UPDATE paper_pdfs SET parse_status = ? WHERE paper_id = ?",
                [status, paper_id],
            )

            # Track counters
            if status == 'parsed':
                parsed += 1
            elif status == 'partial':
                partial += 1
            else:
                failed += 1

            # Progress logging
            done = i + 1
            if done % log_every == 0 or done == len(work_items):
                elapsed = time.time() - t0
                rate = done / elapsed if elapsed > 0 else 0
                remaining = (len(work_items) - done) / rate if rate > 0 else 0
                print(
                    f"  [{done}/{len(work_items)}] "
                    f"parsed={parsed} partial={partial} failed={failed} "
                    f"chunks={total_chunks} "
                    f"({rate:.1f}/s, ~{remaining:.0f}s remaining)"
                )

    con.close()
    elapsed = time.time() - t0

    print(f"\n{'='*60}")
    print(f"BATCH PARSE COMPLETE")
    print(f"{'='*60}")
    print(f"  Parsed (structured): {parsed}")
    print(f"  Partial (raw text):  {partial}")
    print(f"  Failed:              {failed}")
    print(f"  Total chunks:        {total_chunks}")
    print(f"  Time:                {elapsed:.1f}s ({elapsed/max(len(work_items),1):.2f}s/paper)")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    """CLI entry point for batch parsing."""
    parser = argparse.ArgumentParser(description='Batch parse PDFs into fractal.duckdb')
    parser.add_argument('--db', default=DB_PATH, help='Path to fractal.duckdb')
    parser.add_argument('--pdfs', default=PDF_DIR, help='Path to PDFs directory')
    parser.add_argument('--workers', type=int, default=4, help='Number of parallel workers')
    parser.add_argument('--limit', type=int, default=None, help='Max papers to process')
    parser.add_argument('--log-every', type=int, default=50, help='Log progress every N papers')

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,
        format='%(levelname)s: %(message)s',
    )

    run_batch(
        db_path=args.db,
        pdf_dir=args.pdfs,
        workers=args.workers,
        limit=args.limit,
        log_every=args.log_every,
    )


if __name__ == '__main__':
    main()
