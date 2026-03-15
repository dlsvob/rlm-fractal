"""
build_fractal_db.py — Create fractal.duckdb by copying bmdx.duckdb and linking papers to their PDFs.

Phase 1 of the fractal graph system. This script:
1. Copies the BMDX knowledge base (bmdx.duckdb) wholesale into fractal.duckdb,
   preserving all 9 existing tables (papers, citation_edges, genes, etc.).
2. Reads the download manifest (pdfs/download_manifest.json) produced by fetch_pdfs.py
   to identify which papers have downloaded PDFs.
3. Creates a `paper_pdfs` table that links paper_ids to their PDF files on disk,
   recording actual file sizes and download metadata.
4. Verifies that every referenced PDF file actually exists on disk.

The result is a single fractal.duckdb that lets you query papers, browse the citation
graph, AND know which papers have PDFs ready for parsing in Phase 2.

Usage:
    uv run python build_fractal_db.py
"""

import json
import os
import shutil
import sys

import duckdb


# === Configuration ===

# Where the source BMDX database lives (built by rlm-bmdx project)
BMDX_DB_PATH = os.path.expanduser("~/AI/rlm-bmdx/bmdx.duckdb")

# Output database — the fractal graph's persistent store
FRACTAL_DB_PATH = os.path.join(os.path.dirname(__file__), "fractal.duckdb")

# Directory containing downloaded PDFs and the download manifest
PDF_DIR = os.path.join(os.path.dirname(__file__), "pdfs")
MANIFEST_PATH = os.path.join(PDF_DIR, "download_manifest.json")


# === Schema ===

# paper_pdfs links each paper to its PDF file on disk.
# parse_status tracks progress through the Phase 2 parsing pipeline:
#   'pending'  — PDF exists but hasn't been parsed yet
#   'parsed'   — successfully parsed with structure
#   'partial'  — raw text extracted but no structure classification
#   'failed'   — parsing crashed or produced no usable output
PAPER_PDFS_SCHEMA = """
CREATE TABLE IF NOT EXISTS paper_pdfs (
    paper_id      VARCHAR PRIMARY KEY,
    filename      VARCHAR,
    file_size     INTEGER,
    pdf_source    VARCHAR,
    download_url  VARCHAR,
    parse_status  VARCHAR DEFAULT 'pending'
);
"""


def copy_bmdx_database():
    """
    Copy bmdx.duckdb → fractal.duckdb.

    We copy the entire file rather than exporting/importing tables because
    bmdx.duckdb is only ~17 MB and this preserves all indexes, types, and
    constraints exactly. If fractal.duckdb already exists, it is overwritten
    so we always start from a clean copy of the latest bmdx data.
    """
    if not os.path.exists(BMDX_DB_PATH):
        print(f"ERROR: Source database not found at {BMDX_DB_PATH}")
        print("       Run the bmdx pipeline first to build bmdx.duckdb.")
        sys.exit(1)

    print(f"Copying {BMDX_DB_PATH} → {FRACTAL_DB_PATH}")
    shutil.copy2(BMDX_DB_PATH, FRACTAL_DB_PATH)

    # Quick sanity check: open the copy and verify core tables exist
    con = duckdb.connect(FRACTAL_DB_PATH)
    tables = [
        r[0]
        for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
    ]
    con.close()

    expected = {"papers", "citation_edges", "genes", "paper_genes", "paper_claims"}
    missing = expected - set(tables)
    if missing:
        print(f"WARNING: Expected tables missing from copy: {missing}")
    else:
        print(f"  Verified {len(tables)} tables present: {sorted(tables)}")


def load_manifest():
    """
    Load the download manifest JSON created by fetch_pdfs.py.

    The manifest is a dict keyed by paper_id. Each entry has a 'status' field:
    - 'downloaded': PDF was successfully fetched (has 'filename', 'url', 'source')
    - 'download_failed': Unpaywall found a URL but download failed
    - 'no_oa': No open-access PDF found via Unpaywall
    - 'landing_page_only': Only a landing page URL, not a direct PDF link

    We only care about 'downloaded' entries — those are the ones with PDFs on disk.
    """
    if not os.path.exists(MANIFEST_PATH):
        print(f"ERROR: Download manifest not found at {MANIFEST_PATH}")
        print("       Run fetch_pdfs.py first to download PDFs.")
        sys.exit(1)

    with open(MANIFEST_PATH, "r") as f:
        manifest = json.load(f)

    # Count entries by status for reporting
    status_counts = {}
    for entry in manifest.values():
        s = entry.get("status", "unknown")
        status_counts[s] = status_counts.get(s, 0) + 1

    print(f"Manifest loaded: {len(manifest)} entries")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")

    return manifest


def populate_paper_pdfs(con, manifest):
    """
    Create and populate the paper_pdfs table from the download manifest.

    For each 'downloaded' entry in the manifest:
    - Verifies the PDF file actually exists on disk
    - Reads the actual file size (not trusting the manifest)
    - Inserts a row into paper_pdfs with parse_status='pending'

    Papers whose PDFs are missing on disk are logged as warnings but skipped.
    This catches cases where PDFs were deleted or moved after download.
    """
    con.execute(PAPER_PDFS_SCHEMA)

    # Filter to only downloaded entries — those are the ones with PDFs
    downloaded = {
        pid: entry
        for pid, entry in manifest.items()
        if entry.get("status") == "downloaded"
    }

    inserted = 0
    missing_files = 0

    for paper_id, entry in downloaded.items():
        filename = entry.get("filename", "")
        pdf_path = os.path.join(PDF_DIR, filename)

        # Verify the PDF file actually exists on disk
        if not os.path.exists(pdf_path):
            print(f"  WARNING: PDF missing on disk: {filename} (paper_id={paper_id})")
            missing_files += 1
            continue

        # Read actual file size from disk — more reliable than trusting metadata
        file_size = os.path.getsize(pdf_path)

        # pdf_source is the host type from Unpaywall (e.g. 'publisher', 'repository')
        pdf_source = entry.get("source", "unknown")

        # download_url is the URL the PDF was fetched from
        download_url = entry.get("url", "")

        con.execute(
            """
            INSERT INTO paper_pdfs (paper_id, filename, file_size, pdf_source, download_url, parse_status)
            VALUES (?, ?, ?, ?, ?, 'pending')
            """,
            [paper_id, filename, file_size, pdf_source, download_url],
        )
        inserted += 1

    print(f"\npaper_pdfs populated: {inserted} rows inserted, {missing_files} files missing on disk")
    return inserted, missing_files


def verify_results(con):
    """
    Run verification queries on the completed fractal.duckdb to confirm
    the build was successful. Prints summary stats and spot-checks.
    """
    print("\n=== Verification ===")

    # Count papers and PDFs
    paper_count = con.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    pdf_count = con.execute("SELECT COUNT(*) FROM paper_pdfs").fetchone()[0]
    print(f"Papers in DB:        {paper_count}")
    print(f"Papers with PDFs:    {pdf_count}")

    # How many papers with PDFs also exist in the papers table?
    # (They should all match since paper_ids come from the same source)
    linked = con.execute("""
        SELECT COUNT(*) FROM paper_pdfs pp
        JOIN papers p ON pp.paper_id = p.paper_id
    """).fetchone()[0]
    print(f"PDFs linked to papers table: {linked}")

    unlinked = pdf_count - linked
    if unlinked > 0:
        print(f"  WARNING: {unlinked} PDFs have paper_ids not found in the papers table")

    # File size stats — useful for spotting corrupt downloads (too small = likely HTML error pages)
    size_stats = con.execute("""
        SELECT
            MIN(file_size) as min_size,
            MAX(file_size) as max_size,
            ROUND(AVG(file_size)) as avg_size,
            SUM(file_size) as total_size
        FROM paper_pdfs
    """).fetchone()
    print(f"PDF sizes: min={size_stats[0]:,}B, max={size_stats[1]:,}B, "
          f"avg={size_stats[2]:,.0f}B, total={size_stats[3]/1e9:.1f}GB")

    # Spot-check: show 5 random papers with their PDF info
    print("\nSpot-check (5 random papers with PDFs):")
    samples = con.execute("""
        SELECT p.title, pp.filename, pp.file_size, pp.pdf_source
        FROM paper_pdfs pp
        JOIN papers p ON pp.paper_id = p.paper_id
        ORDER BY RANDOM()
        LIMIT 5
    """).fetchall()
    for title, fname, fsize, source in samples:
        # Truncate long titles for display
        short_title = (title[:70] + "...") if len(title) > 70 else title
        print(f"  [{source}] {fsize:>10,}B  {short_title}")

    # Citation graph coverage: how many papers in the citation graph have PDFs?
    cited_with_pdfs = con.execute("""
        SELECT COUNT(DISTINCT ce.source_id) FROM citation_edges ce
        JOIN paper_pdfs pp ON ce.source_id = pp.paper_id
    """).fetchone()[0]
    total_citing = con.execute("SELECT COUNT(DISTINCT source_id) FROM citation_edges").fetchone()[0]
    print(f"\nCitation graph coverage: {cited_with_pdfs}/{total_citing} citing papers have PDFs")


def main():
    """
    Main entry point. Orchestrates the three steps:
    1. Copy bmdx.duckdb → fractal.duckdb
    2. Load manifest and populate paper_pdfs table
    3. Verify the result
    """
    print("=" * 60)
    print("Building fractal.duckdb — Phase 1: Foundation Database")
    print("=" * 60)

    # Step 1: Copy the base knowledge base
    copy_bmdx_database()

    # Step 2: Load manifest and populate paper_pdfs
    manifest = load_manifest()
    con = duckdb.connect(FRACTAL_DB_PATH)

    try:
        inserted, missing = populate_paper_pdfs(con, manifest)

        # Step 3: Verify
        verify_results(con)
    finally:
        con.close()

    print("\n" + "=" * 60)
    print(f"Done. fractal.duckdb is ready at: {FRACTAL_DB_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
