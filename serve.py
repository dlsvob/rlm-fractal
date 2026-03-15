"""
serve.py — Lightweight FastAPI backend for the fractal database browser.

Serves fractal.duckdb data to the React frontend via JSON endpoints.
This is a simplified version of the rlm-pipe API — no user isolation,
no multi-database support. Just one database: fractal.duckdb.

Endpoints:
  GET /api/stats           — database overview (counts of papers, genes, etc.)
  GET /api/citation-graph  — nodes + edges for the D3 force graph
  GET /api/papers          — paginated paper list with search/filter/sort
  GET /api/papers/{id}     — paper detail with abstract, genes, claims, organs, citations
  GET /api/query           — execute read-only SQL (SELECT only)

Usage:
    uv run uvicorn serve:app --port 8889 --reload
"""

import os
import re

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

# === Configuration ===

# Path to the fractal database built by build_fractal_db.py
DB_PATH = os.path.join(os.path.dirname(__file__), "fractal.duckdb")


# === App setup ===

app = FastAPI(title="rlm-fractal browser", version="0.1.0")

# Allow the Vite dev server (port 5173) to make requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_con() -> duckdb.DuckDBPyConnection:
    """
    Open a read-only connection to fractal.duckdb.

    We open a fresh connection per request rather than sharing one because
    DuckDB connections aren't thread-safe. Read-only mode prevents any
    accidental writes from the browser.
    """
    if not os.path.exists(DB_PATH):
        raise HTTPException(500, f"Database not found: {DB_PATH}. Run build_fractal_db.py first.")
    return duckdb.connect(DB_PATH, read_only=True)


# === Endpoints ===


@app.get("/api/stats")
def stats():
    """
    Database overview — counts of papers, genes, claims, citation edges, etc.
    Used by the frontend header to show summary statistics.
    """
    con = get_con()
    try:
        # Get list of all tables so the frontend knows which tabs to show
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]

        # Count rows in core tables — wrap in try/except for tables that may not exist
        def count(table: str) -> int:
            if table not in tables:
                return 0
            return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        # Organ distribution (for filter dropdowns and stats display)
        organs: dict[str, int] = {}
        if "paper_organs" in tables:
            for organ, cnt in con.execute(
                "SELECT organ, COUNT(*) FROM paper_organs GROUP BY organ ORDER BY COUNT(*) DESC"
            ).fetchall():
                organs[organ] = cnt

        # PDF coverage stats
        pdfs_total = count("paper_pdfs")

        return {
            "tables": tables,
            "papers": count("papers"),
            "genes": count("genes"),
            "pathways": count("pathways"),
            "go_terms": count("go_terms"),
            "citation_edges": count("citation_edges"),
            "claims": count("paper_claims"),
            "pdfs": pdfs_total,
            "chunks": count("document_chunks"),
            "doc_edges": count("document_edges"),
            "organs": organs,
        }
    finally:
        con.close()


@app.get("/api/citation-graph")
def citation_graph():
    """
    Full citation graph for D3 force visualization.

    Returns all papers as nodes and all citation edges as links.
    Node attributes include citation count (for sizing), relevance score
    (for coloring), and is_seed flag (for the golden ring).

    For ~5K papers and ~5K edges this is ~1MB of JSON — acceptable for
    a one-time load. If the DB grows much larger, we'd want pagination
    or server-side filtering, but that's not needed at this scale.
    """
    con = get_con()
    try:
        # Check if paper_pdfs table exists for has_pdf flag
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]
        has_pdf_table = "paper_pdfs" in tables

        # Nodes: every paper in the DB
        if has_pdf_table:
            nodes = [
                {
                    "id": r[0],
                    "title": r[1] or "",
                    "year": r[2],
                    "citation_count": r[3] or 0,
                    "relevance_score": r[4] or 0,
                    "is_seed": bool(r[5]),
                    "has_pdf": r[6] is not None,
                }
                for r in con.execute("""
                    SELECT p.paper_id, p.title, p.year, p.citation_count,
                           p.relevance_score, p.is_seed, pp.paper_id
                    FROM papers p
                    LEFT JOIN paper_pdfs pp ON p.paper_id = pp.paper_id
                """).fetchall()
            ]
        else:
            nodes = [
                {
                    "id": r[0],
                    "title": r[1] or "",
                    "year": r[2],
                    "citation_count": r[3] or 0,
                    "relevance_score": r[4] or 0,
                    "is_seed": bool(r[5]),
                    "has_pdf": False,
                }
                for r in con.execute(
                    "SELECT paper_id, title, year, citation_count, relevance_score, is_seed FROM papers"
                ).fetchall()
            ]

        # Edges: citation links between papers
        edges = [
            {"source": r[0], "target": r[1]}
            for r in con.execute("SELECT source_id, target_id FROM citation_edges").fetchall()
        ]

        return {"nodes": nodes, "edges": edges}
    finally:
        con.close()


@app.get("/api/papers")
def papers(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    q: str | None = Query(None),
    sort: str | None = Query(None),
    organ: str | None = Query(None),
    year_min: int | None = Query(None),
):
    """
    Paginated paper list with search, filter, and sort.

    Query params:
      q        — full-text search across title and abstract (case-insensitive LIKE)
      organ    — filter to papers associated with this organ
      year_min — filter to papers published in or after this year
      sort     — column to sort by; prefix with '-' for descending (e.g. '-citation_count')
      page     — page number (1-indexed)
      per_page — results per page (max 100)
    """
    con = get_con()
    try:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]
        has_organs = "paper_organs" in tables

        # Build WHERE clauses from filters
        conditions = []
        params = []

        if q:
            conditions.append("(p.title ILIKE ? OR p.abstract ILIKE ?)")
            params.extend([f"%{q}%", f"%{q}%"])

        if year_min:
            conditions.append("p.year >= ?")
            params.append(year_min)

        if organ and has_organs:
            conditions.append("p.paper_id IN (SELECT paper_id FROM paper_organs WHERE organ = ?)")
            params.append(organ)

        where = " AND ".join(conditions) if conditions else "1=1"

        # Count total matching rows (for pagination)
        total = con.execute(f"SELECT COUNT(*) FROM papers p WHERE {where}", params).fetchone()[0]

        # Determine sort order — whitelist allowed columns to prevent injection
        allowed_sorts = {"title", "year", "citation_count", "relevance_score"}
        order_clause = "p.relevance_score DESC NULLS LAST"
        if sort:
            desc = sort.startswith("-")
            col = sort.lstrip("-")
            if col in allowed_sorts:
                direction = "DESC" if desc else "ASC"
                order_clause = f"p.{col} {direction} NULLS LAST"

        # Fetch page of results
        offset = (page - 1) * per_page
        rows = con.execute(
            f"""
            SELECT p.paper_id, p.title, p.year, p.venue, p.citation_count,
                   p.relevance_score, p.is_seed
            FROM papers p
            WHERE {where}
            ORDER BY {order_clause}
            LIMIT ? OFFSET ?
            """,
            params + [per_page, offset],
        ).fetchall()

        # If organs table exists, fetch organs for these papers in one query
        paper_ids = [r[0] for r in rows]
        organ_map: dict[str, list[str]] = {}
        if has_organs and paper_ids:
            placeholders = ",".join(["?"] * len(paper_ids))
            organ_rows = con.execute(
                f"SELECT paper_id, organ FROM paper_organs WHERE paper_id IN ({placeholders})",
                paper_ids,
            ).fetchall()
            for pid, org in organ_rows:
                organ_map.setdefault(pid, []).append(org)

        items = [
            {
                "paper_id": r[0],
                "title": r[1],
                "year": r[2],
                "venue": r[3],
                "citation_count": r[4],
                "relevance_score": r[5],
                "is_seed": bool(r[6]) if r[6] is not None else False,
                "organs": organ_map.get(r[0], []),
            }
            for r in rows
        ]

        return {
            "items": items,
            "total": total,
            "page": page,
            "per_page": per_page,
        }
    finally:
        con.close()


@app.get("/api/papers/{paper_id}")
def paper_detail(paper_id: str):
    """
    Full detail for a single paper: title, abstract, year, venue, DOI,
    plus associated genes, claims, organs, and citation links (cited_by, references).

    Also includes PDF availability info from the paper_pdfs table.
    """
    con = get_con()
    try:
        row = con.execute(
            "SELECT paper_id, title, year, abstract, venue, doi, citation_count, "
            "relevance_score, is_seed, is_review FROM papers WHERE paper_id = ?",
            [paper_id],
        ).fetchone()

        if not row:
            raise HTTPException(404, f"Paper not found: {paper_id}")

        paper = {
            "paper_id": row[0],
            "title": row[1],
            "year": row[2],
            "abstract": row[3],
            "venue": row[4],
            "doi": row[5],
            "citation_count": row[6],
            "relevance_score": row[7],
            "is_seed": bool(row[8]) if row[8] is not None else False,
            "is_review": bool(row[9]) if row[9] is not None else False,
        }

        # Associated genes
        paper["genes"] = [
            r[0]
            for r in con.execute(
                "SELECT gene_symbol FROM paper_genes WHERE paper_id = ? ORDER BY gene_symbol",
                [paper_id],
            ).fetchall()
        ]

        # Extracted claims
        paper["claims"] = [
            r[0]
            for r in con.execute(
                "SELECT claim FROM paper_claims WHERE paper_id = ?", [paper_id]
            ).fetchall()
        ]

        # Organs
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]
        if "paper_organs" in tables:
            paper["organs"] = [
                r[0]
                for r in con.execute(
                    "SELECT organ FROM paper_organs WHERE paper_id = ?", [paper_id]
                ).fetchall()
            ]
        else:
            paper["organs"] = []

        # Citation links: papers that cite this one, and papers this one references
        paper["cited_by"] = [
            r[0]
            for r in con.execute(
                "SELECT source_id FROM citation_edges WHERE target_id = ?", [paper_id]
            ).fetchall()
        ]
        paper["references"] = [
            r[0]
            for r in con.execute(
                "SELECT target_id FROM citation_edges WHERE source_id = ?", [paper_id]
            ).fetchall()
        ]

        # PDF info (if available)
        if "paper_pdfs" in tables:
            pdf_row = con.execute(
                "SELECT filename, file_size, pdf_source, download_url, parse_status "
                "FROM paper_pdfs WHERE paper_id = ?",
                [paper_id],
            ).fetchone()
            if pdf_row:
                paper["pdf"] = {
                    "filename": pdf_row[0],
                    "file_size": pdf_row[1],
                    "source": pdf_row[2],
                    "url": pdf_row[3],
                    "parse_status": pdf_row[4],
                }

        return paper
    finally:
        con.close()


@app.get("/api/papers/{paper_id}/document")
def paper_document(paper_id: str):
    """
    Full parsed document for a paper: chunks grouped by section with
    cross-reference edges.

    Returns a structure optimized for the Document tab in the UI:
    - sections: ordered list of {heading, chunks} groups
    - cross_refs: list of {source_chunk_id, target_chunk_id} for
      intra-document references (e.g. paragraph mentioning "Table 1"
      → the Table 1 caption chunk)
    - stats: parse quality, chunk/section counts
    """
    con = get_con()
    try:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]

        if "document_chunks" not in tables:
            return {"sections": [], "cross_refs": [], "stats": None}

        # Fetch all chunks for this paper in reading order
        rows = con.execute("""
            SELECT chunk_id, page_num, chunk_order, chunk_type, section_name,
                   text, font_name, font_size, x_pos, y_pos, parse_quality
            FROM document_chunks
            WHERE paper_id = ?
            ORDER BY page_num, chunk_order
        """, [paper_id]).fetchall()

        if not rows:
            return {"sections": [], "cross_refs": [], "stats": None}

        # Group chunks into sections: each heading starts a new section.
        # Chunks before the first heading go into a preamble section.
        sections = []
        current_section = {"heading": None, "section_name": None, "chunks": []}

        for chunk_id, page_num, chunk_order, chunk_type, section_name, \
                text, font_name, font_size, x_pos, y_pos, parse_quality in rows:
            chunk = {
                "chunk_id": chunk_id,
                "page_num": page_num,
                "chunk_type": chunk_type,
                "section_name": section_name,
                "text": text,
                "font_size": font_size,
            }

            if chunk_type == "heading":
                # Save current section if it has content
                if current_section["chunks"] or current_section["heading"]:
                    sections.append(current_section)
                # Start new section
                current_section = {
                    "heading": chunk,
                    "section_name": section_name,
                    "chunks": [],
                }
            else:
                current_section["chunks"].append(chunk)

        # Don't forget the last section
        if current_section["chunks"] or current_section["heading"]:
            sections.append(current_section)

        # Fetch cross-reference edges for this paper
        cross_refs = []
        if "document_edges" in tables:
            ref_rows = con.execute("""
                SELECT source_id, target_id
                FROM document_edges
                WHERE paper_id = ? AND edge_type = 'references'
            """, [paper_id]).fetchall()
            cross_refs = [
                {"source_id": r[0], "target_id": r[1]}
                for r in ref_rows
            ]

        # Stats
        quality = rows[0][10] if rows else None
        type_counts = {}
        for r in rows:
            t = r[3]
            type_counts[t] = type_counts.get(t, 0) + 1

        stats = {
            "parse_quality": quality,
            "total_chunks": len(rows),
            "total_sections": sum(1 for s in sections if s["heading"]),
            "type_counts": type_counts,
            "pages": max(r[1] for r in rows) if rows else 0,
        }

        return {"sections": sections, "cross_refs": cross_refs, "stats": stats}
    finally:
        con.close()


@app.get("/api/papers/{paper_id}/structure")
def paper_structure(paper_id: str):
    """
    Document structure tree for the Document Explorer view.

    Returns a tree of heading nodes, each with their child chunk
    summaries (type, first 100 chars of text, chunk_id). Also returns
    edge counts per type for this paper.
    """
    con = get_con()
    try:
        tables = [
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        ]

        if "document_chunks" not in tables:
            return {"tree": [], "edge_counts": {}}

        # Fetch headings with their contained chunk counts
        headings = con.execute("""
            SELECT chunk_id, page_num, section_name, text
            FROM document_chunks
            WHERE paper_id = ? AND chunk_type = 'heading'
            ORDER BY page_num, chunk_order
        """, [paper_id]).fetchall()

        # For each heading, get its children via containment edges
        tree = []
        if "document_edges" in tables:
            for h_id, h_page, h_section, h_text in headings:
                children = con.execute("""
                    SELECT dc.chunk_id, dc.chunk_type, dc.page_num,
                           LEFT(dc.text, 100) as preview
                    FROM document_edges de
                    JOIN document_chunks dc ON de.target_id = dc.chunk_id
                    WHERE de.source_id = ? AND de.edge_type = 'contains'
                    ORDER BY dc.page_num, dc.chunk_order
                """, [h_id]).fetchall()

                tree.append({
                    "chunk_id": h_id,
                    "page": h_page,
                    "section_name": h_section,
                    "text": h_text[:150],
                    "children": [
                        {
                            "chunk_id": c[0],
                            "chunk_type": c[1],
                            "page": c[2],
                            "preview": c[3],
                        }
                        for c in children
                    ],
                })
        else:
            for h_id, h_page, h_section, h_text in headings:
                tree.append({
                    "chunk_id": h_id,
                    "page": h_page,
                    "section_name": h_section,
                    "text": h_text[:150],
                    "children": [],
                })

        # Edge counts for this paper
        edge_counts = {}
        if "document_edges" in tables:
            for etype, cnt in con.execute("""
                SELECT edge_type, COUNT(*) FROM document_edges
                WHERE paper_id = ? GROUP BY edge_type
            """, [paper_id]).fetchall():
                edge_counts[etype] = cnt

        return {"tree": tree, "edge_counts": edge_counts}
    finally:
        con.close()


@app.get("/api/query")
def query(sql: str = Query(...)):
    """
    Execute a read-only SQL query against the database.

    Only SELECT statements are allowed — anything else is rejected.
    This is the SQL Console feature for ad-hoc exploration.
    """
    # Security: only allow SELECT statements
    stripped = sql.strip().rstrip(";").strip()
    if not re.match(r"(?i)^(SELECT|WITH|EXPLAIN)\b", stripped):
        raise HTTPException(400, "Only SELECT/WITH/EXPLAIN statements are allowed")

    # Block dangerous patterns even within SELECT (e.g. subquery attacks)
    dangerous = re.compile(r"(?i)\b(DROP|DELETE|INSERT|UPDATE|ALTER|CREATE|ATTACH|COPY|EXPORT)\b")
    if dangerous.search(stripped):
        raise HTTPException(400, "Statement contains disallowed keywords")

    con = get_con()
    try:
        result = con.execute(stripped)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        return {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }
    except duckdb.Error as e:
        raise HTTPException(400, str(e))
    finally:
        con.close()
