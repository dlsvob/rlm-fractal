"""
fetch_pdfs.py — Download open-access PDFs for all non-paywalled papers in the BMDX database.

How it works:
1. Reads the non-paywalled papers list (paper_id + DOI) from a TSV file.
2. For each DOI, queries the Unpaywall API to find an open-access PDF URL.
3. Downloads the PDF and saves it to ~/AI/rlm-fractal/pdfs/ named by paper_id.
4. Tracks progress in a JSON manifest so the script can be resumed if interrupted.

Rate limiting:
- Unpaywall API: polite 10 req/s (they ask for < 100K/day)
- PDF downloads: 1 second between requests to avoid hammering hosts

Usage:
    uv run python fetch_pdfs.py
"""

import json
import os
import sys
import time
import csv
import urllib.request
import urllib.error
import ssl

# === Configuration ===

# Directory where PDFs will be saved
PDF_DIR = os.path.expanduser("~/AI/rlm-fractal/pdfs")

# Input file: TSV with paper_id, title, doi, year, citation_count, is_seed, is_review
PAPERS_TSV = "/tmp/non_paywalled_papers.tsv"

# Manifest file: tracks what we've already processed so we can resume
MANIFEST_FILE = os.path.join(PDF_DIR, "download_manifest.json")

# Unpaywall requires an email in the query string (their policy for polite usage)
UNPAYWALL_EMAIL = "fetch@rlm-fractal.local"

# Delay between Unpaywall API calls (seconds) — stay well under their rate limit
UNPAYWALL_DELAY = 0.15

# Delay between PDF download attempts (seconds) — be polite to hosting servers
DOWNLOAD_DELAY = 1.0

# Timeout for HTTP requests (seconds)
REQUEST_TIMEOUT = 30


def load_manifest():
    """
    Load the download manifest from disk. The manifest is a dict keyed by paper_id,
    with values recording the status of each paper's download attempt.
    Returns an empty dict if the manifest doesn't exist yet.
    """
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE, "r") as f:
            return json.load(f)
    return {}


def save_manifest(manifest):
    """
    Write the manifest to disk. Called after each paper is processed so we can
    resume from where we left off if the script is interrupted.
    """
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=2)


def fetch_unpaywall(doi):
    """
    Query the Unpaywall API for a given DOI.
    Returns a dict with 'pdf_url' and 'source' if an open-access PDF is found,
    or a dict with 'error' describing why no PDF was available.

    Unpaywall returns a JSON object with an 'best_oa_location' field that contains
    the URL of the best available open-access version. We prefer the 'url_for_pdf'
    field (direct PDF link) over 'url_for_landing_page' (which may require navigation).
    """
    api_url = f"https://api.unpaywall.org/v2/{doi}?email={UNPAYWALL_EMAIL}"

    try:
        req = urllib.request.Request(api_url, headers={"User-Agent": "rlm-fractal/1.0"})
        # Create a default SSL context — some systems need this for HTTPS
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return {"error": f"HTTP {e.code}"}
    except Exception as e:
        return {"error": str(e)[:200]}

    # Check best_oa_location first (Unpaywall's top pick)
    best = data.get("best_oa_location")
    if best:
        pdf_url = best.get("url_for_pdf")
        if pdf_url:
            return {"pdf_url": pdf_url, "source": best.get("host_type", "unknown")}
        # Fall back to landing page URL — we can still try to download from it
        landing = best.get("url_for_landing_page") or best.get("url")
        if landing:
            return {"pdf_url": landing, "source": best.get("host_type", "unknown"), "is_landing": True}

    # Check all OA locations if best didn't have a PDF
    for loc in data.get("oa_locations", []):
        pdf_url = loc.get("url_for_pdf")
        if pdf_url:
            return {"pdf_url": pdf_url, "source": loc.get("host_type", "unknown")}

    return {"error": "no_oa_pdf_found"}


def download_pdf(url, dest_path):
    """
    Download a PDF from the given URL and save it to dest_path.
    Returns True on success, or an error string on failure.

    We check that the response content-type looks like a PDF and that the
    downloaded content starts with %PDF to avoid saving HTML error pages.
    """
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (rlm-fractal/1.0; academic research)",
            "Accept": "application/pdf,*/*",
        })
        ctx = ssl.create_default_context()
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ctx) as resp:
            content = resp.read()

            # Basic validation: check if it looks like a PDF
            if not content[:5] == b"%PDF-":
                # Some servers return HTML even with a .pdf URL
                # Check content-type header as a fallback
                ctype = resp.headers.get("Content-Type", "")
                if "pdf" not in ctype.lower() and len(content) < 50000:
                    return f"not_pdf (content-type: {ctype}, size: {len(content)})"

            with open(dest_path, "wb") as f:
                f.write(content)

            return True

    except urllib.error.HTTPError as e:
        return f"HTTP {e.code}"
    except Exception as e:
        return str(e)[:200]


def load_papers():
    """
    Read the TSV of non-paywalled papers.
    Returns a list of dicts with keys: paper_id, title, doi, year, citation_count, is_seed, is_review.
    """
    papers = []
    with open(PAPERS_TSV, "r") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            papers.append(row)
    return papers


def main():
    """
    Main loop: iterate over all non-paywalled papers, look up PDF URLs via Unpaywall,
    download the PDFs, and track everything in a manifest file.

    The manifest allows resuming — papers already processed (success or permanent failure)
    are skipped on subsequent runs.
    """
    os.makedirs(PDF_DIR, exist_ok=True)

    papers = load_papers()
    manifest = load_manifest()

    total = len(papers)
    already_done = sum(1 for p in papers if p["paper_id"] in manifest)
    print(f"Papers to process: {total} total, {already_done} already in manifest, {total - already_done} remaining")

    # Counters for this run
    downloaded = 0
    skipped = 0
    failed = 0
    no_pdf = 0

    for i, paper in enumerate(papers):
        pid = paper["paper_id"]
        doi = paper["doi"]

        # Skip if already processed
        if pid in manifest:
            continue

        # Progress update every 50 papers
        if (i + 1) % 50 == 0 or i == 0:
            print(f"\n[{i+1}/{total}] Processing... (downloaded: {downloaded}, no_pdf: {no_pdf}, failed: {failed})")

        # Step 1: Query Unpaywall for the PDF URL
        unpaywall_result = fetch_unpaywall(doi)
        time.sleep(UNPAYWALL_DELAY)

        if "error" in unpaywall_result:
            # No open-access PDF available
            manifest[pid] = {
                "doi": doi,
                "status": "no_oa",
                "reason": unpaywall_result["error"],
                "title": (paper["title"] or "")[:100],
            }
            no_pdf += 1
            # Save manifest periodically (every 10 papers)
            if (no_pdf + downloaded + failed) % 10 == 0:
                save_manifest(manifest)
            continue

        pdf_url = unpaywall_result["pdf_url"]
        is_landing = unpaywall_result.get("is_landing", False)

        # Skip landing pages — they're HTML, not direct PDF links
        if is_landing:
            manifest[pid] = {
                "doi": doi,
                "status": "landing_page_only",
                "url": pdf_url,
                "title": (paper["title"] or "")[:100],
            }
            no_pdf += 1
            if (no_pdf + downloaded + failed) % 10 == 0:
                save_manifest(manifest)
            continue

        # Step 2: Download the PDF
        # Sanitize paper_id for filename (it's a hex hash, should be safe, but be careful)
        safe_pid = pid.replace("/", "_").replace(" ", "_")
        dest_path = os.path.join(PDF_DIR, f"{safe_pid}.pdf")

        result = download_pdf(pdf_url, dest_path)
        time.sleep(DOWNLOAD_DELAY)

        if result is True:
            manifest[pid] = {
                "doi": doi,
                "status": "downloaded",
                "filename": f"{safe_pid}.pdf",
                "source": unpaywall_result.get("source", "unknown"),
                "url": pdf_url,
                "title": (paper["title"] or "")[:100],
            }
            downloaded += 1
        else:
            manifest[pid] = {
                "doi": doi,
                "status": "download_failed",
                "reason": result,
                "url": pdf_url,
                "title": (paper["title"] or "")[:100],
            }
            failed += 1

        # Save manifest periodically
        if (no_pdf + downloaded + failed) % 10 == 0:
            save_manifest(manifest)

    # Final save
    save_manifest(manifest)

    # Summary
    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"  Downloaded:       {downloaded}")
    print(f"  No OA PDF found:  {no_pdf}")
    print(f"  Download failed:  {failed}")
    print(f"  Previously done:  {already_done}")
    print(f"  Total in manifest: {len(manifest)}")
    print(f"{'='*60}")

    # Count actual PDF files on disk
    pdf_count = len([f for f in os.listdir(PDF_DIR) if f.endswith(".pdf")])
    print(f"  PDF files on disk: {pdf_count}")


if __name__ == "__main__":
    main()
