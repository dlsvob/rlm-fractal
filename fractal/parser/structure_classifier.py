"""
structure_classifier.py — Heuristic section classification for untagged PDFs.

PURPOSE:
  80% of our journal PDFs lack BDC/EMC structure tags. The core parser
  extracts text chunks with font name, font size, and position, but
  classifies them all as 'unknown'. This module reclassifies those
  chunks using font-size heuristics and text pattern matching.

APPROACH:
  1. Detect body text font size: the most frequent font size across all
     chunks (by character count, not chunk count — a long paragraph at
     12pt outweighs a short heading at 16pt).
  2. Classify relative to body size:
     - Larger than body → heading
     - Body size → paragraph
     - Smaller than body → caption, footnote, or other
  3. Pattern-match section headers by text content:
     "Abstract", "Introduction", "Methods", "Results", "Discussion",
     "References", "Conclusion", etc.
  4. Pattern-match captions: "Table \d+", "Figure \d+", "Fig. \d+"
  5. Detect reference sections: once "References" heading is seen,
     subsequent paragraphs are classified as 'reference'.

QUALITY TIERS:
  - 'tagged':    PDF had BDC/EMC tags (handled by pdf_parser directly)
  - 'heuristic': untagged, classified by this module's font+pattern rules
  - 'raw':       text extracted but classification confidence too low

HOW IT FITS:
  Called by batch_parse.py after pdf_parser extracts raw chunks.
  Takes a list of TextChunk objects, returns the same list with
  chunk_type updated and a quality tier string.
"""

import re
from collections import Counter
from typing import Optional

from .pdf_parser import TextChunk


# ---------------------------------------------------------------------------
# Constants — section name patterns
# ---------------------------------------------------------------------------

# Standard IMRaD section headings found in scientific papers.
# We match case-insensitively and allow trailing numbering ("1. Introduction").
# Why these: they cover >95% of biomedical journal article structures.
SECTION_PATTERNS = [
    (re.compile(r'^(?:\d+\.?\s*)?abstract\s*$', re.IGNORECASE), 'abstract'),
    (re.compile(r'^(?:\d+\.?\s*)?introduction\s*$', re.IGNORECASE), 'introduction'),
    (re.compile(r'^(?:\d+\.?\s*)?background\s*$', re.IGNORECASE), 'introduction'),
    (re.compile(r'^(?:\d+\.?\s*)?materials?\s+and\s+methods?\s*$', re.IGNORECASE), 'methods'),
    (re.compile(r'^(?:\d+\.?\s*)?methods?\s*$', re.IGNORECASE), 'methods'),
    (re.compile(r'^(?:\d+\.?\s*)?experimental\s*(section|procedures?)?\s*$', re.IGNORECASE), 'methods'),
    (re.compile(r'^(?:\d+\.?\s*)?results?\s*$', re.IGNORECASE), 'results'),
    (re.compile(r'^(?:\d+\.?\s*)?results?\s+and\s+discussion\s*$', re.IGNORECASE), 'results'),
    (re.compile(r'^(?:\d+\.?\s*)?discussion\s*$', re.IGNORECASE), 'discussion'),
    (re.compile(r'^(?:\d+\.?\s*)?conclusions?\s*$', re.IGNORECASE), 'conclusion'),
    (re.compile(r'^(?:\d+\.?\s*)?summary\s*$', re.IGNORECASE), 'conclusion'),
    (re.compile(r'^(?:\d+\.?\s*)?references?\s*$', re.IGNORECASE), 'references'),
    (re.compile(r'^(?:\d+\.?\s*)?bibliography\s*$', re.IGNORECASE), 'references'),
    (re.compile(r'^(?:\d+\.?\s*)?acknowledgm?ents?\s*$', re.IGNORECASE), 'acknowledgments'),
    (re.compile(r'^(?:\d+\.?\s*)?supplementary\s*(materials?|information|data)?\s*$', re.IGNORECASE), 'supplementary'),
    (re.compile(r'^(?:\d+\.?\s*)?appendix\s*', re.IGNORECASE), 'appendix'),
    (re.compile(r'^(?:\d+\.?\s*)?funding\s*$', re.IGNORECASE), 'funding'),
    (re.compile(r'^(?:\d+\.?\s*)?(?:author\s+)?contributions?\s*$', re.IGNORECASE), 'contributions'),
    (re.compile(r'^(?:\d+\.?\s*)?(?:conflicts?\s+of\s+interest|competing\s+interests?)\s*$', re.IGNORECASE), 'conflict_of_interest'),
    (re.compile(r'^(?:\d+\.?\s*)?data\s+availability\s*', re.IGNORECASE), 'data_availability'),
]

# Caption patterns for tables and figures.
RE_TABLE_CAPTION = re.compile(r'^Table\s+[A-Z]?-?\d+[.:]?\s', re.IGNORECASE)
RE_FIGURE_CAPTION = re.compile(r'^(?:Figure|Fig\.?)\s+[A-Z]?-?\d+[.:]?\s', re.IGNORECASE)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def detect_body_font_size(chunks: list[TextChunk]) -> Optional[float]:
    """
    Detect the most common font size in the document, weighted by
    character count.

    Why character count: a 500-character paragraph at 10pt should
    outweigh a 20-character heading at 14pt. The body text font is
    the one used for the most characters overall.

    Returns None if no chunks have font size information.
    """
    # Count total characters at each font size (rounded to 1 decimal)
    size_chars: Counter[float] = Counter()
    for chunk in chunks:
        if chunk.font_size > 0 and chunk.text:
            rounded = round(chunk.font_size, 1)
            size_chars[rounded] += len(chunk.text)

    if not size_chars:
        return None

    # The body font is the size with the most total characters
    body_size = size_chars.most_common(1)[0][0]
    return body_size


def match_section_name(text: str) -> Optional[str]:
    """
    Check if text matches a known section heading pattern.

    Returns the normalized section name (e.g., 'methods', 'results')
    or None if no match.
    """
    cleaned = text.strip()
    # Section headings are typically short (< 60 chars)
    if len(cleaned) > 60:
        return None
    for pattern, name in SECTION_PATTERNS:
        if pattern.match(cleaned):
            return name
    return None


def classify_chunks(chunks: list[TextChunk]) -> tuple[list[TextChunk], str]:
    """
    Reclassify untagged TextChunks using font-size heuristics and
    text pattern matching.

    Args:
        chunks: List of TextChunk objects from pdf_parser (may have
                chunk_type='unknown' for untagged PDFs).

    Returns:
        (chunks, quality_tier) where:
        - chunks: same list with chunk_type and section_name updated
        - quality_tier: 'tagged', 'heuristic', or 'raw'

    The chunks list is mutated in place for efficiency (no copy needed
    since the batch runner doesn't reuse them).
    """
    if not chunks:
        return chunks, 'raw'

    # Check if already classified (tagged PDF)
    classified_count = sum(
        1 for c in chunks if c.chunk_type not in ('unknown', '')
    )
    total = len(chunks)
    if total > 0 and classified_count / total > 0.5:
        # More than half already classified → tagged PDF, leave as-is
        # Just add section_name annotations where possible
        _annotate_sections(chunks)
        return chunks, 'tagged'

    # Untagged PDF — apply heuristic classification
    body_size = detect_body_font_size(chunks)
    if body_size is None:
        # No font size info at all — can't do heuristic classification
        return chunks, 'raw'

    # Thresholds relative to body text size
    # Why these ratios: empirically, journal headings are 1.15-2x body size,
    # captions/footnotes are 0.7-0.9x body size.
    heading_threshold = body_size * 1.15
    small_threshold = body_size * 0.85

    # Track current section for annotation
    current_section: Optional[str] = None
    in_references = False

    for chunk in chunks:
        # Skip chunks that are already well-classified (from table detection etc.)
        if chunk.chunk_type not in ('unknown', ''):
            # Still annotate section if we can
            if current_section:
                chunk.properties['section_name'] = current_section
            continue

        text = chunk.text.strip()
        fs = chunk.font_size

        if not text:
            continue

        # Rule 1: Check for section heading by font size + pattern
        section = match_section_name(text)
        if section and fs >= body_size:
            chunk.chunk_type = 'heading'
            current_section = section
            chunk.properties['section_name'] = section
            if section == 'references':
                in_references = True
            else:
                in_references = False
            continue

        # Rule 2: Large font → heading (even without pattern match)
        if fs > heading_threshold and len(text) < 200:
            chunk.chunk_type = 'heading'
            # Try to detect section from text content
            section = match_section_name(text)
            if section:
                current_section = section
                chunk.properties['section_name'] = section
                if section == 'references':
                    in_references = True
                else:
                    in_references = False
            elif current_section:
                chunk.properties['section_name'] = current_section
            continue

        # Rule 3: Table/figure captions
        if RE_TABLE_CAPTION.match(text):
            chunk.chunk_type = 'table_caption'
            if current_section:
                chunk.properties['section_name'] = current_section
            continue

        if RE_FIGURE_CAPTION.match(text):
            chunk.chunk_type = 'caption'
            if current_section:
                chunk.properties['section_name'] = current_section
            continue

        # Rule 4: Inside references section → reference entries
        if in_references:
            chunk.chunk_type = 'reference'
            chunk.properties['section_name'] = 'references'
            continue

        # Rule 5: Body-sized text → paragraph
        if fs >= small_threshold:
            chunk.chunk_type = 'paragraph'
            if current_section:
                chunk.properties['section_name'] = current_section
            continue

        # Rule 6: Small text → could be footnote, caption, or page number
        if fs > 0 and fs < small_threshold:
            if len(text) < 10:
                # Very short small text → likely page number or label
                chunk.chunk_type = 'artifact'
            else:
                # Longer small text → footnote or caption
                chunk.chunk_type = 'caption'
            if current_section:
                chunk.properties['section_name'] = current_section
            continue

        # Fallback: classify as paragraph if we have body size context
        chunk.chunk_type = 'paragraph'
        if current_section:
            chunk.properties['section_name'] = current_section

    return chunks, 'heuristic'


def _annotate_sections(chunks: list[TextChunk]):
    """
    For already-classified (tagged) chunks, add section_name annotations
    based on heading text pattern matching.

    This enriches tagged PDFs with section context (e.g., knowing that
    a paragraph belongs to the 'methods' section) without changing the
    existing chunk_type classification.
    """
    current_section: Optional[str] = None

    for chunk in chunks:
        if chunk.chunk_type == 'heading':
            section = match_section_name(chunk.text)
            if section:
                current_section = section

        if current_section:
            chunk.properties['section_name'] = current_section
