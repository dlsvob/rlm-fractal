# fractal.parser — PDF parsing pipeline for the fractal graph system.
#
# This package extracts structured text from scientific journal PDFs.
# Ported from pdf-text/parse_pdf.py and hardened for batch processing
# of diverse, heterogeneous PDFs (tagged and untagged, various encodings).
#
# Submodules:
#   pdf_parser.py          — Core parser (object graph, content streams, text extraction)
#   structure_classifier.py — Heuristic section classification for untagged PDFs (Phase 2b)
#   table_extractor.py      — Generalized table detection (Phase 2d)
#   batch_parse.py          — Parallel batch runner (Phase 2c)
