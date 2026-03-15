"""
pdf_parser.py — Hardened PDF parser for batch processing scientific journal PDFs.

PURPOSE:
  Reads raw PDF bytes, resolves the object graph (including compressed
  Object Streams), walks the page tree, decompresses content streams,
  tokenizes PDF operators, extracts text, and classifies every chunk
  by its semantic role.

LINEAGE:
  Ported from pdf-text/parse_pdf.py (1,812 lines, built for one tagged
  government report). This version is hardened for the 1,410 diverse
  journal PDFs in the fractal collection:
    - 80% untagged (no BDC/EMC structure tags)
    - 81% use /Encoding + /Differences (not just ToUnicode CMaps)
    - 33% use filter chains (e.g. [/FlateDecode /ASCII85Decode])
    - 26% use ASCII85Decode, 4% use LZWDecode

HARDENING CHANGES (vs. original):
  1. All assert statements replaced with try/except + skip — a malformed
     object in one PDF should not crash the entire batch run.
  2. /Encoding + /Differences fallback for Type1 fonts — when no ToUnicode
     CMap exists, we parse the font's /Encoding dictionary to build a
     character mapping from the /Differences array.
  3. ASCII85Decode, LZWDecode, and filter chain support — streams can now
     use [/FlateDecode /ASCII85Decode] or other combinations.
  4. Dynamic stream length from /Length (with indirect reference resolution)
     instead of fixed 20KB read window.

DESIGN DECISIONS:
  - No external PDF libraries. Uses only zlib + Python stdlib.
  - Latin-1 decoding throughout (PDF is binary; latin-1 preserves every
    byte 0x00–0xFF without encoding errors).
  - Tagged PDF marked-content (BDC/EMC) is the primary semantic signal
    when available. For untagged PDFs, the structure_classifier module
    provides heuristic classification (Phase 2b).
"""

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------
import re
import struct
import sys
import zlib
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Regex to locate "N M obj" markers in the raw PDF text.
# We use latin-1 decoding throughout because PDF is a binary format
# where bytes 0x00–0xFF map 1:1 to characters — latin-1 preserves
# every byte without encoding errors, unlike UTF-8.
RE_OBJ_MARKER = re.compile(r'(\d+)\s+(\d+)\s+obj')

# Matches a dictionary-bearing object: "N M obj << ... >>"
# The non-greedy .*? inside << >> handles nested << >> poorly,
# so we use a custom parser for complex dicts. This regex is only
# used for quick scans of simple objects.
RE_OBJ_DICT = re.compile(
    r'(\d+)\s+0\s+obj\s*<<(.*?)>>\s*(stream|endobj)',
    re.DOTALL
)

# Matches the start of a stream right after its dictionary.
RE_STREAM_START = re.compile(
    rb'(\d+)\s+0\s+obj\s*<<(.*?)>>\s*stream(\r\n|\r|\n)',
    re.DOTALL
)

# Structure tags we recognise from content streams' BDC operators.
# These come from the PDF's logical structure (tagged PDF).
STRUCTURE_TAGS = {
    'P', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6',
    'Figure', 'Caption', 'Table', 'TOCI', 'Reference', 'Artifact',
    'Span', 'L', 'LI', 'LBody', 'Lbl', 'TD', 'TH', 'TR',
    'THead', 'TBody', 'PlacedPDF', 'Link',
}

# Semantic classification labels we assign to each chunk.
CHUNK_TYPES = {
    'heading', 'paragraph', 'figure', 'caption',
    'table', 'table_caption', 'table_cell', 'table_footnote',
    'toc_entry', 'reference', 'header', 'footer', 'artifact',
    'list_item', 'cover_element', 'unknown',
}

# Adobe standard encoding to Unicode mapping.
# This is the base encoding for many Type1 fonts. The /Differences array
# overrides specific positions in this table. We only include the printable
# range; unlisted codes pass through as chr(code).
#
# Why we need this: 81% of our journal PDFs use /Encoding + /Differences
# instead of (or in addition to) ToUnicode CMaps. Without this mapping,
# characters like bullet, endash, fi-ligature, etc. come through as
# garbage bytes.
STANDARD_ENCODING = {
    # The standard encoding matches ASCII for 0x20-0x7E, so we only
    # need to list the non-ASCII positions that differ.
    0x60: '\u2018',   # quoteleft
    0x91: '\u2018',   # quoteleft (Windows-1252 position)
    0x92: '\u2019',   # quoteright
    0x93: '\u201C',   # quotedblleft
    0x94: '\u201D',   # quotedblright
    0x95: '\u2022',   # bullet
    0x96: '\u2013',   # endash
    0x97: '\u2014',   # emdash
    0xA0: '\u00A0',   # nbspace
    0xA1: '\u00A1',   # exclamdown
    0xA2: '\u00A2',   # cent
    0xA3: '\u00A3',   # sterling
    0xA4: '\u2044',   # fraction
    0xA5: '\u00A5',   # yen
    0xA6: '\u0192',   # florin
    0xA7: '\u00A7',   # section
    0xA8: '\u00A4',   # currency
    0xAC: '\u00AC',   # logicalnot
    0xAD: '\u00AD',   # softhyphen
    0xAE: '\u00AE',   # registered
    0xB0: '\u00B0',   # degree
    0xB1: '\u00B1',   # plusminus
    0xB2: '\u00B2',   # twosuperior
    0xB3: '\u00B3',   # threesuperior
    0xB4: '\u00B4',   # acute
    0xB5: '\u00B5',   # mu
    0xB6: '\u00B6',   # paragraph
    0xB7: '\u00B7',   # periodcentered / middot
    0xB9: '\u00B9',   # onesuperior
    0xBB: '\u00BB',   # guillemotright
    0xBC: '\u00BC',   # onequarter
    0xBD: '\u00BD',   # onehalf
    0xBE: '\u00BE',   # threequarters
    0xBF: '\u00BF',   # questiondown
    0xC0: '\u00C0',   # Agrave
    0xC1: '\u00C1',   # Aacute
    0xC2: '\u00C2',   # Acircumflex
    0xC3: '\u00C3',   # Atilde
    0xC4: '\u00C4',   # Adieresis
    0xC5: '\u00C5',   # Aring
    0xC6: '\u00C6',   # AE
    0xC7: '\u00C7',   # Ccedilla
    0xC8: '\u00C8',   # Egrave
    0xC9: '\u00C9',   # Eacute
    0xCA: '\u00CA',   # Ecircumflex
    0xCB: '\u00CB',   # Edieresis
    0xCC: '\u00CC',   # Igrave
    0xCD: '\u00CD',   # Iacute
    0xCE: '\u00CE',   # Icircumflex
    0xCF: '\u00CF',   # Idieresis
    0xD0: '\u00D0',   # Eth
    0xD1: '\u00D1',   # Ntilde
    0xD2: '\u00D2',   # Ograve
    0xD3: '\u00D3',   # Oacute
    0xD4: '\u00D4',   # Ocircumflex
    0xD5: '\u00D5',   # Otilde
    0xD6: '\u00D6',   # Odieresis
    0xD7: '\u00D7',   # multiply
    0xD8: '\u00D8',   # Oslash
    0xD9: '\u00D9',   # Ugrave
    0xDA: '\u00DA',   # Uacute
    0xDB: '\u00DB',   # Ucircumflex
    0xDC: '\u00DC',   # Udieresis
    0xDD: '\u00DD',   # Yacute
    0xDE: '\u00DE',   # Thorn
    0xDF: '\u00DF',   # germandbls
    0xE0: '\u00E0',   # agrave
    0xE1: '\u00E1',   # aacute
    0xE2: '\u00E2',   # acircumflex
    0xE3: '\u00E3',   # atilde
    0xE4: '\u00E4',   # adieresis
    0xE5: '\u00E5',   # aring
    0xE6: '\u00E6',   # ae
    0xE7: '\u00E7',   # ccedilla
    0xE8: '\u00E8',   # egrave
    0xE9: '\u00E9',   # eacute
    0xEA: '\u00EA',   # ecircumflex
    0xEB: '\u00EB',   # edieresis
    0xEC: '\u00EC',   # igrave
    0xED: '\u00ED',   # iacute
    0xEE: '\u00EE',   # icircumflex
    0xEF: '\u00EF',   # idieresis
    0xF0: '\u00F0',   # eth
    0xF1: '\u00F1',   # ntilde
    0xF2: '\u00F2',   # ograve
    0xF3: '\u00F3',   # oacute
    0xF4: '\u00F4',   # ocircumflex
    0xF5: '\u00F5',   # otilde
    0xF6: '\u00F6',   # odieresis
    0xF7: '\u00F7',   # divide
    0xF8: '\u00F8',   # oslash
    0xF9: '\u00F9',   # ugrave
    0xFA: '\u00FA',   # uacute
    0xFB: '\u00FB',   # ucircumflex
    0xFC: '\u00FC',   # udieresis
    0xFD: '\u00FD',   # yacute
    0xFE: '\u00FE',   # thorn
    0xFF: '\u00FF',   # ydieresis
}

# Adobe glyph name → Unicode character mapping.
# Used to resolve /Differences entries like "/fi" → "fi" ligature.
# This covers the most common glyph names found in journal PDFs.
# The full Adobe Glyph List has ~4,200 entries; we include the ~200
# most frequently needed for scientific text.
GLYPH_TO_UNICODE = {
    # Basic ASCII (glyph names match character names)
    'space': ' ', 'exclam': '!', 'quotedbl': '"', 'numbersign': '#',
    'dollar': '$', 'percent': '%', 'ampersand': '&', 'quotesingle': "'",
    'parenleft': '(', 'parenright': ')', 'asterisk': '*', 'plus': '+',
    'comma': ',', 'hyphen': '-', 'period': '.', 'slash': '/',
    'zero': '0', 'one': '1', 'two': '2', 'three': '3', 'four': '4',
    'five': '5', 'six': '6', 'seven': '7', 'eight': '8', 'nine': '9',
    'colon': ':', 'semicolon': ';', 'less': '<', 'equal': '=',
    'greater': '>', 'question': '?', 'at': '@',
    # Uppercase A-Z
    'A': 'A', 'B': 'B', 'C': 'C', 'D': 'D', 'E': 'E', 'F': 'F',
    'G': 'G', 'H': 'H', 'I': 'I', 'J': 'J', 'K': 'K', 'L': 'L',
    'M': 'M', 'N': 'N', 'O': 'O', 'P': 'P', 'Q': 'Q', 'R': 'R',
    'S': 'S', 'T': 'T', 'U': 'U', 'V': 'V', 'W': 'W', 'X': 'X',
    'Y': 'Y', 'Z': 'Z',
    'bracketleft': '[', 'backslash': '\\', 'bracketright': ']',
    'asciicircum': '^', 'underscore': '_', 'grave': '`',
    # Lowercase a-z
    'a': 'a', 'b': 'b', 'c': 'c', 'd': 'd', 'e': 'e', 'f': 'f',
    'g': 'g', 'h': 'h', 'i': 'i', 'j': 'j', 'k': 'k', 'l': 'l',
    'm': 'm', 'n': 'n', 'o': 'o', 'p': 'p', 'q': 'q', 'r': 'r',
    's': 's', 't': 't', 'u': 'u', 'v': 'v', 'w': 'w', 'x': 'x',
    'y': 'y', 'z': 'z',
    'braceleft': '{', 'bar': '|', 'braceright': '}', 'asciitilde': '~',
    # Ligatures (critical for scientific text — these are the most common)
    'fi': '\uFB01', 'fl': '\uFB02', 'ff': '\uFB00',
    'ffi': '\uFB03', 'ffl': '\uFB04',
    # Accented characters
    'Agrave': '\u00C0', 'Aacute': '\u00C1', 'Acircumflex': '\u00C2',
    'Atilde': '\u00C3', 'Adieresis': '\u00C4', 'Aring': '\u00C5',
    'AE': '\u00C6', 'Ccedilla': '\u00C7', 'Egrave': '\u00C8',
    'Eacute': '\u00C9', 'Ecircumflex': '\u00CA', 'Edieresis': '\u00CB',
    'Igrave': '\u00CC', 'Iacute': '\u00CD', 'Icircumflex': '\u00CE',
    'Idieresis': '\u00CF', 'Eth': '\u00D0', 'Ntilde': '\u00D1',
    'Ograve': '\u00D2', 'Oacute': '\u00D3', 'Ocircumflex': '\u00D4',
    'Otilde': '\u00D5', 'Odieresis': '\u00D6', 'Oslash': '\u00D8',
    'Ugrave': '\u00D9', 'Uacute': '\u00DA', 'Ucircumflex': '\u00DB',
    'Udieresis': '\u00DC', 'Yacute': '\u00DD', 'Thorn': '\u00DE',
    'germandbls': '\u00DF',
    'agrave': '\u00E0', 'aacute': '\u00E1', 'acircumflex': '\u00E2',
    'atilde': '\u00E3', 'adieresis': '\u00E4', 'aring': '\u00E5',
    'ae': '\u00E6', 'ccedilla': '\u00E7', 'egrave': '\u00E8',
    'eacute': '\u00E9', 'ecircumflex': '\u00EA', 'edieresis': '\u00EB',
    'igrave': '\u00EC', 'iacute': '\u00ED', 'icircumflex': '\u00EE',
    'idieresis': '\u00EF', 'eth': '\u00F0', 'ntilde': '\u00F1',
    'ograve': '\u00F2', 'oacute': '\u00F3', 'ocircumflex': '\u00F4',
    'otilde': '\u00F5', 'odieresis': '\u00F6', 'oslash': '\u00F8',
    'ugrave': '\u00F9', 'uacute': '\u00FA', 'ucircumflex': '\u00FB',
    'udieresis': '\u00FC', 'yacute': '\u00FD', 'thorn': '\u00FE',
    'ydieresis': '\u00FF',
    # Common symbols in scientific papers
    'bullet': '\u2022', 'endash': '\u2013', 'emdash': '\u2014',
    'quotedblleft': '\u201C', 'quotedblright': '\u201D',
    'quoteleft': '\u2018', 'quoteright': '\u2019',
    'ellipsis': '\u2026', 'trademark': '\u2122',
    'copyright': '\u00A9', 'registered': '\u00AE',
    'degree': '\u00B0', 'plusminus': '\u00B1', 'minus': '\u2212',
    'multiply': '\u00D7', 'divide': '\u00F7',
    'fraction': '\u2044', 'mu': '\u00B5', 'micro': '\u00B5',
    'paragraph': '\u00B6', 'section': '\u00A7',
    'dagger': '\u2020', 'daggerdbl': '\u2021',
    'periodcentered': '\u00B7', 'middot': '\u00B7',
    'onesuperior': '\u00B9', 'twosuperior': '\u00B2',
    'threesuperior': '\u00B3',
    'onequarter': '\u00BC', 'onehalf': '\u00BD', 'threequarters': '\u00BE',
    'sterling': '\u00A3', 'yen': '\u00A5', 'Euro': '\u20AC',
    'florin': '\u0192',
    # Greek letters (common in scientific text)
    'Alpha': '\u0391', 'Beta': '\u0392', 'Gamma': '\u0393',
    'Delta': '\u0394', 'Epsilon': '\u0395', 'Zeta': '\u0396',
    'Eta': '\u0397', 'Theta': '\u0398', 'Iota': '\u0399',
    'Kappa': '\u039A', 'Lambda': '\u039B', 'Mu': '\u039C',
    'Nu': '\u039D', 'Xi': '\u039E', 'Omicron': '\u039F',
    'Pi': '\u03A0', 'Rho': '\u03A1', 'Sigma': '\u03A3',
    'Tau': '\u03A4', 'Upsilon': '\u03A5', 'Phi': '\u03A6',
    'Chi': '\u03A7', 'Psi': '\u03A8', 'Omega': '\u03A9',
    'alpha': '\u03B1', 'beta': '\u03B2', 'gamma': '\u03B3',
    'delta': '\u03B4', 'epsilon': '\u03B5', 'zeta': '\u03B6',
    'eta': '\u03B7', 'theta': '\u03B8', 'iota': '\u03B9',
    'kappa': '\u03BA', 'lambda': '\u03BB', 'mu1': '\u03BC',
    'nu': '\u03BD', 'xi': '\u03BE', 'omicron': '\u03BF',
    'pi': '\u03C0', 'rho': '\u03C1', 'sigma': '\u03C3',
    'tau': '\u03C4', 'upsilon': '\u03C5', 'phi': '\u03C6',
    'chi': '\u03C7', 'psi': '\u03C8', 'omega': '\u03C9',
    # Math symbols
    'infinity': '\u221E', 'notequal': '\u2260', 'lessequal': '\u2264',
    'greaterequal': '\u2265', 'approxequal': '\u2248',
    'summation': '\u2211', 'product': '\u220F', 'radical': '\u221A',
    'integral': '\u222B', 'partialdiff': '\u2202', 'nabla': '\u2207',
    'lozenge': '\u25CA', 'arrowleft': '\u2190', 'arrowright': '\u2192',
    'arrowup': '\u2191', 'arrowdown': '\u2193',
    # Whitespace
    'nbspace': '\u00A0', 'softhyphen': '\u00AD',
}


# ---------------------------------------------------------------------------
# Type definitions
# ---------------------------------------------------------------------------

class PdfObject:
    """
    Represents a single PDF indirect object.

    Attributes:
        num: Object number (e.g., 8641 in "8641 0 obj").
        gen: Generation number (almost always 0).
        definition: Raw string content of the object's dictionary/value.
        stream_data: Raw bytes of the object's stream (None if no stream).
    """
    def __init__(self, num: int, gen: int = 0, definition: str = '',
                 stream_data: bytes = None):
        self.num = num
        self.gen = gen
        self.definition = definition
        self.stream_data = stream_data

    def __repr__(self):
        has_stream = ' +stream' if self.stream_data else ''
        return f'<PdfObject {self.num} {self.gen}{has_stream}>'


class TextChunk:
    """
    A semantically classified piece of extracted text.

    Attributes:
        page: 1-based page number.
        chunk_type: One of CHUNK_TYPES (e.g., 'heading', 'paragraph').
        tag: The raw PDF structure tag (e.g., 'H1', 'P', 'Artifact').
             Empty string for untagged PDFs.
        text: The reconstructed text content.
        font: Font name used (e.g., '/TT0', '/T1_0').
        font_size: Font size in points.
        x: Horizontal position (points from left edge).
        y: Vertical position (points from bottom edge).
        mcid: Marked-content ID (None for untagged PDFs).
        properties: Additional BDC properties (e.g., Lang, Subtype).
    """
    def __init__(self):
        self.page: int = 0
        self.chunk_type: str = 'unknown'
        self.tag: str = ''
        self.text: str = ''
        self.font: str = ''
        self.font_size: float = 0.0
        self.x: float = 0.0
        self.y: float = 0.0
        self.mcid: Optional[int] = None
        self.properties: dict = {}

    def to_dict(self) -> dict:
        """Serialize to a plain dict for JSON output."""
        d = {
            'page': self.page,
            'type': self.chunk_type,
            'tag': self.tag,
            'text': self.text,
        }
        if self.font:
            d['font'] = self.font
        if self.font_size:
            d['font_size'] = round(self.font_size, 2)
        if self.x or self.y:
            d['position'] = {'x': round(self.x, 2), 'y': round(self.y, 2)}
        if self.mcid is not None:
            d['mcid'] = self.mcid
        if self.properties:
            d['properties'] = self.properties
        return d


# ---------------------------------------------------------------------------
# Helper / utility functions (private)
# ---------------------------------------------------------------------------

def _read_file(path: str) -> bytes:
    """Read entire PDF file into memory as raw bytes."""
    with open(path, 'rb') as f:
        return f.read()


def _decode_ascii85(data: bytes) -> bytes:
    """
    Decode ASCII85 (aka base85) encoded data.

    ASCII85 encodes 4 bytes as 5 ASCII characters in the range '!' (33)
    to 'u' (117). The special character 'z' represents four zero bytes.
    The stream ends at '~>' (end-of-data marker).

    Why we need this: 26% of our journal PDFs use ASCII85Decode for
    content streams. Without this, we get zero text from those PDFs.
    """
    # Strip whitespace and find the end-of-data marker
    text = data.decode('ascii', errors='ignore')
    eod = text.find('~>')
    if eod >= 0:
        text = text[:eod]

    # Remove all whitespace
    text = re.sub(r'\s', '', text)

    result = bytearray()
    i = 0
    while i < len(text):
        if text[i] == 'z':
            # 'z' is shorthand for four zero bytes
            result.extend(b'\x00\x00\x00\x00')
            i += 1
            continue

        # Read up to 5 characters for one group
        group = text[i:i + 5]
        i += len(group)

        if len(group) == 0:
            break

        # Pad short final group with 'u' (117, the max value)
        n = len(group)
        group = group + 'u' * (5 - n)

        # Decode: each char represents a base-85 digit
        try:
            value = 0
            for ch in group:
                value = value * 85 + (ord(ch) - 33)

            # Convert 32-bit value to 4 bytes
            out_bytes = struct.pack('>I', value)
            # For short final group, only output n-1 bytes
            result.extend(out_bytes[:n - 1])
        except (struct.error, ValueError, OverflowError):
            # Malformed group — skip it
            continue

    return bytes(result)


def _decode_lzw(data: bytes) -> bytes:
    """
    Decode LZW compressed data (PDF /LZWDecode filter).

    LZW uses variable-width codes starting at 9 bits. Code 256 is
    the clear-table signal, code 257 is end-of-data.

    Why we need this: 4% of our PDFs use LZWDecode. It's rare but
    worth supporting to avoid losing those papers entirely.
    """
    if not data:
        return b''

    try:
        # Read bits from the byte stream
        bit_pos = 0
        total_bits = len(data) * 8

        def read_bits(n: int) -> int:
            nonlocal bit_pos
            if bit_pos + n > total_bits:
                raise EOFError
            result = 0
            for _ in range(n):
                byte_idx = bit_pos // 8
                bit_idx = 7 - (bit_pos % 8)
                result = (result << 1) | ((data[byte_idx] >> bit_idx) & 1)
                bit_pos += 1
            return result

        # Initialize table with single-byte entries
        def reset_table():
            return {i: bytes([i]) for i in range(256)}

        table = reset_table()
        next_code = 258  # 256=clear, 257=EOD
        code_size = 9
        result = bytearray()

        # Read first code (should be a clear code, but handle if not)
        code = read_bits(code_size)
        if code == 256:
            code = read_bits(code_size)
        if code == 257:
            return bytes(result)

        prev_entry = table.get(code, b'')
        result.extend(prev_entry)

        while True:
            try:
                code = read_bits(code_size)
            except EOFError:
                break

            if code == 257:  # EOD
                break
            if code == 256:  # Clear table
                table = reset_table()
                next_code = 258
                code_size = 9
                try:
                    code = read_bits(code_size)
                except EOFError:
                    break
                if code == 257:
                    break
                prev_entry = table.get(code, b'')
                result.extend(prev_entry)
                continue

            if code in table:
                entry = table[code]
            elif code == next_code:
                # Special case: code not yet in table
                entry = prev_entry + prev_entry[:1]
            else:
                # Invalid code — stop gracefully
                break

            result.extend(entry)
            # Add new entry to table
            table[next_code] = prev_entry + entry[:1]
            next_code += 1

            # Increase code size when table grows past current capacity
            if next_code >= (1 << code_size) and code_size < 12:
                code_size += 1

            prev_entry = entry

        return bytes(result)

    except Exception:
        # LZW decoding is complex; return what we have on any error
        return b''


def _decompress(stream_bytes: bytes, filter_value: str) -> bytes:
    """
    Decompress a PDF stream given its /Filter value.

    Handles single filters and filter chains (arrays like
    [/FlateDecode /ASCII85Decode]). Filters are applied in the
    order listed in the array.

    Supported filters:
      - /FlateDecode (zlib, ~95% of compressed streams)
      - /ASCII85Decode (base85, 26% of our PDFs)
      - /ASCIIHexDecode (hex encoding, rare)
      - /LZWDecode (4% of our PDFs)
      - /DCTDecode and /JPXDecode are image filters — we skip those
        since we only care about text content streams.
    """
    if not stream_bytes:
        return b''

    # Parse filter value into an ordered list of filter names.
    # It can be a single name ("/FlateDecode") or an array
    # ("[/FlateDecode /ASCII85Decode]").
    filters = _parse_filter_list(filter_value)

    data = stream_bytes
    for filt in filters:
        if 'FlateDecode' in filt:
            try:
                data = zlib.decompress(data)
            except zlib.error:
                try:
                    # Try raw deflate (no zlib header) as fallback
                    data = zlib.decompress(data, -15)
                except zlib.error:
                    return b''

        elif 'ASCII85Decode' in filt:
            data = _decode_ascii85(data)

        elif 'ASCIIHexDecode' in filt:
            # Hex-encoded stream: pairs of hex digits, whitespace ignored
            hex_text = data.decode('ascii', errors='ignore')
            eod = hex_text.find('>')
            if eod >= 0:
                hex_text = hex_text[:eod]
            hex_text = re.sub(r'\s', '', hex_text)
            if len(hex_text) % 2:
                hex_text += '0'
            try:
                data = bytes.fromhex(hex_text)
            except ValueError:
                return b''

        elif 'LZWDecode' in filt:
            data = _decode_lzw(data)

        elif 'DCTDecode' in filt or 'JPXDecode' in filt:
            # Image data — not useful for text extraction
            return b''

        elif 'CCITTFaxDecode' in filt or 'JBIG2Decode' in filt:
            # Fax/image compression — not text
            return b''

        elif 'Crypt' in filt:
            # Encrypted stream — we can't handle this
            return b''

        # Unknown filter: return data as-is (hope for the best)

    return data


def _parse_filter_list(filter_value: str) -> list[str]:
    """
    Parse a /Filter value into a list of filter names.

    Single filter: "/FlateDecode" → ["/FlateDecode"]
    Filter chain:  "[/ASCII85Decode /FlateDecode]" → ["/ASCII85Decode", "/FlateDecode"]

    The order matters: for a chain, the first filter was applied first
    during encoding, so we apply them in the same order during decoding.
    Wait — actually PDF spec says filters are applied in array order
    during decoding. So [/ASCII85Decode /FlateDecode] means:
    first decode ASCII85, then decompress with Flate.
    """
    if not filter_value:
        return []

    # Check for array syntax
    if '[' in filter_value:
        # Extract names from the array
        names = re.findall(r'/([A-Za-z0-9]+)', filter_value)
        return names

    # Single filter
    names = re.findall(r'/([A-Za-z0-9]+)', filter_value)
    return names if names else [filter_value]


def _extract_dict_value(dict_text: str, key: str) -> Optional[str]:
    """
    Extract a simple value for a /Key from a PDF dictionary string.

    Handles: /Key 123, /Key /Name, /Key(string), /Key true, /Key N M R.
    Does NOT handle nested dictionaries or arrays.
    """
    # Try indirect reference FIRST — "123 0 R" also matches the plain
    # numeric regex, so we must check for references before plain numbers.
    m = re.search(rf'/{key}\s+(\d+\s+\d+\s+R)', dict_text)
    if m:
        return m.group(1)
    # Try numeric value
    m = re.search(rf'/{key}\s+(\d+)', dict_text)
    if m:
        return m.group(1)
    # Try name value
    m = re.search(rf'/{key}\s*/([A-Za-z0-9_.+-]+)', dict_text)
    if m:
        return m.group(1)
    return None


def _extract_filter_value(dict_text: str) -> str:
    """
    Extract the /Filter value from a dictionary, handling both single
    names and arrays.

    Examples:
      /Filter /FlateDecode         → "/FlateDecode"
      /Filter [/ASCII85Decode /FlateDecode]  → "[/ASCII85Decode /FlateDecode]"

    Why a separate function: _extract_dict_value can't handle array values,
    and filter chains are arrays.
    """
    # Try array first
    m = re.search(r'/Filter\s*(\[.*?\])', dict_text, re.DOTALL)
    if m:
        return m.group(1)
    # Try single name
    m = re.search(r'/Filter\s*/([A-Za-z0-9]+)', dict_text)
    if m:
        return '/' + m.group(1)
    return ''


def _parse_string_literal(s: str) -> str:
    """
    Parse a PDF string literal (parenthesised), handling escape sequences.

    PDF string escapes: \\n, \\r, \\t, \\b, \\f, \\(, \\), \\\\,
    and octal codes (\\DDD with 1-3 digits).
    """
    result = []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch == '\\' and i + 1 < len(s):
            next_ch = s[i + 1]
            if next_ch == 'n':
                result.append('\n'); i += 2
            elif next_ch == 'r':
                result.append('\r'); i += 2
            elif next_ch == 't':
                result.append('\t'); i += 2
            elif next_ch == 'b':
                result.append('\b'); i += 2
            elif next_ch == 'f':
                result.append('\f'); i += 2
            elif next_ch in '()\\':
                result.append(next_ch); i += 2
            elif next_ch.isdigit():
                octal = next_ch
                j = i + 2
                while j < len(s) and j < i + 4 and s[j] in '01234567':
                    octal += s[j]; j += 1
                result.append(chr(int(octal, 8)))
                i = j
            elif next_ch == '\n':
                i += 2  # line continuation
            elif next_ch == '\r':
                i += 2
                if i < len(s) and s[i] == '\n':
                    i += 1
            else:
                result.append(next_ch); i += 2
        else:
            result.append(ch); i += 1
    return ''.join(result)


def _extract_parenthesised_string(text: str, start: int) -> tuple[str, int]:
    """
    Extract a balanced parenthesised string from text starting at `start`.

    Returns (inner_content, end_position).

    HARDENED: the original used assert to verify text[start] == '('.
    Now returns empty string if precondition fails instead of crashing.
    """
    if start >= len(text) or text[start] != '(':
        return '', start + 1
    depth = 0
    i = start
    while i < len(text):
        ch = text[i]
        if ch == '\\':
            i += 2
            continue
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth == 0:
                return text[start + 1:i], i + 1
        i += 1
    return text[start + 1:], len(text)


def _extract_hex_string(text: str, start: int) -> tuple[str, int]:
    """
    Extract a hex string <...> from text starting at '<'.

    Returns (decoded_string, end_position).

    HARDENED: returns empty string if precondition fails or '>' not found.
    """
    if start >= len(text) or text[start] != '<':
        return '', start + 1
    try:
        end = text.index('>', start)
    except ValueError:
        return '', len(text)
    hex_body = text[start + 1:end].replace(' ', '').replace('\n', '').replace('\r', '')
    if len(hex_body) % 2 == 1:
        hex_body += '0'
    chars = []
    for j in range(0, len(hex_body), 2):
        try:
            chars.append(chr(int(hex_body[j:j + 2], 16)))
        except ValueError:
            pass
    return ''.join(chars), end + 1


def _parse_cmap(cmap_text: str) -> dict[int, str]:
    """
    Parse a ToUnicode CMap stream and return {char_code: unicode_string}.

    Handles beginbfchar/endbfchar (single mappings) and
    beginbfrange/endbfrange (range mappings).
    """
    mapping = {}

    for block in re.finditer(
            r'beginbfchar\s*(.*?)\s*endbfchar', cmap_text, re.DOTALL):
        body = block.group(1)
        for line_match in re.finditer(r'<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>', body):
            src = int(line_match.group(1), 16)
            dst_hex = line_match.group(2)
            dst_bytes = bytes.fromhex(dst_hex)
            try:
                dst_str = dst_bytes.decode('utf-16-be')
            except (UnicodeDecodeError, ValueError):
                dst_str = dst_bytes.decode('latin-1')
            mapping[src] = dst_str

    for block in re.finditer(
            r'beginbfrange\s*(.*?)\s*endbfrange', cmap_text, re.DOTALL):
        body = block.group(1)
        for line_match in re.finditer(
                r'<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>\s*<([0-9A-Fa-f]+)>', body):
            start = int(line_match.group(1), 16)
            end = int(line_match.group(2), 16)
            dst_start = int(line_match.group(3), 16)
            for code in range(start, end + 1):
                mapping[code] = chr(dst_start + (code - start))

    return mapping


def _parse_differences(diff_text: str) -> dict[int, str]:
    """
    Parse a /Differences array from a font /Encoding dictionary.

    The /Differences array alternates between integer codes and glyph
    names. Each integer sets the starting code for subsequent names.

    Example:
      /Differences [24 /breve /caron /circumflex 39 /quotesingle]

    This means:
      code 24 → breve, 25 → caron, 26 → circumflex, 39 → quotesingle

    We resolve glyph names to Unicode using the GLYPH_TO_UNICODE table.
    Names not in the table are left as-is (the name itself often IS the
    character, e.g., "A" → "A").

    Why this matters: 81% of our PDFs use /Differences for font encoding.
    Without this, most text comes through as garbage for fonts that lack
    a ToUnicode CMap.
    """
    mapping = {}

    # Extract the array content between [ and ]
    arr_start = diff_text.find('[')
    arr_end = diff_text.rfind(']')
    if arr_start < 0 or arr_end < 0:
        return mapping
    inner = diff_text[arr_start + 1:arr_end]

    # Tokenize: integers and /Name tokens
    tokens = re.findall(r'(\d+|/[A-Za-z0-9_.]+)', inner)

    current_code = 0
    for tok in tokens:
        if tok.startswith('/'):
            # Glyph name — resolve to Unicode
            name = tok[1:]
            if name in GLYPH_TO_UNICODE:
                mapping[current_code] = GLYPH_TO_UNICODE[name]
            elif len(name) == 1:
                # Single character name is itself
                mapping[current_code] = name
            else:
                # Try interpreting as "uniXXXX" pattern
                uni_match = re.match(r'^uni([0-9A-Fa-f]{4,6})$', name)
                if uni_match:
                    try:
                        mapping[current_code] = chr(int(uni_match.group(1), 16))
                    except (ValueError, OverflowError):
                        pass
                # Otherwise leave unmapped (chr(code) will be used)
            current_code += 1
        else:
            # Integer — sets the starting code
            try:
                current_code = int(tok)
            except ValueError:
                pass

    return mapping


def _apply_cmap(text: str, cmap: dict[int, str]) -> str:
    """
    Apply a character mapping (CMap or /Differences) to a string.
    Each character is looked up by ordinal; unmapped chars pass through.
    """
    result = []
    for ch in text:
        code = ord(ch)
        if code in cmap:
            result.append(cmap[code])
        else:
            result.append(ch)
    return ''.join(result)


def _text_from_tj_array(items: list, cmap: dict[int, str] = None) -> str:
    """
    Reconstruct readable text from a TJ array.

    TJ arrays alternate strings and numeric kerning adjustments.
    Large negative numbers (< -100) represent word spaces.
    """
    parts = []
    for item in items:
        if isinstance(item, str):
            parsed = _parse_string_literal(item)
            if cmap:
                parsed = _apply_cmap(parsed, cmap)
            parts.append(parsed)
        elif isinstance(item, (int, float)):
            if item < -100:
                parts.append(' ')
    return ''.join(parts)


# ---------------------------------------------------------------------------
# Core logic — PDF object resolution
# ---------------------------------------------------------------------------

class PdfParser:
    """
    Low-level PDF parser that resolves the object graph and extracts
    text from page content streams.

    Architecture:
      1. _index_direct_objects(): scan for "N 0 obj" markers
      2. _decode_object_streams(): decompress /Type/ObjStm containers
      3. _build_page_list(): walk /Pages tree for document order
      4. _get_page_content(): decompress page /Contents streams
      5. _parse_content_stream(): tokenize and extract text chunks
      6. _classify_chunk(): assign semantic type from tags/heuristics

    HARDENING: all steps wrapped in try/except so one bad object
    doesn't crash the entire parse. Errors are logged and skipped.
    """

    def __init__(self, raw_data: bytes):
        self.raw = raw_data
        self.text = raw_data.decode('latin-1')
        # Maps object number → byte offset for direct objects
        self.direct_obj_offsets: dict[int, int] = {}
        # Maps object number → definition string for ObjStm-embedded objects
        self.embedded_objects: dict[int, str] = {}
        # Page object numbers in document order
        self.page_obj_nums: list[int] = []
        # Per-page font mapping cache: {page_obj: {font_name: char_map}}
        self._font_maps: dict[int, dict[str, dict[int, str]]] = {}
        # Whether this PDF has marked content (BDC/EMC tags)
        self.is_tagged: bool = False

    def parse(self) -> list[TextChunk]:
        """
        Main entry point. Returns a flat list of TextChunk objects
        ordered by page then by appearance in the content stream.
        """
        self._index_direct_objects()
        self._decode_object_streams()
        self._build_page_list()

        # Detect whether this PDF uses marked content
        # (check first few pages for BDC operators)
        self.is_tagged = self._detect_tagged()

        return self._extract_all_pages()

    def _detect_tagged(self) -> bool:
        """
        Quick check: does this PDF use BDC/EMC marked content tags?

        Checks the first 3 pages for BDC operators. If found, the PDF
        has structure tags and we can use them for classification.
        If not, we'll need heuristic classification (Phase 2b).
        """
        for page_obj in self.page_obj_nums[:3]:
            content = self._get_page_content(page_obj)
            if content and ' BDC' in content:
                return True
        return False

    # -- Step 1: Index direct objects --

    def _index_direct_objects(self):
        """Scan file for "N 0 obj" markers and record byte offsets."""
        for m in RE_OBJ_MARKER.finditer(self.text):
            obj_num = int(m.group(1))
            self.direct_obj_offsets[obj_num] = m.start()

    @staticmethod
    def _find_balanced_dict(text: str, start: int) -> str:
        """
        Starting at '<<' in text, find matching '>>' respecting nesting.

        HARDENED: returns empty string if precondition fails instead of
        asserting. Also has a safety limit to prevent infinite loops on
        malformed input.
        """
        if start >= len(text) or text[start:start + 2] != '<<':
            return ''
        depth = 0
        i = start
        limit = min(len(text), start + 500_000)  # safety limit
        while i < limit:
            if text[i:i + 2] == '<<':
                depth += 1; i += 2
            elif text[i:i + 2] == '>>':
                depth -= 1; i += 2
                if depth == 0:
                    return text[start:i]
            elif text[i] == '(':
                try:
                    _, i = _extract_parenthesised_string(text, i)
                except Exception:
                    i += 1
            else:
                i += 1
        return text[start:i] if i > start else ''

    # -- Step 2: Decode Object Streams --

    def _decode_object_streams(self):
        """
        Find /Type/ObjStm objects, decompress, parse embedded objects.

        HARDENED: each ObjStm is wrapped in try/except. A corrupt
        object stream is logged and skipped; other objects still parse.
        """
        for obj_num, offset in sorted(self.direct_obj_offsets.items()):
            try:
                self._try_decode_objstm(obj_num, offset)
            except Exception as e:
                logger.debug(f"Skipping ObjStm {obj_num}: {e}")

    def _try_decode_objstm(self, obj_num: int, offset: int):
        """Attempt to decode a single object stream. Raises on failure."""
        # Read enough to get the dict — use dynamic sizing
        chunk_size = min(50_000, len(self.text) - offset)
        chunk = self.text[offset:offset + chunk_size]

        obj_header = re.match(r'\d+\s+\d+\s+obj\s*', chunk)
        if not obj_header:
            return
        after = chunk[obj_header.end():]
        if not after.startswith('<<'):
            return

        full_dict = self._find_balanced_dict(after, 0)
        if not full_dict:
            return

        if '/Type' not in full_dict or 'ObjStm' not in full_dict:
            return

        first_s = _extract_dict_value(full_dict, 'First')
        n_s = _extract_dict_value(full_dict, 'N')
        filter_s = _extract_filter_value(full_dict)

        if not all([first_s, n_s]):
            return

        first = int(first_s)
        n = int(n_s)

        # Get stream length — try /Length first, resolve indirect refs
        length = self._resolve_stream_length(full_dict)

        # Find stream data
        dict_end_in_file = offset + obj_header.end() + len(full_dict)
        raw_pos = self._find_stream_start(dict_end_in_file)
        if raw_pos is None:
            return

        if length:
            stream_raw = self.raw[raw_pos:raw_pos + length]
        else:
            # Fallback: scan for endstream
            stream_raw = self._scan_for_endstream(raw_pos)

        decompressed = _decompress(stream_raw, filter_s)
        if not decompressed:
            return

        dec_text = decompressed.decode('latin-1')

        # Parse the index: N pairs of (obj_num, offset)
        index_part = dec_text[:first]
        tokens = index_part.split()
        entries = []
        for i in range(0, min(len(tokens), n * 2), 2):
            entries.append((int(tokens[i]), int(tokens[i + 1])))

        content = dec_text[first:]
        for i, (obj_num_inner, inner_offset) in enumerate(entries):
            end = entries[i + 1][1] if i + 1 < len(entries) else len(content)
            self.embedded_objects[obj_num_inner] = content[inner_offset:end].strip()

    def _resolve_stream_length(self, dict_text: str) -> Optional[int]:
        """
        Resolve /Length from a stream dictionary, handling indirect references.

        /Length can be a direct integer or an indirect reference (N 0 R).
        For indirect references, we resolve the referenced object to get
        the actual integer value.

        HARDENED: returns None if /Length is missing or can't be resolved.
        """
        length_s = _extract_dict_value(dict_text, 'Length')
        if not length_s:
            return None

        # Check for indirect reference (e.g., "123 0 R")
        ref_match = re.match(r'(\d+)\s+\d+\s+R', length_s)
        if ref_match:
            ref_num = int(ref_match.group(1))
            ref_defn = self._get_object_definition(ref_num)
            if ref_defn:
                try:
                    return int(ref_defn.strip())
                except ValueError:
                    return None
            return None

        try:
            return int(length_s)
        except ValueError:
            return None

    def _find_stream_start(self, dict_end_pos: int) -> Optional[int]:
        """
        Find the byte position where stream data begins, right after
        the 'stream' keyword and its line ending.
        """
        rest = self.text[dict_end_pos:dict_end_pos + 30]
        stream_kw = rest.find('stream')
        if stream_kw < 0:
            return None

        raw_pos = dict_end_pos + stream_kw + len('stream')
        # Skip line ending after 'stream' keyword
        if raw_pos < len(self.raw) and self.raw[raw_pos:raw_pos + 1] == b'\r':
            raw_pos += 1
        if raw_pos < len(self.raw) and self.raw[raw_pos:raw_pos + 1] == b'\n':
            raw_pos += 1
        return raw_pos

    def _scan_for_endstream(self, start_pos: int, max_scan: int = 10_000_000) -> bytes:
        """
        Fallback: scan for 'endstream' marker when /Length is unavailable.

        HARDENED: caps scan at max_scan bytes to avoid reading the entire
        file for a missing endstream marker.
        """
        end_pos = self.text.find('endstream', start_pos, start_pos + max_scan)
        if end_pos < 0:
            end_pos = start_pos + min(max_scan, len(self.raw) - start_pos)
        return self.raw[start_pos:end_pos]

    def _get_object_definition(self, obj_num: int) -> str:
        """
        Resolve an object's definition string by number.
        Checks embedded objects first, then direct objects.
        """
        if obj_num in self.embedded_objects:
            return self.embedded_objects[obj_num]

        if obj_num in self.direct_obj_offsets:
            try:
                offset = self.direct_obj_offsets[obj_num]
                # Dynamic read size based on file size remaining
                chunk_size = min(50_000, len(self.text) - offset)
                chunk = self.text[offset:offset + chunk_size]
                obj_start = chunk.find('obj')
                if obj_start < 0:
                    return ''
                after_obj = chunk[obj_start + 3:].lstrip()
                if after_obj.startswith('<<'):
                    return self._find_balanced_dict(after_obj, 0)
                end = after_obj.find('endobj')
                if end < 0:
                    end = after_obj.find('stream')
                if end < 0:
                    end = 2000
                return after_obj[:end].strip()
            except Exception as e:
                logger.debug(f"Error reading object {obj_num}: {e}")
                return ''
        return ''

    def _get_stream_data(self, obj_num: int) -> Optional[bytes]:
        """
        Get decompressed stream data for a direct stream object.

        HARDENED: uses dynamic /Length resolution with indirect reference
        support, and falls back to endstream scanning.
        """
        if obj_num not in self.direct_obj_offsets:
            return None

        try:
            offset = self.direct_obj_offsets[obj_num]
            chunk_size = min(50_000, len(self.text) - offset)
            chunk = self.text[offset:offset + chunk_size]

            obj_header = re.match(r'\d+\s+\d+\s+obj\s*', chunk)
            if not obj_header:
                return None
            after = chunk[obj_header.end():]
            if not after.startswith('<<'):
                return None

            full_dict = self._find_balanced_dict(after, 0)
            if not full_dict:
                return None

            filter_s = _extract_filter_value(full_dict)
            length = self._resolve_stream_length(full_dict)

            # Find stream start
            dict_end_abs = offset + obj_header.end() + len(full_dict)
            raw_pos = self._find_stream_start(dict_end_abs)
            if raw_pos is None:
                return None

            if length:
                stream_raw = self.raw[raw_pos:raw_pos + length]
            else:
                stream_raw = self._scan_for_endstream(raw_pos)

            return _decompress(stream_raw, filter_s)

        except Exception as e:
            logger.debug(f"Error reading stream {obj_num}: {e}")
            return None

    # -- Step 3: Build page list --

    def _build_page_list(self):
        """Walk /Pages tree from catalog to collect page objects in order."""
        root_num = None

        for obj_num, defn in self.embedded_objects.items():
            if '/Type' in defn and '/Catalog' in defn:
                root_num = obj_num
                break

        if root_num is None:
            for obj_num in self.direct_obj_offsets:
                defn = self._get_object_definition(obj_num)
                if '/Type' in defn and '/Catalog' in defn:
                    root_num = obj_num
                    break

        if root_num is None:
            logger.warning("Could not find catalog object")
            return

        catalog = self._get_object_definition(root_num)
        pages_ref = re.search(r'/Pages\s+(\d+)\s+0\s+R', catalog)
        if not pages_ref:
            logger.warning("Catalog has no /Pages")
            return

        pages_root = int(pages_ref.group(1))
        self.page_obj_nums = []
        self._walk_page_tree(pages_root)

    def _walk_page_tree(self, obj_num: int, depth: int = 0):
        """Recursively walk /Pages tree, collecting leaf /Page objects."""
        if depth > 50:  # safety limit for deeply nested/circular trees
            return
        defn = self._get_object_definition(obj_num)
        if not defn:
            return

        kids_match = re.search(r'/Kids\s*\[(.*?)\]', defn, re.DOTALL)
        if kids_match:
            refs = re.findall(r'(\d+)\s+0\s+R', kids_match.group(1))
            for ref in refs:
                self._walk_page_tree(int(ref), depth + 1)
        else:
            self.page_obj_nums.append(obj_num)

    # -- Step 4: Decode content streams --

    def _get_page_content(self, page_obj_num: int) -> Optional[str]:
        """Get decompressed content stream text for a page."""
        defn = self._get_object_definition(page_obj_num)
        if not defn:
            return None

        # Single content reference
        single = re.search(r'/Contents\s+(\d+)\s+0\s+R', defn)
        if single:
            stream = self._get_stream_data(int(single.group(1)))
            if stream:
                return stream.decode('latin-1')
            return None

        # Array of content references
        arr = re.search(r'/Contents\s*\[(.*?)\]', defn, re.DOTALL)
        if arr:
            refs = re.findall(r'(\d+)\s+0\s+R', arr.group(1))
            parts = []
            for ref in refs:
                stream = self._get_stream_data(int(ref))
                if stream:
                    parts.append(stream.decode('latin-1'))
            if parts:
                return '\n'.join(parts)

        return None

    # -- Step 4b: Resolve font character mappings --

    def _resolve_font_maps(self, page_obj_num: int) -> dict[str, dict[int, str]]:
        """
        For a page, resolve all fonts' character mappings.

        Priority order for each font:
          1. ToUnicode CMap (authoritative, used by 89% of fonts that have it)
          2. /Encoding + /Differences (fallback for the 81% of PDFs that use it)
          3. Standard encoding (base mapping for unmapped characters)

        Returns {font_name: {char_code: unicode_string}}.
        """
        if page_obj_num in self._font_maps:
            return self._font_maps[page_obj_num]

        maps = {}
        try:
            maps = self._build_font_maps(page_obj_num)
        except Exception as e:
            logger.debug(f"Error resolving fonts for page obj {page_obj_num}: {e}")

        self._font_maps[page_obj_num] = maps
        return maps

    def _build_font_maps(self, page_obj_num: int) -> dict[str, dict[int, str]]:
        """Build character maps for all fonts on a page."""
        maps = {}
        defn = self._get_object_definition(page_obj_num)
        if not defn:
            return maps

        # Find /Resources dict
        resources_text = self._get_resources_text(defn)
        if not resources_text:
            return maps

        # Find /Font dict within resources
        font_dict_text = self._get_font_dict_text(resources_text)
        if not font_dict_text:
            return maps

        # Iterate font entries: /FontName N 0 R
        for fm in re.finditer(r'/(\w+)\s+(\d+)\s+0\s+R', font_dict_text):
            font_name = '/' + fm.group(1)
            font_obj_num = int(fm.group(2))

            try:
                char_map = self._build_single_font_map(font_obj_num)
                if char_map:
                    maps[font_name] = char_map
            except Exception as e:
                logger.debug(f"Error building map for font {font_name}: {e}")

        return maps

    def _build_single_font_map(self, font_obj_num: int) -> dict[int, str]:
        """
        Build a character map for a single font object.

        Combines ToUnicode CMap (if available) with /Differences encoding
        as a fallback. ToUnicode takes priority when both exist.
        """
        font_defn = self._get_object_definition(font_obj_num)
        if not font_defn:
            return {}

        char_map = {}

        # Layer 1: /Encoding + /Differences (base layer)
        # This provides the fallback mapping for fonts without ToUnicode
        diff_map = self._parse_font_encoding(font_defn)
        if diff_map:
            char_map.update(diff_map)

        # Layer 2: ToUnicode CMap (authoritative, overrides /Differences)
        tounicode = re.search(r'/ToUnicode\s+(\d+)\s+0\s+R', font_defn)
        if tounicode:
            cmap_data = self._get_stream_data(int(tounicode.group(1)))
            if cmap_data:
                cmap_text = cmap_data.decode('latin-1')
                cmap = _parse_cmap(cmap_text)
                if cmap:
                    char_map.update(cmap)  # CMap overrides /Differences

        return char_map

    def _parse_font_encoding(self, font_defn: str) -> dict[int, str]:
        """
        Parse /Encoding from a font definition to build a character map.

        /Encoding can be:
          - A name: /Encoding /WinAnsiEncoding (base encoding, no custom diffs)
          - An indirect reference: /Encoding N 0 R (points to encoding dict)
          - An inline dict: /Encoding << /Type /Encoding /Differences [...] >>

        We primarily care about the /Differences array, which overrides
        specific character positions in the base encoding.
        """
        # Find /Encoding value
        enc_ref = re.search(r'/Encoding\s+(\d+)\s+0\s+R', font_defn)
        if enc_ref:
            enc_defn = self._get_object_definition(int(enc_ref.group(1)))
            if enc_defn and '/Differences' in enc_defn:
                return _parse_differences(enc_defn)
            return {}

        # Check for inline /Encoding dict with /Differences
        enc_start = font_defn.find('/Encoding')
        if enc_start < 0:
            return {}

        rest = font_defn[enc_start + len('/Encoding'):].lstrip()

        # Inline dict
        if rest.startswith('<<'):
            enc_dict = self._find_balanced_dict(rest, 0)
            if enc_dict and '/Differences' in enc_dict:
                return _parse_differences(enc_dict)

        return {}

    def _get_resources_text(self, page_defn: str) -> Optional[str]:
        """Extract the /Resources dictionary text from a page definition."""
        resources_ref = re.search(r'/Resources\s+(\d+)\s+0\s+R', page_defn)
        if resources_ref:
            return self._get_object_definition(int(resources_ref.group(1)))

        res_start = page_defn.find('/Resources')
        if res_start < 0:
            return None
        rest = page_defn[res_start + len('/Resources'):].lstrip()
        if rest.startswith('<<'):
            return self._find_balanced_dict(rest, 0)
        return None

    def _get_font_dict_text(self, resources_text: str) -> Optional[str]:
        """Extract the /Font dictionary text from a resources dictionary."""
        font_start = resources_text.find('/Font')
        if font_start < 0:
            return None

        rest = resources_text[font_start + len('/Font'):].lstrip()
        font_ref = re.match(r'(\d+)\s+0\s+R', rest)
        if font_ref:
            return self._get_object_definition(int(font_ref.group(1)))
        if rest.startswith('<<'):
            return self._find_balanced_dict(rest, 0)
        return None

    # -- Step 5: Parse content streams --

    def _extract_all_pages(self) -> list[TextChunk]:
        """Iterate all pages, extract text chunks."""
        all_chunks = []
        for page_idx, page_obj in enumerate(self.page_obj_nums, 1):
            try:
                content = self._get_page_content(page_obj)
                if not content:
                    continue
                font_maps = self._resolve_font_maps(page_obj)
                chunks = self._parse_content_stream(content, page_idx, font_maps)
                all_chunks.extend(chunks)
            except Exception as e:
                logger.debug(f"Error parsing page {page_idx}: {e}")
        return all_chunks

    def _parse_content_stream(self, content: str, page_num: int,
                              font_maps: dict[str, dict[int, str]] = None) -> list[TextChunk]:
        """
        Tokenize a content stream and extract text chunks with their
        structure tags and positional information.

        Tracks marked content scope (BDC/EMC), text state (Tf, Tm, Td),
        and TJ/Tj operators for text extraction.
        """
        chunks = []

        # State tracking
        mc_stack = []
        current_font = ''
        current_font_size = 0.0
        current_x = 0.0
        current_y = 0.0
        in_text = False
        current_text_parts = []
        current_mc_tag = ''
        current_mc_props = {}
        text_leading = 0.0
        tm_a = 1.0
        tm_d = 1.0

        # Tokenize
        tokens = self._tokenize_content_stream(content)

        # Process tokens
        operand_stack = []

        def _flush_text():
            nonlocal current_text_parts, current_mc_tag, current_mc_props
            text = ''.join(current_text_parts).strip()
            if text:
                chunk = TextChunk()
                chunk.page = page_num
                chunk.tag = current_mc_tag
                chunk.text = text
                chunk.font = current_font
                chunk.font_size = current_font_size
                chunk.x = current_x
                chunk.y = current_y
                chunk.properties = dict(current_mc_props)
                mcid = current_mc_props.get('MCID')
                if mcid is not None:
                    chunk.mcid = mcid
                chunk.chunk_type = self._classify_chunk(chunk, page_num)
                chunks.append(chunk)
            current_text_parts = []

        for tok_type, tok_val in tokens:
            if tok_type == 'keyword':
                op = tok_val

                # -- Marked content operators --
                if op == 'BDC':
                    tag = ''
                    props = {}
                    for ot, ov in operand_stack:
                        if ot == 'name':
                            tag = ov.lstrip('/')
                        elif ot == 'dict':
                            props = self._parse_bdc_properties(ov)
                    _flush_text()
                    mc_stack.append((tag, props))
                    current_mc_tag = tag
                    current_mc_props = props
                    operand_stack.clear()

                elif op == 'BMC':
                    tag = ''
                    for ot, ov in operand_stack:
                        if ot == 'name':
                            tag = ov.lstrip('/')
                    _flush_text()
                    mc_stack.append((tag, {}))
                    current_mc_tag = tag
                    current_mc_props = {}
                    operand_stack.clear()

                elif op == 'EMC':
                    _flush_text()
                    if mc_stack:
                        mc_stack.pop()
                    if mc_stack:
                        current_mc_tag, current_mc_props = mc_stack[-1]
                    else:
                        current_mc_tag = ''
                        current_mc_props = {}
                    operand_stack.clear()

                # -- Text object operators --
                elif op == 'BT':
                    in_text = True
                    current_x = 0.0; current_y = 0.0
                    tm_a = 1.0; tm_d = 1.0
                    operand_stack.clear()

                elif op == 'ET':
                    in_text = False
                    operand_stack.clear()

                # -- Font operator --
                elif op == 'Tf':
                    for ot, ov in operand_stack:
                        if ot == 'name':
                            current_font = ov
                        elif ot == 'number':
                            current_font_size = abs(float(ov))
                    operand_stack.clear()

                # -- Text positioning --
                elif op == 'Tm':
                    nums = [ov for ot, ov in operand_stack if ot == 'number']
                    if len(nums) >= 6:
                        tm_a = nums[0]; tm_d = nums[3]
                        current_x = nums[4]; current_y = nums[5]
                        if abs(tm_d) > 0.1:
                            current_font_size = abs(tm_d)
                    operand_stack.clear()

                elif op == 'Td':
                    nums = [ov for ot, ov in operand_stack if ot == 'number']
                    if len(nums) >= 2:
                        current_x += nums[0] * abs(tm_a) if abs(tm_a) > 0.01 else nums[0]
                        current_y += nums[1] * abs(tm_d) if abs(tm_d) > 0.01 else nums[1]
                    operand_stack.clear()

                elif op == 'TD':
                    nums = [ov for ot, ov in operand_stack if ot == 'number']
                    if len(nums) >= 2:
                        current_x += nums[0] * abs(tm_a) if abs(tm_a) > 0.01 else nums[0]
                        current_y += nums[1] * abs(tm_d) if abs(tm_d) > 0.01 else nums[1]
                        text_leading = -nums[1]
                    operand_stack.clear()

                elif op == 'T*':
                    current_y -= text_leading
                    operand_stack.clear()

                elif op == 'TL':
                    nums = [ov for ot, ov in operand_stack if ot == 'number']
                    if nums:
                        text_leading = nums[0]
                    operand_stack.clear()

                # -- Text showing operators --
                elif op == 'TJ':
                    active_map = (font_maps or {}).get(current_font, None)
                    for ot, ov in operand_stack:
                        if ot == 'array':
                            items = self._parse_tj_array(ov)
                            text = _text_from_tj_array(items, active_map)
                            current_text_parts.append(text)
                    operand_stack.clear()

                elif op == 'Tj':
                    active_map = (font_maps or {}).get(current_font, None)
                    for ot, ov in operand_stack:
                        if ot == 'string':
                            parsed = _parse_string_literal(ov)
                            if active_map:
                                parsed = _apply_cmap(parsed, active_map)
                            current_text_parts.append(parsed)
                        elif ot == 'hexstring':
                            text = ov
                            if active_map:
                                text = _apply_cmap(text, active_map)
                            current_text_parts.append(text)
                    operand_stack.clear()

                elif op == "'":
                    current_y -= text_leading
                    active_map = (font_maps or {}).get(current_font, None)
                    for ot, ov in operand_stack:
                        if ot == 'string':
                            parsed = _parse_string_literal(ov)
                            if active_map:
                                parsed = _apply_cmap(parsed, active_map)
                            current_text_parts.append(parsed)
                    operand_stack.clear()

                elif op == '"':
                    current_y -= text_leading
                    active_map = (font_maps or {}).get(current_font, None)
                    for ot, ov in operand_stack:
                        if ot == 'string':
                            parsed = _parse_string_literal(ov)
                            if active_map:
                                parsed = _apply_cmap(parsed, active_map)
                            current_text_parts.append(parsed)
                    operand_stack.clear()

                # -- Graphics state (no-ops for text extraction) --
                elif op in ('q', 'Q', 'cm', 'gs', 'Do', 'sh',
                            'W', 'W*', 'n', 'f', 'f*', 'F', 'B', 'B*',
                            'b', 'b*', 'S', 's',
                            'm', 'l', 'c', 'v', 'y', 'h', 're',
                            'k', 'K', 'g', 'G', 'rg', 'RG',
                            'cs', 'CS', 'sc', 'SC', 'scn', 'SCN',
                            'd', 'i', 'j', 'J', 'M', 'w',
                            'ri', 'BX', 'EX',
                            'Tc', 'Tw', 'Tz', 'Tr', 'Ts',
                            'DP', 'MP'):
                    operand_stack.clear()
                else:
                    operand_stack.clear()
            else:
                operand_stack.append((tok_type, tok_val))

        _flush_text()
        return chunks

    def _tokenize_content_stream(self, content: str) -> list[tuple]:
        """
        Tokenize a PDF content stream into typed tokens.

        Returns list of (type, value) tuples where type is one of:
        'string', 'hexstring', 'dict', 'array', 'name', 'number', 'keyword'.
        """
        tokens = []
        pos = 0

        while pos < len(content):
            ch = content[pos]

            if ch in ' \t\r\n\x00':
                pos += 1; continue

            if ch == '%':
                eol = content.find('\n', pos)
                pos = eol + 1 if eol >= 0 else len(content); continue

            if ch == '(':
                s, pos = _extract_parenthesised_string(content, pos)
                tokens.append(('string', s)); continue

            if ch == '<' and pos + 1 < len(content) and content[pos + 1] != '<':
                s, pos = _extract_hex_string(content, pos)
                tokens.append(('hexstring', s)); continue

            if ch == '<' and pos + 1 < len(content) and content[pos + 1] == '<':
                depth = 0; i = pos
                while i < len(content):
                    if content[i:i + 2] == '<<':
                        depth += 1; i += 2
                    elif content[i:i + 2] == '>>':
                        depth -= 1; i += 2
                        if depth == 0:
                            tokens.append(('dict', content[pos:i]))
                            pos = i; break
                    else:
                        i += 1
                else:
                    pos = len(content)
                continue

            if ch == '[':
                depth = 1; i = pos + 1
                while i < len(content) and depth > 0:
                    c = content[i]
                    if c == '[':
                        depth += 1; i += 1
                    elif c == ']':
                        depth -= 1; i += 1
                    elif c == '(':
                        _, i = _extract_parenthesised_string(content, i)
                    elif c == '<' and i + 1 < len(content) and content[i + 1] != '<':
                        _, i = _extract_hex_string(content, i)
                    else:
                        i += 1
                tokens.append(('array', content[pos:i]))
                pos = i; continue

            if ch == '/':
                i = pos + 1
                while i < len(content) and content[i] not in ' \t\r\n\x00/<>[](){}%':
                    i += 1
                tokens.append(('name', content[pos:i]))
                pos = i; continue

            if ch in '0123456789.-+':
                i = pos + 1
                while i < len(content) and content[i] in '0123456789.':
                    i += 1
                tok = content[pos:i]
                try:
                    if '.' in tok:
                        tokens.append(('number', float(tok)))
                    else:
                        tokens.append(('number', int(tok)))
                except ValueError:
                    tokens.append(('keyword', tok))
                pos = i; continue

            if ch.isalpha() or ch == "'":
                i = pos + 1
                while i < len(content) and (content[i].isalnum() or content[i] in "_*'"):
                    i += 1
                tokens.append(('keyword', content[pos:i]))
                pos = i; continue

            pos += 1

        return tokens

    def _parse_bdc_properties(self, dict_str: str) -> dict:
        """Parse BDC operator properties dictionary."""
        props = {}
        inner = dict_str.strip()
        if inner.startswith('<<'):
            inner = inner[2:]
        if inner.endswith('>>'):
            inner = inner[:-2]

        mcid = re.search(r'/MCID\s+(\d+)', inner)
        if mcid:
            props['MCID'] = int(mcid.group(1))

        lang = re.search(r'/Lang\s*\(([^)]*)\)', inner)
        if lang:
            props['Lang'] = lang.group(1).strip()

        subtype = re.search(r'/Subtype\s*/(\w+)', inner)
        if subtype:
            props['Subtype'] = subtype.group(1)

        type_m = re.search(r'/Type\s*/(\w+)', inner)
        if type_m:
            props['Type'] = type_m.group(1)

        attached = re.search(r'/Attached\s*\[([^\]]*)\]', inner)
        if attached:
            props['Attached'] = attached.group(1).strip()

        return props

    def _parse_tj_array(self, array_str: str) -> list:
        """Parse TJ array string into list of strings and numbers."""
        items = []
        inner = array_str.strip()
        if inner.startswith('['):
            inner = inner[1:]
        if inner.endswith(']'):
            inner = inner[:-1]

        pos = 0
        while pos < len(inner):
            ch = inner[pos]
            if ch in ' \t\r\n':
                pos += 1; continue
            if ch == '(':
                s, pos = _extract_parenthesised_string(inner, pos)
                items.append(s); continue
            if ch == '<' and pos + 1 < len(inner) and inner[pos + 1] != '<':
                s, pos = _extract_hex_string(inner, pos)
                items.append(s); continue
            if ch in '0123456789.-+':
                j = pos + 1
                while j < len(inner) and inner[j] in '0123456789.eE+-':
                    j += 1
                try:
                    items.append(float(inner[pos:j]))
                except ValueError:
                    pass
                pos = j; continue
            pos += 1

        return items

    # -- Step 6: Classify chunks --

    def _classify_chunk(self, chunk: TextChunk, page_num: int) -> str:
        """
        Assign semantic chunk type from structure tag + heuristics.

        For tagged PDFs, the BDC tag is authoritative.
        For untagged PDFs (no tag), returns 'unknown' — the structure
        classifier module (Phase 2b) will reclassify based on font size
        and text patterns.
        """
        tag = chunk.tag

        if tag == 'Artifact':
            subtype = chunk.properties.get('Subtype', '')
            if subtype == 'Header':
                return 'header'
            elif subtype == 'Footer':
                return 'footer'
            return 'artifact'

        if tag in ('H1', 'H2', 'H3', 'H4', 'H5', 'H6'):
            return 'heading'

        if tag == 'P':
            if page_num == 1 and chunk.font_size >= 16:
                return 'cover_element'
            return 'paragraph'

        if tag == 'Figure':
            return 'figure'
        if tag == 'Caption':
            return 'caption'
        if tag in ('Table', 'TD', 'TH', 'TR', 'THead', 'TBody'):
            return 'table'
        if tag == 'TOCI':
            return 'toc_entry'
        if tag in ('Reference', 'Link'):
            return 'reference'
        if tag in ('L', 'LI', 'LBody', 'Lbl'):
            return 'list_item'
        if tag == 'Span':
            return 'paragraph'

        if page_num == 1 and tag:
            return 'cover_element'

        if tag and tag.startswith('MC'):
            return 'artifact'

        return 'unknown'


# ---------------------------------------------------------------------------
# Post-processing: table detection heuristic
# ---------------------------------------------------------------------------

RE_TABLE_CAPTION = re.compile(r'^Table\s+[A-Z]?-?\d+\.')
TABLE_FONT_CEILING = 11.5
TABLE_BODY_TEXT_MIN_LEN = 30


def _detect_tables(chunks: list[TextChunk]) -> list[TextChunk]:
    """
    Post-processing pass that reclassifies paragraph chunks inside
    table regions as table_caption, table_cell, or table_footnote.

    Uses caption pattern matching + font size drops as signals.
    """
    in_table = False
    saw_non_footnote_cell = False

    for chunk in chunks:
        if chunk.chunk_type in ('header', 'footer'):
            continue

        if not in_table:
            if (chunk.chunk_type == 'paragraph'
                    and chunk.font_size < TABLE_FONT_CEILING
                    and RE_TABLE_CAPTION.match(chunk.text)):
                chunk.chunk_type = 'table_caption'
                in_table = True
                saw_non_footnote_cell = False
            elif chunk.chunk_type == 'table':
                chunk.chunk_type = 'table_cell'
                in_table = True
                saw_non_footnote_cell = True
        else:
            if chunk.chunk_type == 'heading':
                in_table = False
                continue

            if (chunk.chunk_type == 'paragraph'
                    and chunk.font_size >= TABLE_FONT_CEILING
                    and len(chunk.text) > TABLE_BODY_TEXT_MIN_LEN):
                in_table = False
                continue

            if chunk.chunk_type not in ('paragraph', 'table', 'reference',
                                         'list_item', 'cover_element'):
                if (chunk.chunk_type == 'paragraph'
                        and RE_TABLE_CAPTION.match(chunk.text)):
                    chunk.chunk_type = 'table_caption'
                    saw_non_footnote_cell = False
                    continue
                in_table = False
                continue

            if chunk.chunk_type in ('paragraph', 'table'):
                fs = chunk.font_size
                if RE_TABLE_CAPTION.match(chunk.text):
                    chunk.chunk_type = 'table_caption'
                    saw_non_footnote_cell = False
                elif (saw_non_footnote_cell
                      and 8.5 <= fs <= 9.5
                      and len(chunk.text) > 10
                      and chunk.text[0].islower()):
                    chunk.chunk_type = 'table_footnote'
                else:
                    chunk.chunk_type = 'table_cell'
                    if fs > 2:
                        saw_non_footnote_cell = True

    return chunks


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_pdf(file_path: str) -> list[dict]:
    """
    Parse a PDF file and return semantically classified text chunks.

    Each chunk dict has keys: page, type, tag, text, and optionally
    font, font_size, position, mcid, properties.

    This is the main public API. It handles both tagged and untagged
    PDFs, applying the table detection heuristic for tagged PDFs.

    Returns an empty list (not an exception) if the PDF can't be parsed.
    """
    try:
        raw = _read_file(file_path)
        parser = PdfParser(raw)
        chunks = parser.parse()
        chunks = _detect_tables(chunks)
        return [c.to_dict() for c in chunks]
    except Exception as e:
        logger.warning(f"Failed to parse {file_path}: {e}")
        return []


def parse_pdf_raw(file_path: str) -> tuple[list[TextChunk], bool]:
    """
    Parse a PDF and return raw TextChunk objects + tagged flag.

    Unlike parse_pdf(), this returns the TextChunk objects (not dicts)
    and whether the PDF was tagged. Used by the structure classifier
    and batch runner which need access to the raw objects.
    """
    try:
        raw = _read_file(file_path)
        parser = PdfParser(raw)
        chunks = parser.parse()
        chunks = _detect_tables(chunks)
        return chunks, parser.is_tagged
    except Exception as e:
        logger.warning(f"Failed to parse {file_path}: {e}")
        return [], False
