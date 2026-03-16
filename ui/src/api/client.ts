/**
 * api/client.ts — HTTP client for the fractal backend API.
 *
 * All requests go through fetch() to /api/* endpoints, which the Vite dev
 * server proxies to the FastAPI backend on port 8888.
 */


/** Typed wrapper around fetch for JSON GET requests. */
export async function get<T>(path: string): Promise<T> {
  const res = await fetch(path);
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`API error ${res.status}: ${body}`);
  }
  return res.json();
}


/* === Type definitions === */

/* Types matching the backend JSON responses */

export interface KbStats {
  tables: string[];
  papers: number;
  genes: number;
  pathways: number;
  go_terms: number;
  citation_edges: number;
  claims: number;
  pdfs: number;
  chunks: number;
  doc_edges: number;
  organs: Record<string, number>;
}

export interface GraphNode {
  id: string;
  title: string;
  year: number | null;
  citation_count: number;
  relevance_score: number;
  is_seed: boolean;
  has_pdf: boolean;
}

export interface GraphEdge {
  source: string;
  target: string;
}

export interface CitationGraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface Paper {
  paper_id: string;
  title: string;
  year: number | null;
  venue: string | null;
  citation_count: number | null;
  relevance_score: number | null;
  is_seed: boolean;
  organs: string[];
}

export interface PaperDetail extends Paper {
  abstract: string | null;
  doi: string | null;
  is_review: boolean;
  genes: string[];
  claims: string[];
  cited_by: string[];
  references: string[];
  pdf?: {
    filename: string;
    file_size: number;
    source: string;
    url: string;
    parse_status: string;
  };
}

export interface PaginatedPapers {
  items: Paper[];
  total: number;
  page: number;
  per_page: number;
}

export interface SqlResult {
  columns: string[];
  rows: unknown[][];
  row_count: number;
}


/* === API functions === */

export function fetchStats(): Promise<KbStats> {
  return get<KbStats>('/api/stats');
}

export function fetchCitationGraph(): Promise<CitationGraphData> {
  return get<CitationGraphData>('/api/citation-graph');
}

export function fetchPapers(params: {
  page?: number;
  per_page?: number;
  q?: string;
  sort?: string;
  organ?: string;
  year_min?: number;
} = {}): Promise<PaginatedPapers> {
  const qs = new URLSearchParams();
  if (params.page) qs.set('page', String(params.page));
  if (params.per_page) qs.set('per_page', String(params.per_page));
  if (params.q) qs.set('q', params.q);
  if (params.sort) qs.set('sort', params.sort);
  if (params.organ) qs.set('organ', params.organ);
  if (params.year_min) qs.set('year_min', String(params.year_min));
  const query = qs.toString();
  return get<PaginatedPapers>(`/api/papers${query ? '?' + query : ''}`);
}

export function fetchPaperDetail(paperId: string): Promise<PaperDetail> {
  return get<PaperDetail>(`/api/papers/${encodeURIComponent(paperId)}`);
}

export function executeQuery(sql: string): Promise<SqlResult> {
  const qs = new URLSearchParams({ sql });
  return get<SqlResult>(`/api/query?${qs.toString()}`);
}


/* === Document types === */

export interface DocChunk {
  chunk_id: number;
  page_num: number;
  chunk_type: string;
  section_name: string | null;
  text: string;
  font_size: number | null;
}

export interface DocSection {
  heading: DocChunk | null;
  section_name: string | null;
  chunks: DocChunk[];
}

export interface CrossRef {
  source_id: number;
  target_id: number;
}

export interface DocumentData {
  sections: DocSection[];
  cross_refs: CrossRef[];
  stats: {
    parse_quality: string;
    total_chunks: number;
    total_sections: number;
    type_counts: Record<string, number>;
    pages: number;
  } | null;
}

export interface StructureNode {
  chunk_id: number;
  page: number;
  section_name: string | null;
  text: string;
  children: {
    chunk_id: number;
    chunk_type: string;
    page: number;
    preview: string;
  }[];
}

export interface StructureData {
  tree: StructureNode[];
  edge_counts: Record<string, number>;
}

export interface ParsedPaper {
  paper_id: string;
  title: string;
  year: number | null;
  venue: string | null;
  parse_quality: string;
  chunk_count: number;
  heading_count: number;
  section_count: number;
  page_count: number;
  paragraph_count: number;
  table_cell_count: number;
  reference_count: number;
}

export interface PaginatedParsedPapers {
  items: ParsedPaper[];
  total: number;
  page: number;
  per_page: number;
}

export function fetchDocuments(params: {
  page?: number;
  per_page?: number;
  q?: string;
  quality?: string;
  organ?: string;
  has_sections?: boolean;
  sort?: string;
} = {}): Promise<PaginatedParsedPapers> {
  const qs = new URLSearchParams();
  if (params.page) qs.set('page', String(params.page));
  if (params.per_page) qs.set('per_page', String(params.per_page));
  if (params.q) qs.set('q', params.q);
  if (params.quality) qs.set('quality', params.quality);
  if (params.organ) qs.set('organ', params.organ);
  if (params.has_sections !== undefined) qs.set('has_sections', String(params.has_sections));
  if (params.sort) qs.set('sort', params.sort);
  const query = qs.toString();
  return get<PaginatedParsedPapers>(`/api/documents${query ? '?' + query : ''}`);
}

export function fetchPaperDocument(paperId: string): Promise<DocumentData> {
  return get<DocumentData>(`/api/papers/${encodeURIComponent(paperId)}/document`);
}

export function fetchPaperStructure(paperId: string): Promise<StructureData> {
  return get<StructureData>(`/api/papers/${encodeURIComponent(paperId)}/structure`);
}
