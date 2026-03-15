/**
 * PaperDetailDrawer.tsx — Slide-out drawer with tabs: Details, Document, Citations.
 *
 * The Details tab shows metadata, abstract, genes, claims, organs.
 * The Document tab shows the parsed PDF content grouped by section,
 * with cross-reference links between chunks (e.g. "Table 1" in text
 * scrolls to the Table 1 caption).
 * The Citations tab shows cited_by and references lists.
 */

import { useEffect, useState, useCallback, useRef } from 'react';
import { Drawer, Tabs, Typography, Tag, Divider, List, Collapse, Empty, Spin, Badge } from 'antd';
import {
  LinkOutlined,
  ExperimentOutlined,
  FileTextOutlined,
  StarFilled,
  FilePdfOutlined,
  ReadOutlined,
  NodeIndexOutlined,
} from '@ant-design/icons';
import type { PaperDetail, DocumentData, DocChunk } from '../api/client';
import { fetchPaperDocument } from '../api/client';

const { Title, Text, Paragraph } = Typography;

const organColors: Record<string, string> = {
  liver: 'green', kidney: 'blue', heart: 'red', lung: 'cyan',
  brain: 'purple', skin: 'orange', blood: 'magenta', thyroid: 'geekblue',
  adrenal: 'gold', testes: 'lime', spleen: 'volcano',
};

/* Color for chunk type badges */
const chunkTypeColor: Record<string, string> = {
  heading: 'blue',
  paragraph: 'default',
  caption: 'orange',
  table_caption: 'orange',
  table_cell: 'purple',
  table_footnote: 'purple',
  reference: 'cyan',
  list_item: 'default',
  artifact: 'default',
};

interface Props {
  paper: PaperDetail | null;
  onClose: () => void;
  onNavigate: (paperId: string) => void;
}

export default function PaperDetailDrawer({ paper, onClose, onNavigate }: Props) {
  const [activeTab, setActiveTab] = useState('details');
  const [docData, setDocData] = useState<DocumentData | null>(null);
  const [docLoading, setDocLoading] = useState(false);
  /* Set of chunk_ids that are cross-reference targets — used for highlighting */
  const [highlightedChunks, setHighlightedChunks] = useState<Set<number>>(new Set());
  const chunkRefs = useRef<Map<number, HTMLDivElement>>(new Map());

  /* Reset state when paper changes */
  useEffect(() => {
    setDocData(null);
    setDocLoading(false);
    setActiveTab('details');
    setHighlightedChunks(new Set());
    chunkRefs.current.clear();
  }, [paper?.paper_id]);

  /* Fetch document data when Document tab is first opened */
  const handleTabChange = useCallback((key: string) => {
    setActiveTab(key);
    if (key === 'document' && !docData && !docLoading && paper) {
      setDocLoading(true);
      fetchPaperDocument(paper.paper_id)
        .then(setDocData)
        .catch(console.error)
        .finally(() => setDocLoading(false));
    }
  }, [docData, docLoading, paper]);

  /* Build a lookup from chunk_id → set of target chunk_ids it references */
  const crossRefTargets = new Map<number, number[]>();
  const crossRefSources = new Map<number, number[]>();
  if (docData) {
    for (const ref of docData.cross_refs) {
      const targets = crossRefTargets.get(ref.source_id) ?? [];
      targets.push(ref.target_id);
      crossRefTargets.set(ref.source_id, targets);
      const sources = crossRefSources.get(ref.target_id) ?? [];
      sources.push(ref.source_id);
      crossRefSources.set(ref.target_id, sources);
    }
  }

  /* Scroll to a chunk and highlight it */
  const scrollToChunk = useCallback((chunkId: number) => {
    setHighlightedChunks(new Set([chunkId]));
    const el = chunkRefs.current.get(chunkId);
    if (el) {
      el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    }
    setTimeout(() => setHighlightedChunks(new Set()), 3000);
  }, []);

  /* Render a single chunk with cross-reference handling */
  const renderChunk = (chunk: DocChunk) => {
    const isHighlighted = highlightedChunks.has(chunk.chunk_id);
    const hasRefs = crossRefTargets.has(chunk.chunk_id);
    const isTarget = crossRefSources.has(chunk.chunk_id);

    return (
      <div
        key={chunk.chunk_id}
        ref={(el) => { if (el) chunkRefs.current.set(chunk.chunk_id, el); }}
        style={{
          padding: '4px 8px',
          marginBottom: 4,
          borderLeft: isTarget ? '3px solid #faad14' : '3px solid transparent',
          background: isHighlighted ? '#fffbe6' : 'transparent',
          transition: 'background 0.5s',
          borderRadius: 4,
        }}
      >
        <div style={{ display: 'flex', gap: 6, alignItems: 'baseline' }}>
          <Tag
            color={chunkTypeColor[chunk.chunk_type] ?? 'default'}
            style={{ fontSize: 10, margin: 0, lineHeight: '16px' }}
          >
            {chunk.chunk_type}
          </Tag>
          <Text type="secondary" style={{ fontSize: 10 }}>p.{chunk.page_num}</Text>
          {hasRefs && (
            <Text
              style={{ fontSize: 10, color: '#1677ff', cursor: 'pointer' }}
              onClick={() => {
                const targets = crossRefTargets.get(chunk.chunk_id) ?? [];
                if (targets.length > 0) scrollToChunk(targets[0]);
              }}
            >
              → refs
            </Text>
          )}
        </div>
        <Text style={{
          fontSize: chunk.chunk_type === 'heading' ? 14 : 13,
          fontWeight: chunk.chunk_type === 'heading' ? 600 : 400,
          color: chunk.chunk_type === 'reference' ? '#666' : '#333',
          fontStyle: chunk.chunk_type === 'caption' || chunk.chunk_type === 'table_caption' ? 'italic' : 'normal',
        }}>
          {chunk.text}
        </Text>
      </div>
    );
  };

  /* === Details tab content === */
  const detailsContent = paper ? (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div>
        <Title level={4} style={{ marginBottom: 4 }}>{paper.title}</Title>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, alignItems: 'center', marginBottom: 8 }}>
          {paper.is_seed && <Tag icon={<StarFilled />} color="gold">Seed Paper</Tag>}
          {paper.is_review && <Tag color="blue">Review</Tag>}
          {paper.year && <Tag>{paper.year}</Tag>}
          {paper.venue && <Tag>{paper.venue}</Tag>}
          <Tag>{paper.citation_count} citation{paper.citation_count !== 1 ? 's' : ''}</Tag>
          {paper.relevance_score != null && (
            <Tag color={paper.relevance_score >= 0.7 ? 'green' : paper.relevance_score >= 0.4 ? 'orange' : 'default'}>
              Relevance: {(paper.relevance_score * 100).toFixed(0)}%
            </Tag>
          )}
        </div>
      </div>

      {paper.doi && (
        <div>
          <a href={`https://doi.org/${paper.doi}`} target="_blank" rel="noopener noreferrer">
            <LinkOutlined /> {paper.doi}
          </a>
        </div>
      )}

      {paper.pdf && (
        <div>
          <Tag icon={<FilePdfOutlined />} color="green">
            PDF: {(paper.pdf.file_size / 1024 / 1024).toFixed(1)} MB ({paper.pdf.source})
          </Tag>
          {paper.pdf.parse_status === 'parsed' && (
            <Tag color="blue">Parsed</Tag>
          )}
        </div>
      )}

      <Divider style={{ margin: '4px 0' }} />

      {paper.abstract && (
        <div>
          <Text strong style={{ fontSize: 14 }}><FileTextOutlined /> Abstract</Text>
          <Paragraph
            ellipsis={{ rows: 6, expandable: true, symbol: 'Show more' }}
            style={{ marginTop: 8, color: '#444' }}
          >
            {paper.abstract}
          </Paragraph>
        </div>
      )}

      {paper.claims.length > 0 && (
        <div>
          <Text strong style={{ fontSize: 14 }}>
            <ExperimentOutlined /> Extracted Claims ({paper.claims.length})
          </Text>
          <List
            size="small"
            dataSource={paper.claims}
            renderItem={(claim) => (
              <List.Item style={{ padding: '6px 0', borderBottom: '1px solid #f0f0f0' }}>
                <Text style={{ fontSize: 13 }}>{claim}</Text>
              </List.Item>
            )}
            style={{ marginTop: 8 }}
          />
        </div>
      )}

      {paper.genes.length > 0 && (
        <div>
          <Text strong style={{ fontSize: 14 }}>Genes ({paper.genes.length})</Text>
          <div style={{ marginTop: 8, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {paper.genes.map((gene) => (
              <Tag key={gene} color="blue">{gene}</Tag>
            ))}
          </div>
        </div>
      )}

      {paper.organs.length > 0 && (
        <div>
          <Text strong style={{ fontSize: 14 }}>Organs ({paper.organs.length})</Text>
          <div style={{ marginTop: 8, display: 'flex', flexWrap: 'wrap', gap: 4 }}>
            {paper.organs.map((organ) => (
              <Tag key={organ} color={organColors[organ.toLowerCase()] ?? 'default'}>{organ}</Tag>
            ))}
          </div>
        </div>
      )}
    </div>
  ) : null;

  /* === Document tab content === */
  const documentContent = (
    <div>
      {docLoading && (
        <div style={{ display: 'flex', justifyContent: 'center', padding: 48 }}>
          <Spin size="large" />
        </div>
      )}

      {docData && docData.stats && (
        <div style={{ marginBottom: 16 }}>
          <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap', fontSize: 12, color: '#888' }}>
            <span>Quality: <Tag color={docData.stats.parse_quality === 'tagged' ? 'green' : 'blue'} style={{ fontSize: 11 }}>{docData.stats.parse_quality}</Tag></span>
            <span>{docData.stats.total_chunks} chunks</span>
            <span>{docData.stats.total_sections} sections</span>
            <span>{docData.stats.pages} pages</span>
            {docData.cross_refs.length > 0 && (
              <span>{docData.cross_refs.length} cross-refs</span>
            )}
          </div>
        </div>
      )}

      {docData && docData.sections.length > 0 && (
        <Collapse
          defaultActiveKey={docData.sections.slice(0, 3).map((_, i) => String(i))}
          ghost
          items={docData.sections.map((section, i) => ({
            key: String(i),
            label: (
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Text strong style={{ fontSize: 13 }}>
                  {section.heading ? section.heading.text.slice(0, 80) : '(Preamble)'}
                </Text>
                {section.section_name && (
                  <Tag color="blue" style={{ fontSize: 10 }}>{section.section_name}</Tag>
                )}
                <Badge count={section.chunks.length} style={{ backgroundColor: '#d9d9d9' }} />
              </div>
            ),
            children: (
              <div style={{ maxHeight: 400, overflow: 'auto' }}>
                {section.chunks.map(renderChunk)}
              </div>
            ),
          }))}
        />
      )}

      {docData && docData.sections.length === 0 && (
        <Empty description="No parsed document content available" />
      )}
    </div>
  );

  /* === Citations tab content === */
  const citationsContent = paper ? (
    <Collapse
      ghost
      items={[
        ...(paper.cited_by?.length ? [{
          key: 'cited_by',
          label: `Cited by (${paper.cited_by.length})`,
          children: (
            <List
              size="small"
              dataSource={paper.cited_by}
              renderItem={(id: string) => (
                <List.Item
                  style={{ padding: '4px 0', cursor: 'pointer' }}
                  onClick={() => onNavigate(id)}
                >
                  <Text style={{ color: '#1677ff', fontSize: 13 }} ellipsis>{id}</Text>
                </List.Item>
              )}
            />
          ),
        }] : []),
        ...(paper.references?.length ? [{
          key: 'references',
          label: `References (${paper.references.length})`,
          children: (
            <List
              size="small"
              dataSource={paper.references}
              renderItem={(id: string) => (
                <List.Item
                  style={{ padding: '4px 0', cursor: 'pointer' }}
                  onClick={() => onNavigate(id)}
                >
                  <Text style={{ color: '#1677ff', fontSize: 13 }} ellipsis>{id}</Text>
                </List.Item>
              )}
            />
          ),
        }] : []),
      ]}
    />
  ) : null;

  return (
    <Drawer
      title={paper ? 'Paper Detail' : 'Loading...'}
      placement="right"
      width={700}
      open={paper !== null}
      onClose={onClose}
      destroyOnClose
    >
      {paper && (
        <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          items={[
            {
              key: 'details',
              label: <span><FileTextOutlined /> Details</span>,
              children: detailsContent,
            },
            {
              key: 'document',
              label: <span><ReadOutlined /> Document</span>,
              children: documentContent,
            },
            {
              key: 'citations',
              label: <span><NodeIndexOutlined /> Citations</span>,
              children: citationsContent,
            },
          ]}
        />
      )}
    </Drawer>
  );
}
