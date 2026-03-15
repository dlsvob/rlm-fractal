/**
 * PaperDetailDrawer.tsx — Slide-out drawer showing full detail for a single paper.
 *
 * Displays title, metadata tags (year, venue, seed, review), abstract, DOI link,
 * extracted genes, claims, organs, citation links, and PDF availability.
 * Citation links are clickable to navigate to other papers.
 */

import { Drawer, Typography, Tag, Divider, List, Collapse, Empty } from 'antd';
import {
  LinkOutlined,
  ExperimentOutlined,
  FileTextOutlined,
  StarFilled,
  FilePdfOutlined,
} from '@ant-design/icons';
import type { PaperDetail } from '../api/client';

const { Title, Text, Paragraph } = Typography;

/* Color-code organs for visual distinction in the tag list */
const organColors: Record<string, string> = {
  liver: 'green', kidney: 'blue', heart: 'red', lung: 'cyan',
  brain: 'purple', skin: 'orange', blood: 'magenta', thyroid: 'geekblue',
  adrenal: 'gold', testes: 'lime', spleen: 'volcano',
};

interface Props {
  paper: PaperDetail | null;
  onClose: () => void;
  /** Called when user clicks a citation link to navigate to another paper */
  onNavigate: (paperId: string) => void;
}

export default function PaperDetailDrawer({ paper, onClose, onNavigate }: Props) {
  return (
    <Drawer
      title={paper ? 'Paper Detail' : 'Loading...'}
      placement="right"
      width={640}
      open={paper !== null}
      onClose={onClose}
      destroyOnClose
    >
      {paper && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
          {/* Title and metadata tags */}
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

          {/* DOI link */}
          {paper.doi && (
            <div>
              <a href={`https://doi.org/${paper.doi}`} target="_blank" rel="noopener noreferrer">
                <LinkOutlined /> {paper.doi}
              </a>
            </div>
          )}

          {/* PDF availability */}
          {paper.pdf && (
            <div>
              <Tag icon={<FilePdfOutlined />} color="green">
                PDF: {(paper.pdf.file_size / 1024 / 1024).toFixed(1)} MB ({paper.pdf.source})
              </Tag>
            </div>
          )}

          <Divider style={{ margin: '4px 0' }} />

          {/* Abstract */}
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

          {/* Extracted claims */}
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

          {/* Genes */}
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

          {/* Organs */}
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

          <Divider style={{ margin: '4px 0' }} />

          {/* Citation links — clickable to navigate to cited/citing papers */}
          {((paper.cited_by?.length ?? 0) > 0 || (paper.references?.length ?? 0) > 0) && (
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
          )}
        </div>
      )}
    </Drawer>
  );
}
