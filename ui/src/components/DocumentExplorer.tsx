/**
 * DocumentExplorer.tsx — Browse document structure as an expandable tree.
 *
 * Pick a paper via search, then see its parsed structure: sections as
 * expandable nodes, each containing their child chunks. Shows edge
 * counts and parse quality. Click a chunk to highlight cross-references.
 */

import { useState, useCallback, useEffect } from 'react';
import { Input, Select, Tree, Tag, Typography, Spin, Empty, Card, Statistic, Row, Col, Badge } from 'antd';
import {
  FileTextOutlined,
  AlignLeftOutlined,
  TableOutlined,
  PictureOutlined,
  UnorderedListOutlined,
  LinkOutlined,
} from '@ant-design/icons';
import type { DataNode } from 'antd/es/tree';
import {
  fetchPapers,
  fetchPaperStructure,
  type Paper,
  type StructureData,
} from '../api/client';

const { Search } = Input;
const { Text } = Typography;

/* Icon for each chunk type */
const chunkIcon: Record<string, React.ReactNode> = {
  paragraph: <AlignLeftOutlined style={{ color: '#666' }} />,
  heading: <FileTextOutlined style={{ color: '#1677ff' }} />,
  caption: <PictureOutlined style={{ color: '#fa8c16' }} />,
  table_caption: <TableOutlined style={{ color: '#722ed1' }} />,
  table_cell: <TableOutlined style={{ color: '#722ed1' }} />,
  reference: <LinkOutlined style={{ color: '#13c2c2' }} />,
  list_item: <UnorderedListOutlined style={{ color: '#666' }} />,
};

export default function DocumentExplorer() {
  /* Paper search and selection */
  const [searchResults, setSearchResults] = useState<Paper[]>([]);
  const [selectedPaperId, setSelectedPaperId] = useState<string | null>(null);
  const [selectedTitle, setSelectedTitle] = useState('');
  const [searching, setSearching] = useState(false);

  /* Document structure data */
  const [structure, setStructure] = useState<StructureData | null>(null);
  const [loading, setLoading] = useState(false);

  /* Search for papers */
  const handleSearch = useCallback((query: string) => {
    if (!query.trim()) return;
    setSearching(true);
    fetchPapers({ q: query, per_page: 20 })
      .then((res) => setSearchResults(res.items))
      .catch(console.error)
      .finally(() => setSearching(false));
  }, []);

  /* Load structure when a paper is selected */
  useEffect(() => {
    if (!selectedPaperId) return;
    setLoading(true);
    setStructure(null);
    fetchPaperStructure(selectedPaperId)
      .then(setStructure)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [selectedPaperId]);

  /* Build Ant Design Tree data from structure */
  const treeData: DataNode[] = structure?.tree.map((node, i) => ({
    key: `h-${node.chunk_id}`,
    title: (
      <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
        <Text strong style={{ fontSize: 13 }}>{node.text.slice(0, 100)}</Text>
        {node.section_name && (
          <Tag color="blue" style={{ fontSize: 10, margin: 0 }}>{node.section_name}</Tag>
        )}
        <Badge
          count={node.children.length}
          style={{ backgroundColor: '#d9d9d9', fontSize: 10 }}
        />
      </div>
    ),
    children: node.children.map((child) => ({
      key: `c-${child.chunk_id}`,
      icon: chunkIcon[child.chunk_type] ?? <FileTextOutlined />,
      title: (
        <div style={{ display: 'flex', gap: 6, alignItems: 'baseline' }}>
          <Tag
            style={{ fontSize: 9, margin: 0, lineHeight: '14px' }}
            color={child.chunk_type === 'paragraph' ? 'default' : child.chunk_type === 'reference' ? 'cyan' : 'orange'}
          >
            {child.chunk_type}
          </Tag>
          <Text type="secondary" style={{ fontSize: 10 }}>p.{child.page}</Text>
          <Text style={{ fontSize: 12 }} ellipsis>
            {child.preview}
          </Text>
        </div>
      ),
      isLeaf: true,
    })),
  })) ?? [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Paper search */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
        <Search
          placeholder="Search papers to explore..."
          allowClear
          onSearch={handleSearch}
          loading={searching}
          style={{ width: 400 }}
        />
        {searchResults.length > 0 && (
          <Select
            placeholder="Select a paper..."
            style={{ flex: 1, minWidth: 300 }}
            value={selectedPaperId ?? undefined}
            onChange={(val) => {
              setSelectedPaperId(val);
              const paper = searchResults.find((p) => p.paper_id === val);
              setSelectedTitle(paper?.title ?? '');
            }}
            showSearch
            filterOption={(input, option) =>
              (option?.label as string)?.toLowerCase().includes(input.toLowerCase()) ?? false
            }
            options={searchResults.map((p) => ({
              value: p.paper_id,
              label: `${p.title?.slice(0, 80)} (${p.year ?? '?'})`,
            }))}
          />
        )}
      </div>

      {/* Selected paper header */}
      {selectedTitle && (
        <Text strong style={{ fontSize: 14 }}>{selectedTitle}</Text>
      )}

      {/* Loading state */}
      {loading && (
        <div style={{ display: 'flex', justifyContent: 'center', padding: 48 }}>
          <Spin size="large" tip="Loading document structure..." />
        </div>
      )}

      {/* Edge counts summary */}
      {structure && (
        <Card size="small" bodyStyle={{ padding: '8px 16px' }}>
          <Row gutter={24}>
            <Col>
              <Statistic title="Sections" value={structure.tree.length} valueStyle={{ fontSize: 18 }} />
            </Col>
            {Object.entries(structure.edge_counts).map(([etype, count]) => (
              <Col key={etype}>
                <Statistic title={etype} value={count} valueStyle={{ fontSize: 18 }} />
              </Col>
            ))}
          </Row>
        </Card>
      )}

      {/* Structure tree */}
      {structure && treeData.length > 0 && (
        <Card
          size="small"
          bodyStyle={{ padding: 8, maxHeight: 'calc(100vh - 320px)', overflow: 'auto' }}
        >
          <Tree
            showLine
            showIcon
            defaultExpandedKeys={treeData.slice(0, 5).map((n) => n.key)}
            treeData={treeData}
            style={{ fontSize: 12 }}
          />
        </Card>
      )}

      {structure && treeData.length === 0 && (
        <Empty description="No document structure found. This paper may not have been parsed, or its PDF had no headings." />
      )}

      {!selectedPaperId && !loading && (
        <Empty
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          description="Search for a paper above to explore its document structure"
        />
      )}
    </div>
  );
}
