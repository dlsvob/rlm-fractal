/**
 * DocumentExplorer.tsx — Browse parsed documents by category with structure tree.
 *
 * Top section: filterable paper list showing parse quality, chunk counts,
 * section counts. Filter by quality tier (tagged/heuristic), search, organ.
 * Bottom section: when a paper is selected, shows its structure tree.
 */

import { useState, useCallback, useEffect } from 'react';
import { Input, Select, Table, Tree, Tag, Typography, Spin, Empty, Card, Statistic, Row, Col, Badge, Space } from 'antd';
import {
  FileTextOutlined,
  AlignLeftOutlined,
  TableOutlined,
  PictureOutlined,
  UnorderedListOutlined,
  LinkOutlined,
} from '@ant-design/icons';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import type { SorterResult } from 'antd/es/table/interface';
import type { DataNode } from 'antd/es/tree';
import {
  fetchDocuments,
  fetchPaperStructure,
  fetchStats,
  type ParsedPaper,
  type PaginatedParsedPapers,
  type StructureData,
  type KbStats,
} from '../api/client';

const { Search } = Input;
const { Text } = Typography;

/* Icon for each chunk type in the tree */
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
  /* --- Paper list state --- */
  const [papersData, setPapersData] = useState<PaginatedParsedPapers | null>(null);
  const [stats, setStats] = useState<KbStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [query, setQuery] = useState('');
  const [quality, setQuality] = useState<string | undefined>(undefined);
  const [organ, setOrgan] = useState<string | undefined>(undefined);
  const [sort, setSort] = useState<string | undefined>(undefined);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(15);

  /* --- Structure tree state --- */
  const [selectedPaperId, setSelectedPaperId] = useState<string | null>(null);
  const [selectedTitle, setSelectedTitle] = useState('');
  const [structure, setStructure] = useState<StructureData | null>(null);
  const [treeLoading, setTreeLoading] = useState(false);

  /* Load stats for organ filter options */
  useEffect(() => {
    fetchStats().then(setStats).catch(console.error);
  }, []);

  const organOptions = stats?.organs
    ? Object.keys(stats.organs).map((o) => ({ label: o, value: o }))
    : [];

  /* Fetch papers when filters change */
  const doFetch = useCallback(() => {
    setLoading(true);
    fetchDocuments({
      page,
      per_page: pageSize,
      q: query || undefined,
      quality,
      organ,
      sort,
    })
      .then(setPapersData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [page, pageSize, query, quality, organ, sort]);

  useEffect(() => { doFetch(); }, [doFetch]);

  /* Load structure when a paper is selected */
  useEffect(() => {
    if (!selectedPaperId) return;
    setTreeLoading(true);
    setStructure(null);
    fetchPaperStructure(selectedPaperId)
      .then(setStructure)
      .catch(console.error)
      .finally(() => setTreeLoading(false));
  }, [selectedPaperId]);

  /* Paper table columns */
  const columns: ColumnsType<ParsedPaper> = [
    {
      title: 'Quality',
      dataIndex: 'parse_quality',
      key: 'parse_quality',
      width: 90,
      render: (q: string) => (
        <Tag color={q === 'tagged' ? 'green' : 'blue'} style={{ fontSize: 11 }}>
          {q}
        </Tag>
      ),
    },
    {
      title: 'Title',
      dataIndex: 'title',
      key: 'title',
      ellipsis: true,
      sorter: true,
      render: (title: string, record: ParsedPaper) => (
        <Text
          ellipsis
          style={{
            cursor: 'pointer',
            color: '#1677ff',
            fontWeight: record.paper_id === selectedPaperId ? 600 : 400,
          }}
        >
          {title}
        </Text>
      ),
    },
    {
      title: 'Year',
      dataIndex: 'year',
      key: 'year',
      width: 60,
      sorter: true,
      render: (y: number | null) => y ?? <Text type="secondary">--</Text>,
    },
    {
      title: 'Chunks',
      dataIndex: 'chunk_count',
      key: 'chunk_count',
      width: 70,
      align: 'right',
      sorter: true,
    },
    {
      title: 'Headings',
      dataIndex: 'heading_count',
      key: 'heading_count',
      width: 80,
      align: 'right',
      sorter: true,
    },
    {
      title: 'Sections',
      dataIndex: 'section_count',
      key: 'section_count',
      width: 80,
      align: 'right',
      sorter: true,
      render: (n: number) => n > 0 ? <Text style={{ color: '#52c41a' }}>{n}</Text> : <Text type="secondary">0</Text>,
    },
    {
      title: 'Pages',
      dataIndex: 'page_count',
      key: 'page_count',
      width: 60,
      align: 'right',
      sorter: true,
    },
    {
      title: 'Tables',
      dataIndex: 'table_cell_count',
      key: 'table_cell_count',
      width: 65,
      align: 'right',
      sorter: true,
      render: (n: number) => n > 0 ? n : <Text type="secondary">0</Text>,
    },
  ];

  /* Build tree data */
  const treeData: DataNode[] = structure?.tree.map((node) => ({
    key: `h-${node.chunk_id}`,
    title: (
      <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
        <Text strong style={{ fontSize: 13 }}>{node.text.slice(0, 100)}</Text>
        {node.section_name && (
          <Tag color="blue" style={{ fontSize: 10, margin: 0 }}>{node.section_name}</Tag>
        )}
        <Badge count={node.children.length} style={{ backgroundColor: '#d9d9d9', fontSize: 10 }} />
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
          <Text style={{ fontSize: 12 }} ellipsis>{child.preview}</Text>
        </div>
      ),
      isLeaf: true,
    })),
  })) ?? [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* === Filter bar === */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
        <Search
          placeholder="Search parsed papers..."
          allowClear
          onSearch={(v) => { setQuery(v); setPage(1); }}
          style={{ width: 300 }}
        />
        <Select
          placeholder="Parse quality"
          allowClear
          value={quality}
          onChange={(v) => { setQuality(v); setPage(1); }}
          style={{ width: 140 }}
          options={[
            { label: 'Tagged', value: 'tagged' },
            { label: 'Heuristic', value: 'heuristic' },
          ]}
        />
        <Select
          placeholder="Organ"
          allowClear
          value={organ}
          onChange={(v) => { setOrgan(v); setPage(1); }}
          style={{ width: 150 }}
          options={organOptions}
          showSearch
        />
      </div>

      {/* === Paper list === */}
      <Table<ParsedPaper>
        columns={columns}
        dataSource={papersData?.items ?? []}
        rowKey="paper_id"
        loading={loading}
        size="small"
        onChange={(pagination, _filters, sorter) => {
          const s = Array.isArray(sorter) ? sorter[0] : sorter as SorterResult<ParsedPaper>;
          if (s?.field && s?.order) {
            const dir = s.order === 'descend' ? '-' : '';
            setSort(`${dir}${String(s.field)}`);
          } else {
            setSort(undefined);
          }
          setPage(pagination.current ?? 1);
          setPageSize(pagination.pageSize ?? 15);
        }}
        onRow={(record) => ({
          onClick: () => {
            setSelectedPaperId(record.paper_id);
            setSelectedTitle(record.title);
          },
          style: {
            cursor: 'pointer',
            background: record.paper_id === selectedPaperId ? '#e6f4ff' : undefined,
          },
        })}
        pagination={{
          current: papersData?.page ?? page,
          pageSize: papersData?.per_page ?? pageSize,
          total: papersData?.total ?? 0,
          showSizeChanger: true,
          pageSizeOptions: ['10', '15', '25', '50'],
          showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} documents`,
        }}
        scroll={{ x: 800 }}
      />

      {/* === Structure tree === */}
      {selectedPaperId && (
        <Card
          size="small"
          title={
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <FileTextOutlined />
              <Text strong style={{ fontSize: 13 }}>{selectedTitle?.slice(0, 80)}</Text>
            </div>
          }
        >
          {treeLoading && (
            <div style={{ display: 'flex', justifyContent: 'center', padding: 32 }}>
              <Spin />
            </div>
          )}

          {structure && (
            <div style={{ marginBottom: 12 }}>
              <Space size={16}>
                {Object.entries(structure.edge_counts).map(([etype, count]) => (
                  <Text key={etype} type="secondary" style={{ fontSize: 12 }}>
                    {etype}: <Text strong>{count.toLocaleString()}</Text>
                  </Text>
                ))}
              </Space>
            </div>
          )}

          {structure && treeData.length > 0 && (
            <div>
              <Tree
                showLine
                showIcon
                defaultExpandedKeys={treeData.slice(0, 3).map((n) => n.key)}
                treeData={treeData}
                style={{ fontSize: 12 }}
              />
            </div>
          )}

          {structure && treeData.length === 0 && (
            <Empty description="No headings found — this paper's structure is flat (all paragraphs, no detected sections)" />
          )}
        </Card>
      )}
    </div>
  );
}
