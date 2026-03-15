/**
 * PaperBrowser.tsx — Paginated, searchable, sortable table of papers.
 *
 * Fetches papers from the backend with server-side pagination, search,
 * organ filtering, and year filtering. Clicking a row opens the paper
 * detail drawer.
 */

import { useEffect, useState, useCallback } from 'react';
import { Table, Input, Select, InputNumber, Tag, Space, Typography } from 'antd';
import { CheckCircleFilled, StarFilled } from '@ant-design/icons';
import type { ColumnsType, TablePaginationConfig } from 'antd/es/table';
import type { SorterResult } from 'antd/es/table/interface';
import {
  fetchPapers,
  fetchPaperDetail,
  fetchStats,
  type Paper,
  type PaperDetail,
  type PaginatedPapers,
  type KbStats,
} from '../api/client';
import PaperDetailDrawer from './PaperDetailDrawer';

const { Search } = Input;
const { Text } = Typography;

/* Color-code organs for visual distinction */
const organColors: Record<string, string> = {
  liver: 'green', kidney: 'blue', heart: 'red', lung: 'cyan',
  brain: 'purple', skin: 'orange', blood: 'magenta', thyroid: 'geekblue',
  adrenal: 'gold', testes: 'lime', spleen: 'volcano',
};

export default function PaperBrowser() {
  const [papersData, setPapersData] = useState<PaginatedPapers | null>(null);
  const [stats, setStats] = useState<KbStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [selectedPaper, setSelectedPaper] = useState<PaperDetail | null>(null);

  /* Filter state */
  const [query, setQuery] = useState('');
  const [organ, setOrgan] = useState<string | undefined>(undefined);
  const [yearMin, setYearMin] = useState<number | undefined>(undefined);
  const [sort, setSort] = useState<string | undefined>(undefined);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(20);

  /* Load stats once (for organ filter options) */
  useEffect(() => {
    fetchStats().then(setStats).catch(console.error);
  }, []);

  /* Organ options for the filter dropdown */
  const organOptions = stats?.organs
    ? Object.keys(stats.organs).map((o) => ({ label: o, value: o }))
    : [];

  /* Fetch papers from the backend whenever filters/pagination change */
  const doFetch = useCallback(() => {
    setLoading(true);
    fetchPapers({
      page,
      per_page: pageSize,
      q: query || undefined,
      sort,
      organ,
      year_min: yearMin,
    })
      .then(setPapersData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [page, pageSize, query, sort, organ, yearMin]);

  useEffect(() => { doFetch(); }, [doFetch]);

  const handleSearch = useCallback((value: string) => {
    setQuery(value);
    setPage(1);
  }, []);

  const handleTableChange = useCallback(
    (pagination: TablePaginationConfig, _filters: Record<string, unknown>, sorter: SorterResult<Paper> | SorterResult<Paper>[]) => {
      setPage(pagination.current ?? 1);
      setPageSize(pagination.pageSize ?? 20);

      const s = Array.isArray(sorter) ? sorter[0] : sorter;
      if (s?.field && s?.order) {
        const dir = s.order === 'descend' ? '-' : '';
        setSort(`${dir}${String(s.field)}`);
      } else {
        setSort(undefined);
      }
    },
    [],
  );

  const handleRowClick = useCallback((paper: Paper) => {
    fetchPaperDetail(paper.paper_id)
      .then(setSelectedPaper)
      .catch(console.error);
  }, []);

  const columns: ColumnsType<Paper> = [
    {
      title: 'Title',
      dataIndex: 'title',
      key: 'title',
      ellipsis: true,
      width: '35%',
      sorter: true,
      render: (title: string, record: Paper) => (
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          {record.is_seed && <StarFilled style={{ color: '#faad14', fontSize: 12, flexShrink: 0 }} />}
          <Text ellipsis style={{ cursor: 'pointer', color: '#1677ff' }}>{title}</Text>
        </div>
      ),
    },
    {
      title: 'Year',
      dataIndex: 'year',
      key: 'year',
      width: 80,
      sorter: true,
      render: (year: number | null) => year ?? <Text type="secondary">--</Text>,
    },
    {
      title: 'Venue',
      dataIndex: 'venue',
      key: 'venue',
      ellipsis: true,
      width: '15%',
      render: (venue: string) => venue || <Text type="secondary">--</Text>,
    },
    {
      title: 'Citations',
      dataIndex: 'citation_count',
      key: 'citation_count',
      width: 90,
      sorter: true,
      align: 'right',
    },
    {
      title: 'Relevance',
      dataIndex: 'relevance_score',
      key: 'relevance_score',
      width: 100,
      sorter: true,
      align: 'right',
      render: (score: number) => {
        const pct = (score * 100).toFixed(0);
        const color = score >= 0.7 ? '#52c41a' : score >= 0.4 ? '#faad14' : '#999';
        return <Text style={{ color }}>{pct}%</Text>;
      },
    },
    {
      title: 'Seed',
      dataIndex: 'is_seed',
      key: 'is_seed',
      width: 60,
      align: 'center',
      render: (isSeed: boolean) =>
        isSeed ? <CheckCircleFilled style={{ color: '#52c41a' }} /> : null,
    },
    {
      title: 'Organs',
      dataIndex: 'organs',
      key: 'organs',
      width: '15%',
      render: (organs: string[]) => (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
          {(organs ?? []).map((o) => (
            <Tag key={o} color={organColors[o.toLowerCase()] ?? 'default'} style={{ fontSize: 11, margin: 0 }}>
              {o}
            </Tag>
          ))}
        </div>
      ),
    },
  ];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Filter row */}
      <div style={{ display: 'flex', gap: 12, flexWrap: 'wrap', alignItems: 'center' }}>
        <Search
          placeholder="Search papers..."
          allowClear
          onSearch={handleSearch}
          style={{ width: 300 }}
        />
        <Select
          placeholder="Filter by organ"
          allowClear
          options={organOptions}
          value={organ}
          onChange={(val) => { setOrgan(val); setPage(1); }}
          style={{ width: 180 }}
        />
        <Space size={4}>
          <Text type="secondary" style={{ fontSize: 13 }}>Year from:</Text>
          <InputNumber
            placeholder="e.g. 2015"
            min={1900}
            max={2030}
            value={yearMin}
            onChange={(val) => { setYearMin(val ?? undefined); setPage(1); }}
            style={{ width: 100 }}
          />
        </Space>
      </div>

      {/* Papers table */}
      <Table<Paper>
        columns={columns}
        dataSource={papersData?.items ?? []}
        rowKey="paper_id"
        loading={loading}
        size="small"
        onChange={handleTableChange as never}
        onRow={(record) => ({
          onClick: () => handleRowClick(record),
          style: { cursor: 'pointer' },
        })}
        pagination={{
          current: papersData?.page ?? page,
          pageSize: papersData?.per_page ?? pageSize,
          total: papersData?.total ?? 0,
          showSizeChanger: true,
          pageSizeOptions: ['10', '20', '50', '100'],
          showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} papers`,
        }}
        scroll={{ x: 900 }}
      />

      {/* Paper detail drawer */}
      <PaperDetailDrawer
        paper={selectedPaper}
        onClose={() => setSelectedPaper(null)}
        onNavigate={(paperId) => {
          fetchPaperDetail(paperId)
            .then(setSelectedPaper)
            .catch(console.error);
        }}
      />
    </div>
  );
}
