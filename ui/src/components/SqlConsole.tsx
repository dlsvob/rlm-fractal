/**
 * SqlConsole.tsx — Interactive SQL query interface for ad-hoc database exploration.
 *
 * Provides a textarea for writing SQL queries and displays results in a table.
 * Only SELECT/WITH/EXPLAIN statements are allowed (enforced server-side).
 * Ctrl+Enter or the Execute button runs the query.
 */

import { useState, useCallback } from 'react';
import { Input, Button, Table, Typography, Alert, Space } from 'antd';
import { PlayCircleOutlined } from '@ant-design/icons';
import { executeQuery, type SqlResult } from '../api/client';

const { Text } = Typography;
const { TextArea } = Input;

export default function SqlConsole() {
  const [sql, setSql] = useState("SELECT p.title, p.year, p.citation_count\nFROM papers p\nORDER BY p.citation_count DESC\nLIMIT 20");
  const [result, setResult] = useState<SqlResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const runQuery = useCallback(() => {
    if (!sql.trim()) return;
    setLoading(true);
    setError(null);
    executeQuery(sql)
      .then(setResult)
      .catch((err) => { setError(err.message); setResult(null); })
      .finally(() => setLoading(false));
  }, [sql]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.ctrlKey && e.key === 'Enter') {
        e.preventDefault();
        runQuery();
      }
    },
    [runQuery],
  );

  /* Build Ant Design table columns from the query result */
  const columns = result?.columns.map((col) => ({
    title: col,
    dataIndex: col,
    key: col,
    ellipsis: true,
    render: (val: unknown) => {
      if (val === null || val === undefined) return <Text type="secondary">NULL</Text>;
      if (typeof val === 'object') return <Text code>{JSON.stringify(val)}</Text>;
      return String(val);
    },
  })) ?? [];

  /* Convert row arrays to objects keyed by column name */
  const dataSource = result?.rows.map((row, i) => {
    const obj: Record<string, unknown> = { _key: i };
    result.columns.forEach((col, j) => { obj[col] = row[j]; });
    return obj;
  }) ?? [];

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <div>
        <TextArea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={handleKeyDown}
          rows={5}
          style={{ fontFamily: 'monospace', fontSize: 13 }}
          placeholder="Enter a SELECT query..."
        />
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 8 }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Ctrl+Enter to execute. Read-only (SELECT/WITH/EXPLAIN only).</Text>
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            onClick={runQuery}
            loading={loading}
          >
            Execute
          </Button>
        </div>
      </div>

      {error && <Alert message="Query Error" description={error} type="error" closable />}

      {result && (
        <div>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {result.row_count} row{result.row_count !== 1 ? 's' : ''} returned
          </Text>
          <Table
            columns={columns}
            dataSource={dataSource}
            rowKey="_key"
            size="small"
            pagination={{ pageSize: 50, showSizeChanger: true }}
            scroll={{ x: 'max-content' }}
            style={{ marginTop: 8 }}
          />
        </div>
      )}
    </div>
  );
}
