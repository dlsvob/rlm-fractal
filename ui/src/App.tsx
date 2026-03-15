/**
 * App.tsx — Root component for the fractal database browser.
 *
 * Renders the app shell: a collapsible left sidebar for navigation
 * and a content area that switches between views:
 *   - Citation Graph (D3 force network)
 *   - Papers (searchable table)
 *   - Document Explorer (structure tree for parsed PDFs)
 *   - SQL Console (ad-hoc queries)
 */

import { useState, useEffect } from 'react';
import { Layout, Menu, Typography } from 'antd';
import {
  NodeIndexOutlined,
  FileTextOutlined,
  ReadOutlined,
  ConsoleSqlOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';
import { fetchStats, type KbStats } from './api/client';
import CitationGraph from './components/CitationGraph';
import PaperBrowser from './components/PaperBrowser';
import DocumentExplorer from './components/DocumentExplorer';
import SqlConsole from './components/SqlConsole';

const { Sider, Header, Content } = Layout;
const { Text } = Typography;

function fmt(n: number): string {
  return n.toLocaleString();
}

export default function App() {
  const [activeView, setActiveView] = useState('citation-graph');
  const [collapsed, setCollapsed] = useState(false);
  const [stats, setStats] = useState<KbStats | null>(null);

  useEffect(() => {
    fetchStats().then(setStats).catch(console.error);
  }, []);

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        trigger={null}
        width={200}
        style={{ borderRight: '1px solid #f0f0f0' }}
      >
        <div
          style={{
            height: 48,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            borderBottom: '1px solid #f0f0f0',
            cursor: 'pointer',
          }}
          onClick={() => setCollapsed(!collapsed)}
        >
          {collapsed ? <MenuUnfoldOutlined /> : <MenuFoldOutlined />}
        </div>

        <Menu
          mode="inline"
          selectedKeys={[activeView]}
          onClick={({ key }) => setActiveView(key)}
          items={[
            { key: 'citation-graph', icon: <NodeIndexOutlined />, label: 'Citation Graph' },
            { key: 'papers', icon: <FileTextOutlined />, label: 'Papers' },
            { key: 'documents', icon: <ReadOutlined />, label: 'Documents' },
            { key: 'sql-console', icon: <ConsoleSqlOutlined />, label: 'SQL Console' },
          ]}
          style={{ borderRight: 0 }}
        />
      </Sider>

      <Layout>
        <Header
          style={{
            background: '#fff',
            padding: '0 24px',
            borderBottom: '1px solid #f0f0f0',
            display: 'flex',
            alignItems: 'center',
            gap: 16,
            height: 48,
            lineHeight: '48px',
          }}
        >
          <DatabaseOutlined style={{ fontSize: 18, color: '#1677ff' }} />
          <Text strong style={{ fontSize: 16 }}>fractal.duckdb</Text>
          {stats && (
            <div style={{ display: 'flex', gap: 24, marginLeft: 24 }}>
              <Text type="secondary">{fmt(stats.papers)} papers</Text>
              <Text type="secondary">{fmt(stats.citation_edges)} citations</Text>
              <Text type="secondary">{fmt(stats.chunks)} chunks</Text>
              <Text type="secondary">{fmt(stats.doc_edges)} edges</Text>
              <Text type="secondary">{fmt(stats.pdfs)} PDFs</Text>
            </div>
          )}
        </Header>

        <Content style={{ padding: 24, background: '#fff', overflow: 'auto' }}>
          {activeView === 'citation-graph' && <CitationGraph />}
          {activeView === 'papers' && <PaperBrowser />}
          {activeView === 'documents' && <DocumentExplorer />}
          {activeView === 'sql-console' && <SqlConsole />}
        </Content>
      </Layout>
    </Layout>
  );
}
