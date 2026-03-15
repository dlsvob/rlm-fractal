/**
 * App.tsx — Root component for the fractal database browser.
 *
 * Renders the app shell: a collapsible left sidebar for navigation
 * and a content area that switches between the Citation Graph,
 * Papers table, and SQL Console views.
 */

import { useState, useEffect } from 'react';
import { Layout, Menu, Typography } from 'antd';
import {
  NodeIndexOutlined,
  FileTextOutlined,
  ConsoleSqlOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  DatabaseOutlined,
} from '@ant-design/icons';
import { fetchStats, type KbStats } from './api/client';
import CitationGraph from './components/CitationGraph';
import PaperBrowser from './components/PaperBrowser';
import SqlConsole from './components/SqlConsole';

const { Sider, Header, Content } = Layout;
const { Text } = Typography;

/** Format large numbers with commas for display. */
function fmt(n: number): string {
  return n.toLocaleString();
}

export default function App() {
  /* Which view is active — drives the sidebar highlight and content area */
  const [activeView, setActiveView] = useState('citation-graph');
  /* Whether the sidebar is collapsed to icon-only mode */
  const [collapsed, setCollapsed] = useState(false);
  /* Database stats shown in the header bar */
  const [stats, setStats] = useState<KbStats | null>(null);

  /* Load stats once on mount */
  useEffect(() => {
    fetchStats().then(setStats).catch(console.error);
  }, []);

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {/* === Left sidebar === */}
      <Sider
        collapsible
        collapsed={collapsed}
        trigger={null}
        width={200}
        style={{ borderRight: '1px solid #f0f0f0' }}
      >
        {/* Collapse toggle button */}
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

        {/* Navigation menu — each entry maps to a view */}
        <Menu
          mode="inline"
          selectedKeys={[activeView]}
          onClick={({ key }) => setActiveView(key)}
          items={[
            { key: 'citation-graph', icon: <NodeIndexOutlined />, label: 'Citation Graph' },
            { key: 'papers', icon: <FileTextOutlined />, label: 'Papers' },
            { key: 'sql-console', icon: <ConsoleSqlOutlined />, label: 'SQL Console' },
          ]}
          style={{ borderRight: 0 }}
        />
      </Sider>

      <Layout>
        {/* === Header bar with database stats === */}
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
              <Text type="secondary">{fmt(stats.genes)} genes</Text>
              <Text type="secondary">{fmt(stats.claims)} claims</Text>
              <Text type="secondary">{fmt(stats.pdfs)} PDFs</Text>
            </div>
          )}
        </Header>

        {/* === Main content area === */}
        <Content style={{ padding: 24, background: '#fff', overflow: 'auto' }}>
          {activeView === 'citation-graph' && <CitationGraph />}
          {activeView === 'papers' && <PaperBrowser />}
          {activeView === 'sql-console' && <SqlConsole />}
        </Content>
      </Layout>
    </Layout>
  );
}
