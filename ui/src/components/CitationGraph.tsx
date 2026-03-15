/**
 * CitationGraph.tsx — D3 force-directed graph of the citation network.
 *
 * Ported from rlm-pipe-ui's CitationGraph component. Each paper is a node,
 * each citation link is an edge. Nodes are sized by citation count, colored
 * by relevance score, and seed papers get a golden ring. Papers with PDFs
 * get a green ring.
 *
 * Interactions: click a node to open paper detail, drag nodes to reposition,
 * scroll to zoom, pan by dragging the background.
 */

import { useRef, useEffect, useState, useCallback } from 'react';
import { Spin, Empty } from 'antd';
import * as d3 from 'd3';
import {
  fetchCitationGraph,
  fetchPaperDetail,
  type CitationGraphData,
  type GraphNode,
  type GraphEdge,
  type PaperDetail,
} from '../api/client';
import PaperDetailDrawer from './PaperDetailDrawer';

/* D3 simulation needs mutable x/y/vx/vy fields on each node */
interface SimNode extends GraphNode, d3.SimulationNodeDatum {}

/* Edges reference SimNodes after the simulation resolves string IDs */
interface SimEdge extends d3.SimulationLinkDatum<SimNode> {
  source: SimNode | string;
  target: SimNode | string;
}

export default function CitationGraph() {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  const [graphData, setGraphData] = useState<CitationGraphData | null>(null);
  const [loading, setLoading] = useState(true);
  const [selectedPaper, setSelectedPaper] = useState<PaperDetail | null>(null);

  /* Fetch the full citation graph on mount */
  useEffect(() => {
    fetchCitationGraph()
      .then(setGraphData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  /* When a node is clicked, fetch that paper's detail and show the drawer */
  const handleNodeClick = useCallback((node: SimNode) => {
    fetchPaperDetail(node.id)
      .then(setSelectedPaper)
      .catch(console.error);
  }, []);

  /* === D3 rendering === */
  useEffect(() => {
    if (!graphData || !svgRef.current) return;
    if (graphData.nodes.length === 0) return;

    const svgEl = svgRef.current;
    const container = svgEl.parentElement;
    const width = container?.clientWidth ?? 900;
    const height = container?.clientHeight ?? 600;

    /* Clear any previous render */
    d3.select(svgEl).selectAll('*').remove();

    const svg = d3.select(svgEl)
      .attr('width', width)
      .attr('height', height)
      .attr('viewBox', `0 0 ${width} ${height}`);

    /* Zoomable group — all nodes and edges live inside this <g> */
    const g = svg.append('g');

    const zoom = d3.zoom<SVGSVGElement, unknown>()
      .scaleExtent([0.1, 8])
      .on('zoom', (event: d3.D3ZoomEvent<SVGSVGElement, unknown>) => {
        g.attr('transform', event.transform.toString());
      });
    svg.call(zoom);

    /* Deep-copy node data so D3 can mutate x/y without touching React state */
    const nodes: SimNode[] = graphData.nodes.map((n) => ({ ...n }));
    const nodeMap = new Map(nodes.map((n) => [n.id, n]));

    /* Only keep edges where both endpoints exist in the node set */
    const edges: SimEdge[] = graphData.edges
      .filter((e: GraphEdge) => nodeMap.has(e.source) && nodeMap.has(e.target))
      .map((e: GraphEdge) => ({ source: e.source, target: e.target }));

    /* Color scale: relevance 0 → blue, 0.5 → orange, 1 → red */
    const colorScale = d3.scaleLinear<string>()
      .domain([0, 0.5, 1])
      .range(['#4a90d9', '#f5a623', '#d94a4a'])
      .clamp(true);

    /* Size scale: citation count → node radius (sqrt so area is proportional) */
    const maxCitations = d3.max(nodes, (d) => d.citation_count) ?? 10;
    const sizeScale = d3.scaleSqrt()
      .domain([0, maxCitations])
      .range([4, 20]);

    /* Force simulation */
    const simulation = d3.forceSimulation<SimNode>(nodes)
      .force('link', d3.forceLink<SimNode, SimEdge>(edges).id((d) => d.id).distance(60).strength(0.3))
      .force('charge', d3.forceManyBody<SimNode>().strength(-80))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collide', d3.forceCollide<SimNode>().radius((d) => sizeScale(d.citation_count) + 2));

    /* Draw edges as lines */
    const link = g.append('g')
      .attr('class', 'edges')
      .selectAll('line')
      .data(edges)
      .join('line')
      .attr('stroke', '#ccc')
      .attr('stroke-opacity', 0.3)
      .attr('stroke-width', 1);

    /* Draw nodes as groups (so we can stack circles for rings) */
    const node = g.append('g')
      .attr('class', 'nodes')
      .selectAll<SVGGElement, SimNode>('g')
      .data(nodes)
      .join('g')
      .attr('cursor', 'pointer')
      .on('click', (_event, d) => handleNodeClick(d));

    /* Seed paper ring — golden outline behind the main circle */
    node.filter((d) => d.is_seed)
      .append('circle')
      .attr('r', (d) => sizeScale(d.citation_count) + 3)
      .attr('fill', 'none')
      .attr('stroke', '#faad14')
      .attr('stroke-width', 2.5);

    /* PDF available ring — green outline (slightly larger than seed ring) */
    node.filter((d) => d.has_pdf && !d.is_seed)
      .append('circle')
      .attr('r', (d) => sizeScale(d.citation_count) + 3)
      .attr('fill', 'none')
      .attr('stroke', '#52c41a')
      .attr('stroke-width', 1.5);

    /* Main circle — sized by citations, colored by relevance */
    node.append('circle')
      .attr('r', (d) => sizeScale(d.citation_count))
      .attr('fill', (d) => colorScale(d.relevance_score))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1);

    /* Tooltip (positioned absolutely over the SVG container) */
    const tooltip = d3.select(tooltipRef.current);

    node
      .on('mouseenter', (event, d) => {
        tooltip
          .style('display', 'block')
          .style('left', `${event.offsetX + 12}px`)
          .style('top', `${event.offsetY - 10}px`)
          .html(
            `<strong>${d.title}</strong><br/>` +
            `Year: ${d.year ?? 'N/A'}<br/>` +
            `Citations: ${d.citation_count}<br/>` +
            `Relevance: ${(d.relevance_score * 100).toFixed(0)}%` +
            (d.is_seed ? '<br/><em>Seed paper</em>' : '') +
            (d.has_pdf ? '<br/><em>PDF available</em>' : ''),
          );
      })
      .on('mousemove', (event) => {
        tooltip
          .style('left', `${event.offsetX + 12}px`)
          .style('top', `${event.offsetY - 10}px`);
      })
      .on('mouseleave', () => {
        tooltip.style('display', 'none');
      });

    /* Drag behavior — pin the node while dragging, release when done */
    const drag = d3.drag<SVGGElement, SimNode>()
      .on('start', (event, d) => {
        if (!event.active) simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
      })
      .on('drag', (event, d) => {
        d.fx = event.x;
        d.fy = event.y;
      })
      .on('end', (event, d) => {
        if (!event.active) simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
      });

    node.call(drag);

    /* Update positions on each tick of the simulation */
    simulation.on('tick', () => {
      link
        .attr('x1', (d) => (d.source as SimNode).x ?? 0)
        .attr('y1', (d) => (d.source as SimNode).y ?? 0)
        .attr('x2', (d) => (d.target as SimNode).x ?? 0)
        .attr('y2', (d) => (d.target as SimNode).y ?? 0);

      node.attr('transform', (d) => `translate(${d.x ?? 0},${d.y ?? 0})`);
    });

    /* Stop simulation on unmount to prevent memory leaks */
    return () => {
      simulation.stop();
    };
  }, [graphData, handleNodeClick]);

  if (loading) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: 600 }}>
        <Spin size="large" tip="Loading citation graph..." />
      </div>
    );
  }

  if (!graphData || graphData.nodes.length === 0) {
    return <Empty description="No citation graph data available" />;
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Legend explaining visual encodings */}
      <div style={{ display: 'flex', gap: 24, alignItems: 'center', flexWrap: 'wrap', fontSize: 13 }}>
        <span>
          <svg width="16" height="16" style={{ verticalAlign: 'middle' }}>
            <circle cx="8" cy="8" r="6" fill="#4a90d9" stroke="#fff" strokeWidth="1" />
          </svg>{' '}
          Low relevance
        </span>
        <span>
          <svg width="16" height="16" style={{ verticalAlign: 'middle' }}>
            <circle cx="8" cy="8" r="6" fill="#d94a4a" stroke="#fff" strokeWidth="1" />
          </svg>{' '}
          High relevance
        </span>
        <span>
          <svg width="22" height="22" style={{ verticalAlign: 'middle' }}>
            <circle cx="11" cy="11" r="9" fill="none" stroke="#faad14" strokeWidth="2.5" />
            <circle cx="11" cy="11" r="6" fill="#888" stroke="#fff" strokeWidth="1" />
          </svg>{' '}
          Seed paper
        </span>
        <span>
          <svg width="22" height="22" style={{ verticalAlign: 'middle' }}>
            <circle cx="11" cy="11" r="9" fill="none" stroke="#52c41a" strokeWidth="1.5" />
            <circle cx="11" cy="11" r="6" fill="#888" stroke="#fff" strokeWidth="1" />
          </svg>{' '}
          Has PDF
        </span>
        <span style={{ color: '#999' }}>
          Node size = citation count | Scroll to zoom | Drag nodes | Click for detail
        </span>
      </div>

      {/* Graph container */}
      <div
        style={{
          position: 'relative',
          width: '100%',
          height: 'calc(100vh - 180px)',
          border: '1px solid #f0f0f0',
          borderRadius: 8,
          overflow: 'hidden',
          background: '#fafafa',
        }}
      >
        <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />

        {/* Tooltip overlay — positioned by D3 event handlers */}
        <div
          ref={tooltipRef}
          style={{
            display: 'none',
            position: 'absolute',
            pointerEvents: 'none',
            background: 'rgba(0,0,0,0.85)',
            color: '#fff',
            padding: '8px 12px',
            borderRadius: 6,
            fontSize: 12,
            lineHeight: 1.5,
            maxWidth: 320,
            zIndex: 10,
          }}
        />
      </div>

      {/* Paper detail drawer — opens when a node is clicked */}
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
