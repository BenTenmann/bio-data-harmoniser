"use client";
import React from "react";
import Dagre from "@dagrejs/dagre";
import { CustomNode } from "@/components/node";
import {
  Background,
  BackgroundVariant,
  Controls,
  MarkerType,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
} from "reactflow";
import "reactflow/dist/style.css";
import "tailwindcss/tailwind.css";
import { type NodeMetadata } from "@/components/node";

type LoggedNode = {
  id: string;
  name: string;
  data: NodeMetadata;
  upstream_node_ids: string[];
};

type DataExtractionDag = {
  nodes: LoggedNode[];
};

const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));

function DAGCore({ dag, runId }: { dag: DataExtractionDag; runId: string }) {
  const [nodes, setNodes, onNodesChange] = useNodesState(
    dag.nodes.map((node, index) => ({
      id: node.id,
      type: "customNode",
      data: { ...node.data, runId: runId },
      position: { x: 0, y: index * 100 },
    })),
  );
  const [edges, setEdges, onEdgesChange] = useEdgesState(
    dag.nodes.flatMap((node) =>
      node.upstream_node_ids.map((upstreamNodeId) => ({
        id: `${upstreamNodeId}-${node.id}`,
        source: upstreamNodeId,
        target: node.id,
        type: "smoothstep",
        markerEnd: {
          type: MarkerType.ArrowClosed,
        },
        style: {
          strokeWidth: 2,
        },
      })),
    ),
  );
  const nodeTypes = React.useMemo(() => ({ customNode: CustomNode }), []);
  const { fitView } = useReactFlow();
  React.useEffect(() => {
    g.setGraph({ rankdir: "LR", nodesep: 100, ranksep: 200 });
    edges.forEach((edge) => g.setEdge(edge.source, edge.target));
    nodes.forEach((node) => g.setNode(node.id, node as any));
    Dagre.layout(g);
    setNodes([
      ...nodes.map((node) => {
        const pos = g.node(node.id);
        return { ...node, position: { x: pos.x, y: pos.y } };
      }),
    ]);
    setEdges([...edges]);
    window.requestAnimationFrame(() => {
      fitView();
    });
  }, []);
  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      fitView
    >
      <MiniMap />
      <Controls />
      <Background variant={BackgroundVariant.Dots} gap={12} size={1} />
    </ReactFlow>
  );
}

export function DAG({ dag, runId }: { dag: DataExtractionDag; runId: string }) {
  return (
    <div className="h-screen w-screen shadow-inner">
      <ReactFlowProvider>
        <DAGCore dag={dag} runId={runId} />
      </ReactFlowProvider>
    </div>
  );
}
