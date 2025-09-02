export interface NodeData {
  actors: string[];
  loaded_libs: string[];
  available_ops: string[];
  error: string | null;
  latencyMs: number | null;
  lastUpdated: string | null;
}

export interface Node {
  hostname: string;
  port: number;
  data: NodeData;
}
