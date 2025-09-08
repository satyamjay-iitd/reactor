import { useEffect, useMemo, useRef, useState} from "react";
import "./App.css";
import type { Node, NodeData } from "@/types";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/app-sidebar";
import Header from "@/components/header";
import NodeCard from "@/components/node-card";
import { Badge } from "@/components/ui/badge";
import { ThemeProvider } from "@/components/theme-provider";
import { DefaultApi, Configuration } from "@reactor/api-client";

const POLL_MS: number = Number(import.meta.env.VITE_POLL_MS ?? 5000);

function App() {
  const [pollingEnabled, setPollingEnabled] = useState<boolean>(true);
  const [nodes, setNodes] = useState<Node[]>([]);
  const nodesRef = useRef<Node[]>(nodes);
  const [lastSweep, setLastSweep] = useState<string | null>(null);
  const timerRef = useRef<NodeJS.Timeout | null>(null);
  useEffect(() => {
    nodesRef.current = nodes;
  }, [nodes]);

  const handleDeleteNode = (hostname: string) => {
    setNodes((prev) => prev.filter((n) => n.hostname !== hostname));
  };

  const pollOnce = async () => {
    const updatedNodes = await Promise.all(
      nodesRef.current.map(async (node) => {
        const config = new Configuration({
          basePath: `http://${node.hostname}:${node.port}`,
        });
        const api = new DefaultApi(config);

        try {
          const res = await api.getStatus();

          const newData: NodeData = {
            actors: Array.isArray(res.data.actors) ? res.data.actors : [],
            loaded_libs: res.data.loaded_libs,
            error: null,
            latencyMs: null,
            lastUpdated: new Date().toISOString(),
          };
          return { ...node, data: newData };
        } catch (error: any) {
          const newData: NodeData = {
            actors: [],
            loaded_libs: {},
            error: error.message ?? "Unknown error",
            latencyMs: null,
            lastUpdated: new Date().toISOString(),
          };
          return { ...node, data: newData };
        }
      }),
    );
    setNodes(updatedNodes);
    setLastSweep(new Date().toISOString());
  };

  useEffect(() => {
    if (!pollingEnabled) return;
    pollOnce();
    timerRef.current = setInterval(pollOnce, POLL_MS);
    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
    };
  }, [pollingEnabled]);

  const totals = useMemo(() => {
    let up = 0;
    let down = 0;

    nodes.forEach((node) => {
      if (!node.data || node.data.error) down++;
      else up++;
    });

    return { up, down, total: nodes.length };
  }, [nodes]);

  const totalActors = useMemo(
    () => nodes.reduce((sum, node) => sum + (node.data?.actors.length || 0), 0),
    [nodes],
  );

  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <SidebarProvider
        style={
          {
            "--sidebar-width": "calc(var(--spacing) * 72)",
            "--header-height": "calc(var(--spacing) * 12)",
          } as React.CSSProperties
        }
      >
        <AppSidebar variant="inset" />
        <SidebarInset>
          {/* Header */}
          <Header
            pollingEnabled={pollingEnabled}
            setPollingEnabled={setPollingEnabled}
            pollOnce={pollOnce}
            POLL_MS={POLL_MS}
            nodes={nodes}
            lastSweep={lastSweep}
            setNodes={setNodes}
          />

          {/* Main content */}
          <div className="flex flex-1 flex-col">
            <div className="@container/main flex flex-1 flex-col gap-2">
              <div className="m-4 flex flex-wrap items-center gap-2 text-sm">
                <Badge>Online: {totals.up}</Badge>
                <Badge variant="destructive">Offline: {totals.down}</Badge>
                <Badge variant="secondary">Total: {totals.total}</Badge>
                <Badge variant="secondary">Total Actors: {totalActors}</Badge>
              </div>

              {nodes.map((node) => (
                <NodeCard
                  key={node.hostname}
                  node={node}
                  handleDeleteNode={handleDeleteNode}
                />
              ))}
            </div>
          </div>
        </SidebarInset>
      </SidebarProvider>
    </ThemeProvider>
  );
}

export default App;
