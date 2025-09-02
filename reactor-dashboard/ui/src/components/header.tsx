import { ModeToggle } from "@/components/mode-toggle";
import JobRunner from "@/components/deploy-form";
import { Button } from "@/components/ui/button";
import { SidebarTrigger } from "@/components/ui/sidebar";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import type { Node } from "@/types";
import { useState } from "react";

interface HeaderProps {
  pollingEnabled: boolean;
  setPollingEnabled: (value: boolean) => void;
  pollOnce: () => void;
  POLL_MS: number;
  lastSweep: string | null;
  nodes: Node[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
}

interface AddNodeDialogProps {
  onAddNode: (node: Node) => void;
}

function AddNodeDialog({ onAddNode }: AddNodeDialogProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [hostname, setHostname] = useState("localhost");
  const [port, setPort] = useState("3000");

  const handleAdd = () => {
    if (!hostname.trim()) {
      alert("Hostname cannot be empty.");
      return;
    }
    if (
      !port ||
      isNaN(Number(port)) ||
      Number(port) < 1 ||
      Number(port) > 65535
    ) {
      alert("Please enter a valid port number (1–65535).");
      return;
    }

    const newNode: Node = {
      hostname,
      port: Number(port),
      data: {
          actors: [],
          loaded_libs: {},
          error: "Not polled yet",
          latencyMs: null,
          lastUpdated: null,
      },
    };

    onAddNode(newNode);
    setHostname("localhost");
    setPort("3000");
    setIsOpen(false);
  };

  return (
    <Dialog open={isOpen} onOpenChange={setIsOpen}>
      <DialogTrigger asChild>
        <Button variant="default">Add Node</Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[400px]">
        <DialogHeader>
          <DialogTitle>Add a New Node</DialogTitle>
          <DialogDescription>
            Enter the hostname and port of the node.
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-col gap-3 mt-4">
          <Input
            placeholder="Hostname (e.g. localhost)"
            value={hostname}
            onChange={(e) => setHostname(e.target.value)}
          />
          <Input
            placeholder="Port (e.g. 3000)"
            value={port}
            onChange={(e) => setPort(e.target.value)}
          />
        </div>

        <DialogFooter className="mt-4">
          <Button onClick={handleAdd}>Add Node</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}


export default function Header({
  pollingEnabled,
  setPollingEnabled,
  pollOnce,
  POLL_MS,
  lastSweep,
  nodes,
  setNodes,
}: HeaderProps) {
  return (
    <header className="flex h-(--header-height) shrink-0 items-center gap-2 border-b transition-[width,height] ease-linear group-has-data-[collapsible=icon]/sidebar-wrapper:h-(--header-height)">
      <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
        <SidebarTrigger className="-ml-1" />
        <Separator
          orientation="vertical"
          className="mx-2 data-[orientation=vertical]:h-4"
        />
        <h1 className="text-xl font-bold">Reactor</h1>
        <div className="ml-auto flex items-center gap-2">
          <Badge variant="outline">
            Poll every {POLL_MS} ms
            {lastSweep && (
              <span className="ml-2">
                Last sweep: {new Date(lastSweep).toLocaleTimeString()}
              </span>
            )}
          </Badge>
          <Separator
            orientation="vertical"
            className="mx-2 data-[orientation=vertical]:h-4"
          />
          <AddNodeDialog
            onAddNode={(node) => setNodes((prev) => [...prev, node])}
          />
          <Separator
            orientation="vertical"
            className="mx-2 data-[orientation=vertical]:h-4"
          />

          <Switch
            id="polling-toggle"
            checked={pollingEnabled}
            onCheckedChange={(checked) => setPollingEnabled(checked)}
          />
          <Label htmlFor="polling-toggle">Polling</Label>
          <Separator
            orientation="vertical"
            className="mx-2 data-[orientation=vertical]:h-4"
          />

          <Button onClick={pollOnce} variant="secondary">
            Refresh now
          </Button>

          <JobRunner nodes={nodes} />

          <ModeToggle></ModeToggle>
        </div>

        <span className="text-sm text-gray-600"></span>
      </div>
    </header>
  );
}
