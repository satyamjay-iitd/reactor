"use client";

import { useState } from "react";

import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea"
import { JobController, ManualPlacementManager, type PhysicalOp } from "@/reactor-ctrl";
import { Toaster } from "@/components/ui/sonner";
import { toast } from "sonner"
import type { Node } from "@/types";

type SelectOpProps = {
  items: string[]
  placeholder?: string
  onChange: any
}


export function SelectOp({ items, placeholder = "Select Op", onChange }: SelectOpProps) {
  return (
    <Select onValueChange={onChange}>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {items.map((item) => (
            <SelectItem key={item} value={item}>
              {item}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

type PlaceOnProps = {
  items: string[]
  placeholder?: string
  onChange: any
}

function PlaceOn({ items, placeholder = "Place On", onChange }: PlaceOnProps) {
  return (
    <Select onValueChange={onChange}>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder={placeholder} />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          {items.map((item) => (
            <SelectItem key={item} value={item}>
              {item}
            </SelectItem>
          ))}
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

function RemovePlacement({
  items,
  placement,
  setPlacement,
}: {
  items: Set<string>;
  placement: ManualPlacementManager;
  setPlacement: React.Dispatch<React.SetStateAction<ManualPlacementManager>>;
}) {
  const [selected, setSelected] = useState<string | null>(null);

  return (
    <div className="flex gap-2 items-center">
      <Select value={selected ?? ""} onValueChange={setSelected}>
        <SelectTrigger className="w-[180px]">
          <SelectValue placeholder="Unplace" />
        </SelectTrigger>
        <SelectContent>
          <SelectGroup>
            {[...items].map((item) => (
              <SelectItem key={item} value={item}>
                {item}
              </SelectItem>
            ))}
          </SelectGroup>
        </SelectContent>
      </Select>

      <Button
        variant="destructive"
        disabled={!selected}
        onClick={() => {
          if (selected) {
            console.log("Unplacing:", selected);
            const newPlacement = placement.clone();
            newPlacement.remove_actor(selected);
            setPlacement(newPlacement);
            setSelected(null);
          }
        }}
      >
        UnPlace
      </Button>
    </div>
  );
}

type DialogDemoProps = {
  nodes: Node[]
}

function OpToNodes(nodes: Node[]): Record<string, string[]> {
  const opToNodes: Record<string, string[]> = {}

  for (const node of nodes) {
    for (const ops of Object.values(node.data.loaded_libs)) {
      for (const op of ops){
        if (!opToNodes[op]) {
          opToNodes[op] = []
        }
        opToNodes[op].push(node.hostname)
      }
    }
  }
  return opToNodes
}

function OpToLib(nodes: Node[]): Record<string, string> {
  const opToLib: Record<string, string> = {}

  for (const node of nodes) {
    for (const [lib, ops] of Object.entries(node.data.loaded_libs)) {
      for (const op of ops){
        opToLib[op] = lib;
      }
    }
  }
  return opToLib;
}

export default function JobRunner({ nodes }: DialogDemoProps) {
  const [selectedOp, setSelectedOp] = useState<string | null>(null);
  const [selectedNode, setSelectedNode] = useState<string | null>(null);
  const [actorName, setActorName] = useState<string | null>(null);
  const [numActors, setNumActors] = useState<number>(1);
  const [args, setArgs] = useState<Record<string, any>>({});
  const [rawArgs, setRawArgs] = useState("{}",);
  const [placement, setPlacement] = useState<ManualPlacementManager>(new ManualPlacementManager());
  const [isJsonValid, setIsJsonValid] = useState<boolean>(true);

  const opToNodes = OpToNodes(nodes);
  const opToLib = OpToLib(nodes);
  const allOps = Object.keys(opToNodes);
  const nodesForOp = selectedOp ? opToNodes[selectedOp] ?? [] : [];

  let jc = new JobController(placement);
  for (const node of nodes){
    jc.registerNode(node.hostname, node.hostname, node.port);
  }

  const handlePlace = () => {
    if (!selectedOp) {
      toast.error("Please select an Op");
      return;
    }
    if (!actorName) {
      toast.error("Please Provide Actor Name");
      return;
    }
    if (!selectedNode) {
      toast.error("Please select a Node");
      return;
    }
    if (!isJsonValid) {
      toast.error("Provide valid Json for Op Args");
      return;
    }

    const newPlacement = placement.clone();
    const physicalOps: PhysicalOp[] = [];

    if (numActors == 1){
      const op: PhysicalOp = {
        nodename: selectedNode,
        actorName: `${actorName}`,
        payload: args,
      };
      physicalOps.push(op);
    }else{
      for (let i = 0; i < numActors; i++) {
        const op: PhysicalOp = {
          nodename: selectedNode,
          actorName: `${actorName}${i + 1}`,
          payload: args,
        };
        physicalOps.push(op);
      }
    }

    physicalOps.forEach((op) => newPlacement.add_op(selectedOp, op));

    setPlacement(newPlacement);
  };

  const handleDeploy = () => {
    jc.startJob([...placement.get_ops(opToLib)]);
    toast.success("Job Deployed sucessfully");
  };

  return (
    <Sheet>
      <Toaster position="top-center" richColors/>
      <SheetTrigger asChild>
        <Button>Deploy Job</Button>
      </SheetTrigger>
      <SheetContent className="w-[1000px] sm:w-[1000px]">
        <SheetHeader>
          <SheetTitle>Deploy Job</SheetTitle>
        </SheetHeader>
        <div className="grid flex-1 auto-rows-min gap-6 px-4">
          <SelectOp items={[...allOps]} onChange={setSelectedOp} />

          <div className="grid gap-3">
            <Label htmlFor="job-name">Actor Name Prefix</Label>
            <Input
              id="actor-name"
              name="actor_name"
              value={actorName ?? ""}
              onChange={(e) => setActorName(e.target.value)}
            />
          </div>

          <div className="flex items-center space-x-2">
            <PlaceOn items={nodesForOp} onChange={setSelectedNode} />
            <Input
              type="number"
              placeholder="1"
              className="w-20"
              min={1}
              value={numActors}
              onChange={(e) => setNumActors(Number(e.target.value))}
            />
            <Button onClick={handlePlace}>Place</Button>
          </div>

          <div className="grid gap-3">
            <Label htmlFor="oparg-json">Op Arg JSON</Label>
            <Textarea
              id="oparg-json"
              name="oparg_json"
              value={rawArgs}
              required
              className={isJsonValid ? "" : "border-red-500 focus-visible:ring-red-500"}
              onChange={(e) => {
                const text = e.target.value;
                setRawArgs(text);
                try {
                  const parsed = JSON.parse(text);
                  setArgs(parsed);
                  setIsJsonValid(true);
                } catch {
                  setArgs({});
                  setIsJsonValid(false);
                }
              }}
           />
          </div>

          <div className="flex items-center space-x-2 w-full">
            <RemovePlacement
              items={placement.get_actors()}
              placement={placement}
              setPlacement={setPlacement}
            />
          </div>

          <div className="grid gap-3">
            <Label htmlFor="job-json">Job JSON</Label>
            <Textarea
              id="job-json"
              name="job_json"
              disabled required value={placement.mapToJson()}
            />
          </div>

          <div className="grid gap-3">
            <Label htmlFor="job-name">Job Name</Label>
            <Input id="job-name" name="job_name" defaultValue="" />
          </div>


        </div>
        <SheetFooter>
          <Button variant="secondary">Load</Button>
          <Button variant="secondary">Save changes</Button>
          <Button type="submit" onClick={handleDeploy}>Deploy</Button>
          <SheetClose asChild>
            <Button variant="outline">Close</Button>
          </SheetClose>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
