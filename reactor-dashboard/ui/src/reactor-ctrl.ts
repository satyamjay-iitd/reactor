import { DefaultApi, Configuration } from "@reactor/api-client";
import type { RemoteActorInfo, SpawnArgs } from "@reactor/api-client";


export interface LogicalOp {
  name: string;
  libName: string;
}

export interface PhysicalOp {
  nodename: string;
  actorName: string;
  payload: Record<string, any>;
}

export interface PlacementManager {
  place(opInfo: LogicalOp): PhysicalOp[];
}


export class ManualPlacementManager implements PlacementManager {
  private map: Map<string, PhysicalOp[]>;
  private actors: Set<string>;

  constructor() {
    this.map = new Map();
    this.actors = new Set();
  }

  add_op(node: string, op: PhysicalOp){
    if(this.actors.has(op.actorName)){
      this.remove_op(node, op.actorName);
    }
    if (!this.map.has(node)) {
      this.map.set(node, []);
    }
    this.map.get(node)!.push(op);
    this.actors.add(op.actorName);
  }

  remove_op(node: string, op: string): void {
    const ops = this.map.get(node);
    if (!ops) return;

    const index = ops.findIndex((item) => item.actorName === op);
    if (index !== -1) {
      ops.splice(index, 1);
    }
    if (ops.length === 0) {
      this.map.delete(node);
    }
    this.actors.delete(op);
  }

  remove_actor(actor: string): void {
    for (const [node, ops] of this.map.entries()) {
      if (ops.some(op => op.actorName === actor)) {
        this.remove_op(node, actor);
      }
    }
  }

  place(opInfo: LogicalOp): PhysicalOp[] {
    console.log(this.map);
    const ops = this.map.get(opInfo.name);
    if (!ops) {
      throw new Error(`No physical ops found for logical op: ${opInfo.name}`);
    }
    return ops;
  }

  clone(): ManualPlacementManager {
    const newMap = new ManualPlacementManager();;

    for (const [key, ops] of this.map.entries()) {
      newMap.map.set(key, [...ops]);
      for (const op of ops) {
        newMap.actors.add(op.actorName);
      }
    }

    return newMap;
  }

  mapToJson(): string {
    const obj: Record<string, PhysicalOp[]> = {};
    this.map.forEach((value, key) => {
      obj[key] = value;
    });
    return JSON.stringify(obj, null, 2);
  }

  get_actors(): Set<string> {
    return this.actors;  
  }

  get_ops(opToLib: Record<string, string>): Set<LogicalOp> {
    const ops = Object.entries(opToLib).map(([name, libName]) => ({
      name,
      libName,
    }));

    return new Set(ops);
  }

}

export class NodeHandle {
  hostname: string;
  clientConfig: Configuration;
  actors: RemoteActorInfo[] = [];
  loadedLibs: string[] = [];
  api: DefaultApi;

  constructor(hostname: string, clientConfig: Configuration) {
    this.hostname = hostname;
    this.clientConfig = clientConfig;
    this.api = new DefaultApi(clientConfig);
  }

  // Place an actor on this node
  async place(logicalOp: { name: string; libName: string }, physicalOp: { actorName: string; payload: any }): Promise<RemoteActorInfo> {
    const spawnArgs: SpawnArgs = {
      actor_name: physicalOp.actorName,
      operator_name: logicalOp.name,
      lib_name: logicalOp.libName,
      payload: physicalOp.payload,
    };
    const response = await this.api.startActor(spawnArgs);
    const remoteActorInfo: RemoteActorInfo = response.data;
    remoteActorInfo.hostname = this.hostname;
    this.actors.push(remoteActorInfo);
    return remoteActorInfo;
  }

  // Notify that a remote actor was added
  async notifyRemoteActorAdded(remoteActor: RemoteActorInfo) {
    await this.api.actorAdded(remoteActor);
  }

  // Stop all actors on this node
  async stopAllActors() {
    await this.api.stopAllActors();
  }
}


export class JobController<PM extends PlacementManager> {
  pm: PM;
  nodes: Map<string, NodeHandle> = new Map();

  constructor(pm: PM) {
    this.pm = pm;
  }

  registerNode(name: string, hostname: string, port: number) {
    const clientConfig = new Configuration({
      basePath: `http://${hostname}:${port}`
    });
    this.nodes.set(
      name,
      new NodeHandle(hostname, clientConfig)
    );
  }

  async startJob(ops: LogicalOp[]) {
    for (const op of ops) {
      const physicalOps: PhysicalOp[] = this.pm.place(op);

      for (const physicalOp of physicalOps) {
        console.info("Starting Physical op:", physicalOp);

        const nodeHandle = this.nodes.get(physicalOp.nodename);
        if (!nodeHandle) {
          throw new Error("Node must be registered before placement");
        }

        const remoteActorInfo = await nodeHandle.place(op, physicalOp);

        // Notify other nodes asynchronously
        const handles = Array.from(this.nodes.entries())
          .filter(([k]) => k !== physicalOp.nodename)
          .map(([_, node]) => node.notifyRemoteActorAdded(remoteActorInfo));

        await Promise.all(handles);
      }
    }
  }

  async stopJob() {
    // Pop nodes one by one
    const nodesArray = Array.from(this.nodes.values());
    for (const nodeHandle of nodesArray) {
      await nodeHandle.stopAllActors();
    }
    this.nodes.clear();
  }
}

