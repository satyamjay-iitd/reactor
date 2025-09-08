import {
  Card,
  CardHeader,
  CardContent,
  CardTitle,
  CardDescription,
  CardAction,
  CardFooter,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Trash2 } from "lucide-react";
import type { Node } from "@/types";


interface NodeCardProps {
  node: Node;
  handleDeleteNode: (hostname: string) => void;
}

export default function NodeCard({ node, handleDeleteNode }: NodeCardProps) {
  return (
    <div className="*:data-[slot=card]:from-primary/5 *:data-[slot=card]:to-card dark:*:data-[slot=card]:bg-card grid grid-cols-1 gap-4 px-4 *:data-[slot=card]:bg-gradient-to-t *:data-[slot=card]:shadow-xs lg:px-6 @xl/main:grid-cols-2 @5xl/main:grid-cols-4">
      <Card className="@container/card">
        <CardHeader>
          <CardTitle className="text-2xl font-semibold tabular-nums @[250px]/card:text-3xl">
            {node.hostname}
          </CardTitle>

          <CardDescription>
            <Badge
              variant={!node.data.error ? "default" : "destructive"}
              className="text-xs font-medium"
            >
              {!node.data.error ? "Online" : "Offline"}
            </Badge>
          </CardDescription>

          <CardAction>
            <Button
              variant="destructive"
              onClick={() => handleDeleteNode(node.hostname)}
            >
              <Trash2 className="w-4 h-4" />
            </Button>
          </CardAction>
        </CardHeader>

        <CardContent>
          <div className="mt-1 flex justify-between gap-4 flex-wrap">
            {/* Actors */}
            <div className="flex-1 min-w-[45%] text-left">
              <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-2">
                Actors
              </h3>
              {node.data.actors.length > 0 ? (
                <div className="flex flex-wrap justify-start gap-2">
                  {node.data.actors.map((actor, idx) => (
                    <Badge key={idx} variant="default">
                      {actor}
                    </Badge>
                  ))}
                </div>
              ) : (
                <p className="text-gray-500 italic">No actors</p>
              )}
            </div>

            {/* Loaded Libs */}
            <div className="flex-1 min-w-[45%] text-right">
              <h3 className="font-medium text-gray-700 dark:text-gray-300 mb-2">
                Loaded Libs
              </h3>
              {Object.keys(node.data.loaded_libs).length > 0 ? (
                <div className="flex flex-wrap justify-end gap-2">
                  {Object.keys(node.data.loaded_libs).map((lib, idx) => (
                    <Badge key={idx} variant="secondary">
                      {lib}
                    </Badge>
                  ))}
                </div>
              ) : (
                <p className="text-gray-500 italic">No libs loaded</p>
              )}
            </div>
          </div>
        </CardContent>

        <CardFooter className="flex-col items-start gap-1.5 text-sm">
          <div className="flex justify-between w-full font-medium">
            <div>
              Latency:{" "}
              {node.data && node.data.latencyMs !== null
                ? `${node.data.latencyMs} ms`
                : "—"}
            </div>
            <div>
              Updated:{" "}
              {node.data?.lastUpdated
                ? new Date(node.data.lastUpdated).toLocaleTimeString()
                : "—"}
            </div>
          </div>
        </CardFooter>
      </Card>
    </div>
  );
}
