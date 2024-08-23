<script lang="ts">
  import { formatDistanceToNow } from "date-fns";
  import {
    ArrowLeft,
    CheckCircle2,
    Clock,
    Database,
    Globe,
    Activity,
    ExternalLink,
    RefreshCw,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Badge } from "$lib/components/ui/badge";
  import { Card, CardContent } from "$lib/components/ui/card";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import { getColorFromName } from "../utils";

  export let consumer;

  $: healthColor =
    consumer.health > 90
      ? "text-green-500"
      : consumer.health > 70
        ? "text-yellow-500"
        : "text-red-500";

  function formatTimestamp(timestamp: string) {
    const date = new Date(timestamp);
    return formatDistanceToNow(date, { addSuffix: true });
  }
</script>

<div class="min-h-screen font-sans bg-white">
  <header class="bg-white border-b sticky top-0 z-10">
    <div class="container mx-auto px-4 py-4">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
          <a href="/consumers">
            <Button variant="ghost" size="sm">
              <ArrowLeft class="h-4 w-4" />
            </Button>
          </a>
          <h1 class="text-xl font-semibold">{consumer.name}</h1>
          <Badge
            variant={consumer.status === "active" ? "default" : "secondary"}
          >
            {consumer.status}
          </Badge>
        </div>
        <div class="flex items-center space-x-4">
          <div
            class="hidden md:flex items-center space-x-4 text-sm text-gray-500"
          >
            <div class="flex items-center space-x-2">
              <Clock class="h-4 w-4" />
              <span>Created {formatTimestamp(consumer.inserted_at)}</span>
            </div>
            <div class="flex items-center space-x-2">
              <RefreshCw class="h-4 w-4" />
              <span>Updated {formatTimestamp(consumer.updated_at)}</span>
            </div>
          </div>
          <Button variant="outline" size="sm">Edit</Button>
          <Button
            variant="outline"
            size="sm"
            class="text-red-600 hover:text-red-700">Delete</Button
          >
        </div>
      </div>
    </div>
  </header>

  <div class="container mx-auto px-4 py-8">
    <div class="grid gap-6 md:grid-cols-3 mb-8">
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Health</span>
            <Activity class="h-5 w-5 {healthColor}" />
          </div>
          <div class="flex items-baseline space-x-2">
            <span class="text-2xl font-bold {healthColor}"
              >{consumer.health}%</span
            >
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500"
              >Messages Processed</span
            >
            <CheckCircle2 class="h-5 w-5 text-green-500" />
          </div>
          <span class="text-2xl font-bold"
            >{consumer.messages_processed.toLocaleString()}</span
          >
        </CardContent>
      </Card>
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Avg. Latency</span>
            <Clock class="h-5 w-5 text-blue-500" />
          </div>
          <span class="text-2xl font-bold">{consumer.avg_latency} ms</span>
        </CardContent>
      </Card>
    </div>

    <div class="space-y-6">
      <Card>
        <CardContent class="p-6">
          <h2 class="text-lg font-semibold mb-4">Configuration</h2>
          <div class="grid md:grid-cols-2 gap-4">
            <div>
              <span class="text-sm text-gray-500">ID</span>
              <p class="font-medium">{consumer.id}</p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Message Kind</span>
              <p class="font-medium">{consumer.message_kind}</p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Ack Wait</span>
              <p class="font-medium">{consumer.ack_wait_ms} ms</p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Max Ack Pending</span>
              <p class="font-medium">{consumer.max_ack_pending}</p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Max Deliver</span>
              <p class="font-medium">{consumer.max_deliver}</p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Max Waiting</span>
              <p class="font-medium">{consumer.max_waiting}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <h2 class="text-lg font-semibold">HTTP Endpoint</h2>
            <a href="/http-endpoints/{consumer.http_endpoint.id}">
              <Button variant="outline" size="sm">
                <ExternalLink class="h-4 w-4 mr-2" />
                View Endpoint
              </Button>
            </a>
          </div>
          <div class="flex items-center space-x-2">
            <Globe class="h-5 w-5 text-gray-400" />
            <span class="font-medium">{consumer.http_endpoint.url}</span>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <h2 class="text-lg font-semibold">Source table</h2>
            <a href="/databases/{consumer.postgres_database.id}">
              <Button variant="outline" size="sm">
                <ExternalLink class="h-4 w-4 mr-2" />
                View Database
              </Button>
            </a>
          </div>
          <div class="mb-4 flex items-center space-x-2">
            <Database class="h-5 w-5 text-gray-400" />
            <pre class="font-medium">{consumer.postgres_database.name}</pre>
          </div>
          <div class="mb-4 flex items-center space-x-2">
            <icon
              class="hero-table-cells w-6 h-6 rounded {getColorFromName(
                `${consumer.source_table.schema}.${consumer.source_table.name}`
              )}"
            ></icon>

            <pre class="font-medium">{consumer.source_table.schema}.{consumer
                .source_table.name}</pre>
          </div>
          <div class="mb-4">
            <h3 class="text-md font-semibold mb-2">Filters</h3>
            {#if consumer.source_table.column_filters.length > 0}
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Column</TableHead>
                    <TableHead>Operator</TableHead>
                    <TableHead>Value</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {#each consumer.source_table.column_filters as filter}
                    <TableRow>
                      <TableCell><code>{filter.column}</code></TableCell>
                      <TableCell><code>{filter.operator}</code></TableCell>
                      <TableCell><code>{filter.value}</code></TableCell>
                    </TableRow>
                  {/each}
                </TableBody>
              </Table>
            {:else}
              <div
                class="bg-gray-50 border border-gray-200 rounded-lg p-6 text-center"
              >
                <h4 class="text-sm font-medium text-gray-900 mb-1">
                  No filters applied
                </h4>
                <p class="text-sm text-gray-500 mb-4">
                  This consumer will process all data from the source table.
                </p>
              </div>
            {/if}
          </div>
        </CardContent>
      </Card>
    </div>
  </div>
</div>
