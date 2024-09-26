<script lang="ts">
  import {
    CheckCircle2,
    Database,
    ArrowUpRight,
    ArrowLeftFromLine,
    HelpCircle,
    SquareStack,
    RefreshCw,
    XCircle,
    ExternalLink,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Card, CardContent } from "$lib/components/ui/card";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import { getColorFromName, formatNumberWithCommas } from "../utils";
  import * as Dialog from "$lib/components/ui/dialog";
  import HealthComponent from "../health/HealthComponent.svelte";
  import CodeWithSecret from "../components/CodeWithSecret.svelte";
  import { Badge } from "$lib/components/ui/badge";
  import * as Alert from "$lib/components/ui/alert";
  import CodeWithCopy from "$lib/components/CodeWithCopy.svelte";

  export let consumer;
  export let metrics;
  export let live;
  export let parent;
  export let apiBaseUrl;
  export let api_token;
  export let replica_identity;

  // Remove functions and variables related to header
  let refreshReplicaWarningLoading = false;

  function handleRefreshReplicaWarning() {
    refreshReplicaWarningLoading = true;
    live.pushEventTo("#" + parent, "refresh_replica_warning", {}, () => {
      refreshReplicaWarningLoading = false;
    });
  }

  function handleDismissReplicaWarning() {
    consumer.replica_warning_dismissed = true;
    live.pushEventTo("#" + parent, "dismiss_replica_warning", {});
  }
</script>

<!-- Wrap the content in a flex column that fills available space -->
<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <!-- Main content without the header -->
    <div class="grid gap-6 md:grid-cols-3 mb-8">
      <HealthComponent health={consumer.health} />
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500"
              >Messages Processed</span
            >
            <CheckCircle2 class="h-5 w-5 text-green-500" />
          </div>
          <span class="text-2xl font-bold">
            {metrics.messages_processed_count
              ? metrics.messages_processed_count.toLocaleString()
              : "N/A"}
          </span>
        </CardContent>
      </Card>
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Throughput</span>
            <ArrowUpRight class="h-5 w-5 text-blue-500" />
          </div>
          <span class="text-2xl font-bold"
            >{metrics.messages_processed_throughput ?? "N/A"}/min</span
          >
        </CardContent>
      </Card>
    </div>

    <div class="space-y-6">
      <Card>
        <CardContent class="p-6">
          <div class="flex items-center space-x-4 mb-4">
            <h2 class="text-lg font-semibold">Configuration</h2>
            <Badge variant="default">
              <ArrowLeftFromLine class="h-4 w-4 mr-1" />
              Pull consumer
            </Badge>
            {#if consumer.message_kind === "event"}
              <Badge variant="default">
                <SquareStack class="h-4 w-4 mr-1" />
                Changes
              </Badge>
            {:else if consumer.message_kind === "record"}
              <Badge variant="default">
                <SquareStack class="h-4 w-4 mr-1" />
                Rows
              </Badge>
            {/if}
          </div>
          <div class="grid md:grid-cols-2 gap-4">
            <div>
              <span class="text-sm text-gray-500">Visibility Timeout</span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b class="text-sm">Visibility Timeout</b>
                    <br />
                    Defines a time limit in which a message must be successfully
                    acknowledged by the worker. After this time limit elapses, if
                    the message has not yet been acknowledged, it will be available
                    for delivery again to other workers.
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <p class="font-medium">
                {formatNumberWithCommas(consumer.ack_wait_ms)} ms
              </p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Max Ack Pending</span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b class="text-sm">Max Ack Pending</b>
                    <br />
                    Defines the maximum number of messages, without acknowledgment,
                    that can be outstanding. Once this limit is reached, message
                    delivery will be suspended. This limit applies across all of
                    the consumer's bound subscriptions. A value of -1 means there
                    can be any number of pending acknowledgments (i.e., no flow control).
                    The default is 1000.
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <p class="font-medium">
                {formatNumberWithCommas(consumer.max_ack_pending)} messages
              </p>
            </div>
            <!-- <div>
              <span class="text-sm text-gray-500">Max Waiting</span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b>Max waiting</b> The maximum number of messages that can be
                    waiting to be processed before the consumer stops processing
                    new messages.
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <p class="font-medium">
                {formatNumberWithCommas(consumer.max_waiting)}
              </p>
            </div> -->
          </div>
        </CardContent>
      </Card>

      {#if consumer.message_kind === "event" && replica_identity !== "full" && replica_identity !== "loading" && !consumer.replica_warning_dismissed}
        <Alert.Root variant="warning">
          <Alert.Title class="flex items-center justify-between">
            <span>Warning: Replica Identity Not Set to Full</span>
            <div class="space-x-2">
              <Button
                variant="outline"
                loading={refreshReplicaWarningLoading}
                size="sm"
                on:click={handleRefreshReplicaWarning}
              >
                <RefreshCw class="h-4 w-4 mr-1" />
                Refresh
                <span slot="loading">Refreshing...</span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                on:click={handleDismissReplicaWarning}
              >
                <XCircle class="h-4 w-4 mr-1" />
                Dismiss
              </Button>
            </div>
          </Alert.Title>
          <Alert.Description>
            <p class="mb-2">
              The replica identity for your table is not set to 'full'. This
              means the <code>changes</code> field in message payloads will be empty.
            </p>
            <p class="mb-2">
              If you want the <code>changes</code> field to appear in message payloads,
              run the following SQL command:
            </p>
            <CodeWithCopy
              maxWidth="750px"
              language="sql"
              code={`alter table "${consumer.source_table.schema}"."${consumer.source_table.name}" replica identity full;`}
            />
          </Alert.Description>
        </Alert.Root>
      {/if}

      <Card>
        <CardContent class="p-6">
          <h2 class="text-lg font-semibold mb-4">
            Receive and Acknowledge Messages
          </h2>
          <div class="space-y-4">
            <div>
              <h3 class="text-md font-semibold mb-2">Receive Messages</h3>
              <CodeWithSecret
                tabs={[
                  {
                    name: "cURL",
                    value: `curl -X GET "${apiBaseUrl}/api/http_pull_consumers/${consumer.name}/receive" \\
       -H "Authorization: Bearer {{secret}}"`,
                  },
                ]}
                secret={api_token.token}
              />
            </div>
            <div>
              <h3 class="text-md font-semibold mb-2">Acknowledge Messages</h3>
              <CodeWithSecret
                tabs={[
                  {
                    name: "cURL",
                    value: `curl -X POST "${apiBaseUrl}/api/http_pull_consumers/${consumer.name}/ack" \\
       -H "Authorization: Bearer {{secret}}" \\
       -H "Content-Type: application/json" \\
       -d '{"ack_ids": ["<ack_id>"]}'`,
                  },
                ]}
                secret={api_token.token}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <h2 class="text-lg font-semibold">Source table</h2>
            <a href="/databases/{consumer.postgres_database.id}">
              <Button variant="outline" size="sm">
                View Database
                <ArrowUpRight class="h-4 w-4 ml-2" />
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

<!-- Remove the delete confirmation dialog if it's now handled elsewhere -->
