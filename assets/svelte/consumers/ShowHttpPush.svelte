<script lang="ts">
  import {
    CheckCircle2,
    Database,
    Webhook,
    ExternalLink,
    ArrowUpRight,
    ArrowRight,
    ArrowRightToLine,
    HelpCircle,
    SquareStack,
    RefreshCw,
    XCircle,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import { getColorFromName, formatNumberWithCommas } from "../utils";
  import HealthComponent from "../health/HealthComponent.svelte";
  import { Badge } from "$lib/components/ui/badge";
  import { concatenateUrl } from "../databases/utils";
  import * as Alert from "$lib/components/ui/alert";
  import CodeWithCopy from "$lib/components/CodeWithCopy.svelte";

  export let consumer;
  export let live;
  export let parent;
  export let metrics = {
    messages_processed_count: 0,
    messages_processed_throughput: 0,
    messages_failing_count: 0,
  };
  export let replica_identity;

  let refreshReplicaWarningLoading = false;

  function isWebhookSiteUrl(url: string): boolean {
    return url.startsWith("https://webhook.site/");
  }

  function getWebhookSiteViewUrl(url: string): string {
    const uuid = url.split("/").pop();
    return `https://webhook.site/#!/view/${uuid}`;
  }

  $: fullEndpointUrl = concatenateUrl(
    consumer.http_endpoint.url,
    consumer.http_endpoint_path
  );
</script>

<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <div class="grid gap-6 md:grid-cols-3 mb-8">
      <HealthComponent health={consumer.health} />
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Messages</span>
            {#if metrics.messages_failing_count > 0}
              <XCircle class="h-5 w-5 text-red-500" />
            {:else}
              <CheckCircle2 class="h-5 w-5 text-green-500" />
            {/if}
          </div>
          <div class="flex justify-between items-center space-x-4">
            <span>
              <span class="text-2xl font-bold">
                {metrics.messages_processed_count
                  ? formatNumberWithCommas(metrics.messages_processed_count)
                  : "0"}
              </span>
              <span class="font-medium ml-1">acked</span>
            </span>
            <span>
              <span
                class="text-2xl font-bold {metrics.messages_failing_count > 0
                  ? 'text-red-500'
                  : 'text-black'}"
              >
                {formatNumberWithCommas(metrics.messages_failing_count)}
              </span>
              <span class="font-medium ml-1">failing</span>
            </span>
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Throughput</span>
            <ArrowUpRight class="h-5 w-5 text-blue-500" />
          </div>
          <span class="text-2xl font-bold"
            >{metrics.messages_processed_throughput ?? "N/A"}</span
          >
          <span class="font-medium ml-1">/min</span>
        </CardContent>
      </Card>
    </div>

    <div class="space-y-6">
      <Card>
        <CardContent class="p-6">
          <div class="flex items-center space-x-4 mb-4">
            <h2 class="text-lg font-semibold">Configuration</h2>
            <Badge variant="default">
              <ArrowRightToLine class="h-4 w-4 mr-1" />
              Push consumer
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
          <div class="grid lg:grid-cols-2 xl:grid-cols-3 gap-4">
            <div>
              <span class="text-sm text-gray-500"> Request Timeout </span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b>Request Timeout</b>
                    <br />
                    Defines the time limit for a message to be successfully acknowledged
                    (i.e. return a status 200) by the webhook endpoint. Exceeding
                    this time period triggers a retry.
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <p class="font-medium">
                {formatNumberWithCommas(consumer.ack_wait_ms)} ms
              </p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Max Pending Messages</span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b>Max Pending Messages</b>
                    <br />
                    The maximum number of messages that can be outstanding (delivered,
                    not yet acknowledged). Once this limit is reached, delivery of
                    new messages will be suspended. Your endpoint will only receive
                    new messages once it begins acking outstanding messages.
                    <br />
                    <br />
                    This is a helpful mechanism for flow control and back-pressure.
                    If the target system is having trouble processing messages, it
                    halts the pipeline until messages start clearing again.
                    <br />
                    <br />
                    A ceiling of 10,000 is very reasonable for most applications.
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <p class="font-medium">
                {formatNumberWithCommas(consumer.max_ack_pending)}
              </p>
            </div>
            <div>
              <span class="text-sm text-gray-500">HTTP Endpoint Path</span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b>HTTP Endpoint Path</b>
                    <br />
                    The path to use when pushing messages to the HTTP endpoint.
                    <br />
                    <br />
                    This path is relative to the base URL of the HTTP endpoint.
                    <br />
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <div class="mt-2">
                {#if consumer.http_endpoint_path === ""}
                  <span
                    class="bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md"
                    >No path configured</span
                  >
                {:else}
                  <span
                    class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md"
                    >{consumer.http_endpoint_path}</span
                  >
                {/if}
              </div>
            </div>
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
                on:click={() => {
                  refreshReplicaWarningLoading = true;
                  live.pushEventTo(
                    "#" + parent,
                    "refresh_replica_warning",
                    {},
                    () => {
                      refreshReplicaWarningLoading = false;
                    }
                  );
                }}
              >
                <RefreshCw class="h-4 w-4 mr-1" />
                Refresh
                <span slot="loading">Refreshing...</span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                on:click={() => {
                  consumer.replica_warning_dismissed = true;
                  live.pushEventTo("#" + parent, "dismiss_replica_warning", {});
                }}
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
          <div class="flex justify-between items-center mb-4">
            <h2 class="text-lg font-semibold">HTTP Endpoint</h2>
            <div class="flex space-x-2">
              <a
                href="/http-endpoints/{consumer.http_endpoint.id}"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <Button variant="outline" size="sm">
                  <ExternalLink class="h-4 w-4 mr-2" />
                  View Endpoint
                </Button>
              </a>
              {#if isWebhookSiteUrl(consumer.http_endpoint.url)}
                <a
                  href={getWebhookSiteViewUrl(consumer.http_endpoint.url)}
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  <Button variant="outline" size="sm">
                    <ExternalLink class="h-4 w-4 mr-2" />
                    View on Webhook.site
                  </Button>
                </a>
              {/if}
            </div>
          </div>
          <div class="flex items-center space-x-2 overflow-x-auto">
            <Webhook class="h-5 w-5 text-gray-400" />
            <span
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
              >{fullEndpointUrl}</span
            >
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <h2 class="text-lg font-semibold">Source table</h2>
            <a
              href="/databases/{consumer.postgres_database.id}"
              data-phx-link="redirect"
              data-phx-link-state="push"
            >
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
