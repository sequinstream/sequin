<script lang="ts">
  import {
    CheckCircle2,
    Webhook,
    ExternalLink,
    ArrowUpRight,
    ArrowRightToLine,
    HelpCircle,
    SquareStack,
    XCircle,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { formatNumberWithCommas } from "../utils";
  import HealthComponent from "../health/HealthComponent.svelte";
  import { Badge } from "$lib/components/ui/badge";
  import { concatenateUrl } from "../databases/utils";
  import ShowSequence from "./ShowSequence.svelte";
  import Cursor from "./Cursor.svelte";
  export let consumer;
  export let metrics = {
    messages_processed_count: 0,
    messages_processed_throughput: 0,
    messages_failing_count: 0,
  };
  export let cursor_position = null;

  function isWebhookSiteUrl(url: string): boolean {
    return url.startsWith("https://webhook.site/");
  }

  function getWebhookSiteViewUrl(url: string): string {
    const uuid = url.split("/").pop();
    return `https://webhook.site/#!/view/${uuid}`;
  }

  $: fullEndpointUrl = concatenateUrl(
    consumer.http_endpoint.url,
    consumer.http_endpoint_path,
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

      <Cursor
        {cursor_position}
        messages_processed_count={metrics.messages_processed_count
          ? metrics.messages_processed_count.toLocaleString()
          : 0}
      />

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

      <ShowSequence {consumer} />
    </div>
  </div>
</div>
