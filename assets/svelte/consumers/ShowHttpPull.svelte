<script lang="ts">
  import {
    CheckCircle2,
    ArrowUpRight,
    ArrowLeftFromLine,
    HelpCircle,
    SquareStack,
  } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { formatNumberWithCommas } from "../utils";
  import HealthComponent from "../health/HealthComponent.svelte";
  import CodeWithSecret from "../components/CodeWithSecret.svelte";
  import { Badge } from "$lib/components/ui/badge";
  import ShowSequence from "./ShowSequence.svelte";
  import Cursor from "./Cursor.svelte";

  export let live;
  export let parent;
  export let consumer;
  export let metrics;
  export let apiBaseUrl;
  export let api_token;
  export let cursor_position;

  let isRewinding = false;

  function onRewind(newCursorPosition: string): { ok: boolean } {
    isRewinding = true;
    try {
      live.pushEventTo(
        "#" + parent,
        "rewind",
        {
          new_cursor_position: newCursorPosition,
        },
        (reply) => reply,
      );
      isRewinding = false;
      return { ok: true };
    } catch (error) {
      console.error("Rewind operation failed:", error);
      isRewinding = false;
      return { ok: false };
    }
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
              : 0}
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
          </div>
        </CardContent>
      </Card>

      <Cursor
        {cursor_position}
        messages_processed_count={metrics.messages_processed_count
          ? metrics.messages_processed_count.toLocaleString()
          : 0}
        {onRewind}
      />

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

      <ShowSequence {consumer} />
    </div>
  </div>
</div>

<!-- Remove the delete confirmation dialog if it's now handled elsewhere -->
