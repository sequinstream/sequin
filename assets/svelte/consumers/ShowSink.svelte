<script lang="ts">
  import {
    CheckCircle2,
    ArrowUpRight,
    HelpCircle,
    XCircle,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { formatNumberWithCommas } from "../utils";
  import HealthComponent from "../health/HealthComponent.svelte";
  import ShowSequence from "./ShowSequence.svelte";
  import Cursor from "./Cursor.svelte";
  import type {
    Consumer,
    HttpPushConsumer,
    SqsConsumer,
    RedisConsumer,
    KafkaConsumer,
    SequinStreamConsumer,
  } from "../types/consumer";
  import SinkCardHttpPush from "../components/SinkCardHttpPush.svelte";
  import SinkCardSqs from "../components/SinkCardSqs.svelte";
  import SinkCardRedis from "../components/SinkCardRedis.svelte";
  import SinkCardKafka from "../components/SinkCardKafka.svelte";
  import SinkCardSequinStream from "../components/SinkCardSequinStream.svelte";
  export let live;
  export let parent;
  export let consumer: Consumer;

  export let metrics = {
    messages_processed_count: 0,
    messages_processed_throughput: 0,
    messages_failing_count: 0,
  };
  export let cursor_position = null;
  export let apiBaseUrl: string;
  export let apiTokens: any[];

  function onRewind(
    newCursorPosition: string | null,
    callback: (result: { ok: boolean }) => void,
  ) {
    live.pushEventTo(
      "#" + parent,
      "rewind",
      { new_cursor_position: newCursorPosition },
      (reply) => callback(reply),
    );
  }

  // Add type predicates
  function isHttpPushConsumer(
    consumer: Consumer,
  ): consumer is HttpPushConsumer {
    return consumer.sink.type === "http_push";
  }

  function isSqsConsumer(consumer: Consumer): consumer is SqsConsumer {
    return consumer.sink.type === "sqs";
  }

  function isRedisConsumer(consumer: Consumer): consumer is RedisConsumer {
    return consumer.sink.type === "redis";
  }

  function isKafkaConsumer(consumer: Consumer): consumer is KafkaConsumer {
    return consumer.sink.type === "kafka";
  }

  function isSequinStreamConsumer(
    consumer: Consumer,
  ): consumer is SequinStreamConsumer {
    return consumer.sink.type === "sequin_stream";
  }
</script>

<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <div class="grid gap-6 lg:grid-cols-3 mb-8">
      <HealthComponent
        health={consumer.health}
        paused={consumer.status === "disabled"}
      />
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
          </div>
          <div class="grid lg:grid-cols-3 gap-4">
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
              <span class="text-sm text-gray-500">Batch Size</span>
              <Tooltip.Root openDelay={200}>
                <Tooltip.Trigger>
                  <HelpCircle
                    class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
                  />
                </Tooltip.Trigger>
                <Tooltip.Content class="max-w-xs">
                  <p class="text-xs text-gray-500">
                    <b>Batch Size</b>
                    <br />
                    The number of records included in each webhook request. A larger
                    batch size can improve throughput but increases the payload size
                    of each request.
                  </p>
                </Tooltip.Content>
              </Tooltip.Root>
              <p class="font-medium">
                {consumer.batch_size || 1}
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

      {#if isHttpPushConsumer(consumer)}
        <SinkCardHttpPush {consumer} />
      {:else if isSqsConsumer(consumer)}
        <SinkCardSqs {consumer} />
      {:else if isRedisConsumer(consumer)}
        <SinkCardRedis {consumer} />
      {:else if isKafkaConsumer(consumer)}
        <SinkCardKafka {consumer} />
      {:else if isSequinStreamConsumer(consumer)}
        <SinkCardSequinStream {consumer} {apiBaseUrl} {apiTokens} />
      {/if}

      <ShowSequence {consumer} />
    </div>
  </div>
</div>
