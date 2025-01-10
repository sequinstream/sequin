<script lang="ts">
  import { CheckCircle2, HelpCircle, XCircle, RotateCw } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { formatNumberWithCommas } from "../utils";
  import HealthComponent from "../health/HealthComponent.svelte";
  import ShowSequence from "./ShowSequence.svelte";
  import Backfill from "./Backfill.svelte";
  import type {
    Consumer,
    HttpPushConsumer,
    SqsConsumer,
    RedisConsumer,
    KafkaConsumer,
    SequinStreamConsumer,
    GcpPubsubConsumer,
    NatsConsumer,
    RabbitMqConsumer,
  } from "../types/consumer";
  import SinkCardHttpPush from "../components/SinkCardHttpPush.svelte";
  import SinkCardSqs from "../components/SinkCardSqs.svelte";
  import SinkCardRedis from "../components/SinkCardRedis.svelte";
  import SinkCardKafka from "../components/SinkCardKafka.svelte";
  import SinkCardSequinStream from "../components/SinkCardSequinStream.svelte";
  import SinkCardGcpPubSub from "../components/SinkCardGcpPubSub.svelte";
  import SinkCardNats from "../components/SinkCardNats.svelte";
  import SinkCardRabbitMq from "../components/SinkCardRabbitMq.svelte";
  import * as d3 from "d3";
  import { onMount } from "svelte";
  import * as Alert from "$lib/components/ui/alert";
  import CodeWithCopy from "$lib/components/CodeWithCopy.svelte";

  export let live;
  export let parent;
  export let consumer: Consumer;
  export let showReplicaWarning: boolean;

  function pushEvent(event: string, data: any, cb: (val: any) => void) {
    live.pushEventTo(`#${parent}`, event, data, cb);
  }

  export let metrics = {
    messages_processed_count: 0,
    messages_processed_throughput: 0,
    messages_failing_count: 0,
    messages_processed_throughput_timeseries: [],
  };
  export let cursor_position: {
    is_backfilling: boolean;
    cursor_type: string;
    backfill: {
      id: number;
      state: string;
      rows_initial_count: number;
      rows_processed_count: number;
      rows_ingested_count: number;
    } | null;
    last_completed_at: number;
  } | null;
  export let apiBaseUrl: string;
  export let apiTokens: any[];

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

  function isNatsConsumer(consumer: Consumer): consumer is NatsConsumer {
    return consumer.sink.type === "nats";
  }

  function isGcpPubsubConsumer(
    consumer: Consumer,
  ): consumer is GcpPubsubConsumer {
    return consumer.sink.type === "gcp_pubsub";
  }

  function isSequinStreamConsumer(
    consumer: Consumer,
  ): consumer is SequinStreamConsumer {
    return consumer.sink.type === "sequin_stream";
  }

  function isRabbitMqConsumer(
    consumer: Consumer,
  ): consumer is RabbitMqConsumer {
    return consumer.sink.type === "rabbitmq";
  }

  let chartElement;
  let updateChart;
  let resizeObserver;
  let refreshReplicaWarningLoading = false;
  let dismissToastWarningLoading = false;

  onMount(() => {
    if (metrics.messages_processed_throughput_timeseries.length > 0) {
      updateChart = createThroughputChart(
        chartElement,
        metrics.messages_processed_throughput_timeseries,
        {
          lineColor: "rgb(59, 130, 246)", // blue-500
          lineOpacity: 0.75,
          areaColor: "rgb(59, 130, 246)",
          areaOpacity: 0.05,
        },
      );

      // Create resize observer
      resizeObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          if (entry.target === chartElement) {
            // Recreate the chart with new dimensions
            updateChart = createThroughputChart(
              chartElement,
              metrics.messages_processed_throughput_timeseries,
              {
                lineColor: "rgb(59, 130, 246)",
                lineOpacity: 0.75,
                areaColor: "rgb(59, 130, 246)",
                areaOpacity: 0.05,
              },
            );
          }
        }
      });

      // Start observing the chart element
      resizeObserver.observe(chartElement);
    }

    // Cleanup on component destruction
    return () => {
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
    };
  });

  // Add reactive statement to watch metrics changes
  $: if (
    updateChart &&
    metrics.messages_processed_throughput_timeseries.length > 0
  ) {
    updateChart(metrics.messages_processed_throughput_timeseries);
  }

  function createThroughputChart(element, data, options = {}) {
    const config = {
      width: element.clientWidth,
      height: element.clientHeight,
      margin: { top: 50, right: 0, bottom: 0, left: 0 },
      lineColor: "#3b82f6",
      areaColor: "#3b82f6",
      areaOpacity: 0.1,
      ...options,
    };

    // Clear existing SVG
    d3.select(element).selectAll("svg").remove();

    const svg = d3
      .select(element)
      .append("svg")
      .attr("width", config.width)
      .attr("height", config.height)
      .style("overflow", "visible");

    const x = d3
      .scaleLinear()
      .domain([0, data.length - 1])
      .range([config.margin.left, config.width - config.margin.right]);

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(data) * 1.1])
      .range([config.height - config.margin.bottom, config.margin.top]);

    const line = d3
      .line()
      .x((d, i) => x(i))
      .y((d) => y(d));

    const area = d3
      .area()
      .x((d, i) => x(i))
      .y0(config.height)
      .y1((d) => y(d));

    svg
      .append("path")
      .datum(data)
      .attr("fill", config.areaColor)
      .attr("fill-opacity", config.areaOpacity)
      .attr("d", area);

    svg
      .append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", config.lineColor)
      .attr("stroke-width", 1.5)
      .attr("d", line);

    return function update(newData) {
      y.domain([0, d3.max(newData) * 1.1]);

      svg
        .select("path[fill]")
        .datum(newData)
        .transition()
        .duration(0)
        .attr("d", area);

      svg
        .select("path[stroke]")
        .datum(newData)
        .transition()
        .duration(0)
        .attr("d", line);
    };
  }

  function handleDismissToastWarning() {
    dismissToastWarningLoading = true;
    pushEvent("dismiss_toast_warning", {}, () => {
      dismissToastWarningLoading = false;
    });
  }
</script>

<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <div class="grid gap-6 lg:grid-cols-3 mb-8">
      <HealthComponent health={consumer.health} {pushEvent} />
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
              <span class="font-medium ml-1 text-gray-500">delivered</span>
            </span>
            <span>
              <span
                class="text-2xl font-bold {metrics.messages_failing_count > 0
                  ? 'text-red-500'
                  : 'text-black'}"
              >
                {formatNumberWithCommas(metrics.messages_failing_count)}
              </span>
              <span class="font-medium ml-1 text-gray-500">failing</span>
            </span>
          </div>
        </CardContent>
      </Card>
      <Card>
        <CardContent class="p-6 relative h-32">
          <div bind:this={chartElement} class="absolute inset-0" />
          <div class="relative z-10">
            <div class="flex justify-between items-center mb-4">
              <span class="text-sm font-medium text-gray-500">Throughput</span>
              <div>
                <span class="text-2xl font-bold"
                  >{metrics.messages_processed_throughput ?? "N/A"}</span
                >
                <span class="font-medium ml-1 text-gray-500">msgs/sec</span>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
    {#if consumer.annotations && consumer.annotations.unchanged_toast_replica_identity_dismissed === false}
      <Alert.Root variant="warning" class="mb-8">
        <Alert.Title class="flex items-center justify-between">
          <span>Warning: Large column values delivered as unchanged_toast</span>
          <div class="space-x-2">
            <Button
              variant="outline"
              size="sm"
              on:click={handleDismissToastWarning}
              loading={dismissToastWarningLoading}
            >
              <XCircle class="h-4 w-4 mr-1" />
              Dismiss
            </Button>
          </div>
        </Alert.Title>
        <Alert.Description>
          <p class="mb-2">
            Sequin is delivering some record values as the string
            "unchanged_toast" due to their large size.
          </p>
          <p class="mb-2">
            To receive the full values for these records, alter the table's
            replica identity to "full":
          </p>
          <CodeWithCopy
            maxWidth="750px"
            language="sql"
            code={`alter table "${consumer.sequence.table_schema}"."${consumer.sequence.table_name}" replica identity full;`}
          />
        </Alert.Description>
      </Alert.Root>
    {/if}

    <div class="space-y-6">
      {#if showReplicaWarning}
        <Alert.Root variant="warning">
          <Alert.Title class="flex items-center justify-between">
            <span>Warning: Replica identity not set to full</span>
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
                    },
                  );
                }}
              >
                <RotateCw class="h-4 w-4 mr-1" />
                Refresh
                <span slot="loading">Refreshing...</span>
              </Button>
              <Button
                variant="outline"
                size="sm"
                on:click={() => {
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
              The replica identity for your table is not set to <code>full</code
              >. This means the <code>changes</code> field in message payloads will
              be empty.
            </p>
            <p class="mb-2">
              If you want the <code>changes</code> field to appear in message payloads,
              run the following SQL command:
            </p>
            <CodeWithCopy
              maxWidth="750px"
              language="sql"
              code={`alter table ${consumer.sequence.table_schema}.${consumer.sequence.table_name} replica identity full;`}
            />
          </Alert.Description>
        </Alert.Root>
      {/if}

      {#if isHttpPushConsumer(consumer) || isSequinStreamConsumer(consumer)}
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

              <div hidden={!isHttpPushConsumer(consumer)}>
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
                      batch size can improve throughput but increases the payload
                      size of each request.
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
                      not yet acknowledged). Once this limit is reached, delivery
                      of new messages will be suspended. Your endpoint will only
                      receive new messages once it begins acking outstanding messages.
                      <br />
                      <br />
                      This is a helpful mechanism for flow control and back-pressure.
                      If the target system is having trouble processing messages,
                      it halts the pipeline until messages start clearing again.
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
      {/if}

      <Backfill
        {cursor_position}
        onRun={(newCursorPosition, callback) => {
          live.pushEventTo(
            "#" + parent,
            "run-backfill",
            { new_cursor_position: newCursorPosition },
            (reply) => callback(reply),
          );
        }}
        onCancel={(callback) => {
          live.pushEventTo("#" + parent, "cancel-backfill", {}, (reply) =>
            callback(reply),
          );
        }}
      />

      {#if isHttpPushConsumer(consumer)}
        <SinkCardHttpPush {consumer} />
      {:else if isSqsConsumer(consumer)}
        <SinkCardSqs {consumer} />
      {:else if isRedisConsumer(consumer)}
        <SinkCardRedis {consumer} />
      {:else if isKafkaConsumer(consumer)}
        <SinkCardKafka {consumer} />
      {:else if isGcpPubsubConsumer(consumer)}
        <SinkCardGcpPubSub {consumer} />
      {:else if isSequinStreamConsumer(consumer)}
        <SinkCardSequinStream {consumer} {apiBaseUrl} {apiTokens} />
      {:else if isNatsConsumer(consumer)}
        <SinkCardNats {consumer} />
      {:else if isRabbitMqConsumer(consumer)}
        <SinkCardRabbitMq {consumer} />
      {/if}

      <ShowSequence {consumer} />
    </div>
  </div>
</div>
