<script lang="ts">
  import { CheckCircle2, HelpCircle, XCircle, RotateCw } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { formatNumberWithCommas } from "../utils";
  import HealthSummary from "../health/HealthSummary.svelte";
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
  import SqsSinkCard from "../sinks/sqs/SqsSinkCard.svelte";
  import RedisSinkCard from "../sinks/redis/RedisSinkCard.svelte";
  import KafkaSinkCard from "../sinks/kafka/KafkaSinkCard.svelte";
  import SequinStreamSinkCard from "../sinks/sequin_stream/SequinStreamSinkCard.svelte";
  import GcpPubsubSinkCard from "../sinks/gcp_pubsub/GcpPubsubSinkCard.svelte";
  import NatsSinkCard from "../sinks/nats/NatsSinkCard.svelte";
  import RabbitMqSinkCard from "../sinks/rabbitmq/RabbitMqSinkCard.svelte";
  import * as d3 from "d3";
  import { onMount } from "svelte";
  import HealthAlerts from "$lib/health/HealthAlerts.svelte";

  export let live;
  export let parent;
  export let consumer: Consumer;

  function pushEvent(event: string, data: any, cb: (val: any) => void) {
    live.pushEventTo(`#${parent}`, event, data, cb);
  }

  export let metrics = {
    messages_failing_count: 0,
    messages_pending_count: 0,
    messages_processed_count: 0,
    messages_processed_throughput: 0,
    messages_processed_throughput_timeseries: [],
    messages_processed_bytes: 0,
    messages_processed_bytes_timeseries: [],
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
  let bytesChartElement;
  let updateBytesChart;
  let resizeBytesObserver;

  let hovered_messages_processed_throughput: number | null = null;
  let hovered_messages_processed_bytes: number | null = null;

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

    if (metrics.messages_processed_bytes_timeseries.length > 0) {
      updateBytesChart = createBytesChart(
        bytesChartElement,
        metrics.messages_processed_bytes_timeseries,
        {
          lineColor: "rgb(59, 130, 246)", // blue-500
          lineOpacity: 0.75,
          areaColor: "rgb(59, 130, 246)",
          areaOpacity: 0.05,
        },
      );

      // Create resize observer
      resizeBytesObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          if (entry.target === bytesChartElement) {
            // Recreate the chart with new dimensions
            updateBytesChart = createBytesChart(
              bytesChartElement,
              metrics.messages_processed_bytes_timeseries,
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
      resizeBytesObserver.observe(bytesChartElement);
    }

    // Cleanup on component destruction
    return () => {
      if (resizeObserver) {
        resizeObserver.disconnect();
      }
      if (resizeBytesObserver) {
        resizeBytesObserver.disconnect();
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

  $: if (
    updateBytesChart &&
    metrics.messages_processed_bytes_timeseries.length > 0
  ) {
    updateBytesChart(metrics.messages_processed_bytes_timeseries);
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

    const chartWidth = config.width - config.margin.left - config.margin.right;
    const chartHeight =
      config.height - config.margin.top - config.margin.bottom;

    const x = d3
      .scaleLinear()
      .domain([0, data.length - 1])
      .range([0, chartWidth]);

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(data) * 1.1])
      .range([chartHeight, 0]);

    // Create a chart group and translate it to account for margins
    const chartGroup = svg
      .append("g")
      .attr(
        "transform",
        `translate(${config.margin.left},${config.margin.top})`,
      );

    // Add hover elements
    const focus = chartGroup.append("g").style("display", "none");

    focus.append("circle").attr("r", 4).attr("fill", config.lineColor);

    // Add mouse tracking area for the entire card space
    const mouseArea = svg
      .append("rect")
      .attr("width", config.width)
      .attr("height", config.height)
      .style("fill", "none")
      .style("pointer-events", "all");

    // Rest of the chart elements
    const line = d3
      .line()
      .x((d, i) => x(i))
      .y((d) => y(d));

    const area = d3
      .area()
      .x((d, i) => x(i))
      .y0(chartHeight)
      .y1((d) => y(d));

    chartGroup
      .append("path")
      .datum(data)
      .attr("fill", config.areaColor)
      .attr("fill-opacity", config.areaOpacity)
      .attr("d", area);

    chartGroup
      .append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", config.lineColor)
      .attr("stroke-width", 1.5)
      .attr("d", line);

    let lastMouseX = null;
    let currentData = data;

    const handleMouseMove = function (event) {
      const mouseX = event
        ? d3.pointer(event, mouseArea.node())[0]
        : lastMouseX;
      if (mouseX === null) return;

      lastMouseX = mouseX;
      const xValue = Math.round(x.invert(mouseX));

      if (xValue >= 0 && xValue < currentData.length) {
        const yValue = currentData[xValue];
        focus
          .style("display", null)
          .attr("transform", `translate(${x(xValue)},${y(yValue)})`);

        hovered_messages_processed_throughput = yValue;
      }
    };

    // Add default state dot (visible when not hovering)
    const defaultDot = chartGroup
      .append("circle")
      .attr("r", 4)
      .attr("fill", config.lineColor)
      .attr(
        "transform",
        `translate(${x(data.length - 1)},${y(data[data.length - 1])})`,
      );

    // Modify mouseArea event handlers
    mouseArea
      .on("mouseenter", () => {
        focus.style("display", null);
        defaultDot.style("display", "none");
      })
      .on("mouseleave", () => {
        focus.style("display", "none");
        defaultDot.style("display", null);
        lastMouseX = null;
        hovered_messages_processed_throughput = null;
      })
      .on("mousemove", handleMouseMove);

    return function update(newData) {
      currentData = newData;
      y.domain([0, d3.max(newData) * 1.1]);

      // Update default dot position
      defaultDot.attr(
        "transform",
        `translate(${x(newData.length - 1)},${y(newData[newData.length - 1])})`,
      );

      chartGroup
        .select("path[fill]")
        .datum(newData)
        .transition()
        .duration(0)
        .attr("d", area);

      chartGroup
        .select("path[stroke]")
        .datum(newData)
        .transition()
        .duration(0)
        .attr("d", line);

      if (lastMouseX !== null) {
        handleMouseMove(null);
      }
    };
  }

  function createBytesChart(element, data, options = {}) {
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

    const chartWidth = config.width - config.margin.left - config.margin.right;
    const chartHeight =
      config.height - config.margin.top - config.margin.bottom;

    const x = d3
      .scaleLinear()
      .domain([0, data.length - 1])
      .range([0, chartWidth]);

    const y = d3
      .scaleLinear()
      .domain([0, d3.max(data) * 1.1])
      .range([chartHeight, 0]);

    // Create a chart group and translate it to account for margins
    const chartGroup = svg
      .append("g")
      .attr(
        "transform",
        `translate(${config.margin.left},${config.margin.top})`,
      );

    // Add hover elements
    const focus = chartGroup.append("g").style("display", "none");

    focus.append("circle").attr("r", 4).attr("fill", config.lineColor);

    // Add mouse tracking area for the entire card space
    const mouseArea = svg
      .append("rect")
      .attr("width", config.width)
      .attr("height", config.height)
      .style("fill", "none")
      .style("pointer-events", "all");

    // Rest of the chart elements
    const line = d3
      .line()
      .x((d, i) => x(i))
      .y((d) => y(d));

    const area = d3
      .area()
      .x((d, i) => x(i))
      .y0(chartHeight)
      .y1((d) => y(d));

    chartGroup
      .append("path")
      .datum(data)
      .attr("fill", config.areaColor)
      .attr("fill-opacity", config.areaOpacity)
      .attr("d", area);

    chartGroup
      .append("path")
      .datum(data)
      .attr("fill", "none")
      .attr("stroke", config.lineColor)
      .attr("stroke-width", 1.5)
      .attr("d", line);

    let lastMouseX = null;
    let currentData = data;

    const handleMouseMove = function (event) {
      const mouseX = event
        ? d3.pointer(event, mouseArea.node())[0]
        : lastMouseX;
      if (mouseX === null) return;

      lastMouseX = mouseX;
      const xValue = Math.round(x.invert(mouseX));

      if (xValue >= 0 && xValue < currentData.length) {
        const yValue = currentData[xValue];
        focus
          .style("display", null)
          .attr("transform", `translate(${x(xValue)},${y(yValue)})`);

        hovered_messages_processed_bytes = yValue;
      }
    };

    // Add default state dot (visible when not hovering)
    const defaultDot = chartGroup
      .append("circle")
      .attr("r", 4)
      .attr("fill", config.lineColor)
      .attr(
        "transform",
        `translate(${x(data.length - 1)},${y(data[data.length - 1])})`,
      );

    // Modify mouseArea event handlers
    mouseArea
      .on("mouseenter", () => {
        focus.style("display", null);
        defaultDot.style("display", "none");
      })
      .on("mouseleave", () => {
        focus.style("display", "none");
        defaultDot.style("display", null);
        lastMouseX = null;
        hovered_messages_processed_bytes = null;
      })
      .on("mousemove", handleMouseMove);

    return function update(newData) {
      currentData = newData;
      y.domain([0, d3.max(newData) * 1.1]);

      // Update default dot position
      defaultDot.attr(
        "transform",
        `translate(${x(newData.length - 1)},${y(newData[newData.length - 1])})`,
      );

      chartGroup
        .select("path[fill]")
        .datum(newData)
        .transition()
        .duration(0)
        .attr("d", area);

      chartGroup
        .select("path[stroke]")
        .datum(newData)
        .transition()
        .duration(0)
        .attr("d", line);

      if (lastMouseX !== null) {
        handleMouseMove(null);
      }
    };
  }

  function formatBytes(bytes: number): { value: string; unit: string } {
    const units = ["bytes", "KB", "MB", "GB", "TB"];
    let value = bytes;
    let unitIndex = 0;

    while (value >= 1024 && unitIndex < units.length - 1) {
      value /= 1024;
      unitIndex++;
    }

    // Round to 1 decimal place if we've converted to a larger unit
    const formattedValue =
      unitIndex === 0 ? Math.round(value).toString() : value.toFixed(1);

    return {
      value: formattedValue,
      unit: units[unitIndex],
    };
  }
</script>

<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <div class="grid gap-4 lg:grid-cols-2 mb-8">
      <HealthSummary
        health={consumer.health}
        {pushEvent}
        status={consumer.status}
      />
      <Card class="h-32">
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
              <div class="text-md font-bold">
                {metrics.messages_pending_count
                  ? formatNumberWithCommas(metrics.messages_pending_count)
                  : "0"}
              </div>
              <div class="text-xs text-gray-500">pending</div>
            </span>
            <span>
              <div class="text-md font-bold">
                {metrics.messages_processed_count
                  ? formatNumberWithCommas(metrics.messages_processed_count)
                  : "0"}
              </div>
              <div class="text-xs text-gray-500">processed</div>
            </span>
            <span>
              <div
                class="text-md font-bold {metrics.messages_failing_count > 0
                  ? 'text-red-500'
                  : 'text-black'}"
              >
                {formatNumberWithCommas(metrics.messages_failing_count)}
              </div>
              <div class="text-xs text-gray-500">failing</div>
            </span>
          </div>
        </CardContent>
      </Card>
      <Card class="h-32">
        <CardContent class="p-6 relative h-full">
          <div bind:this={chartElement} class="absolute inset-0" />
          <div class="relative z-10 pointer-events-none">
            <div class="flex justify-between items-center mb-4">
              <span class="text-sm font-medium mb-auto text-gray-500"
                >Message Throughput</span
              >
              <div>
                <span class="text-2xl font-bold"
                  >{(hovered_messages_processed_throughput ??
                    metrics.messages_processed_throughput) > 0
                    ? (
                        hovered_messages_processed_throughput ??
                        metrics.messages_processed_throughput
                      ).toFixed(1)
                    : "0"}</span
                >
                <span class="text-xs font-medium ml-1 text-gray-500"
                  >msgs/sec</span
                >
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
      <Card class="h-32">
        <CardContent class="p-6 relative h-full">
          <div bind:this={bytesChartElement} class="absolute inset-0" />
          <div class="relative z-10 pointer-events-none">
            <div class="flex justify-between items-center mb-4">
              <span class="text-sm font-medium mb-auto text-gray-500"
                >Bytes Throughput</span
              >
              <div>
                {#if (hovered_messages_processed_bytes ?? metrics.messages_processed_bytes) !== null}
                  {@const formatted = formatBytes(
                    hovered_messages_processed_bytes ??
                      metrics.messages_processed_bytes,
                  )}
                  <span class="text-2xl font-bold">{formatted.value}</span>
                  <span class="text-xs font-medium ml-1 text-gray-500"
                    >{formatted.unit}/sec</span
                  >
                {:else}
                  <span class="text-2xl font-bold">0</span>
                  <span class="text-xs font-medium ml-1 text-gray-500"
                    >bytes/sec</span
                  >
                {/if}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>

    <HealthAlerts checks={consumer.health.checks} {pushEvent} />

    <div class="space-y-6">
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
        <SqsSinkCard {consumer} />
      {:else if isRedisConsumer(consumer)}
        <RedisSinkCard {consumer} />
      {:else if isKafkaConsumer(consumer)}
        <KafkaSinkCard {consumer} />
      {:else if isGcpPubsubConsumer(consumer)}
        <GcpPubsubSinkCard {consumer} />
      {:else if isSequinStreamConsumer(consumer)}
        <SequinStreamSinkCard {consumer} {apiBaseUrl} {apiTokens} />
      {:else if isNatsConsumer(consumer)}
        <NatsSinkCard {consumer} />
      {:else if isRabbitMqConsumer(consumer)}
        <RabbitMqSinkCard {consumer} />
      {/if}

      <ShowSequence {consumer} />
    </div>
  </div>
</div>
