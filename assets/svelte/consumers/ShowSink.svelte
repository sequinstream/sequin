<script lang="ts">
  import {
    CheckCircle2,
    HelpCircle,
    XCircle,
    Loader2,
    ArrowDownSquare,
    ExternalLink,
  } from "lucide-svelte";
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
  } from "./types";
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
  import BackfillForm from "../components/BackfillForm.svelte";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Button } from "$lib/components/ui/button";
  import Beta from "../components/Beta.svelte";

  export let live;
  export let parent;
  export let consumer: Consumer;
  export let transform: {
    id: string;
    name: string;
    description: string;
    transform: {
      type: string;
      path: string;
      code: string;
      sink_type: string;
    };
  } | null;

  function pushEvent(event: string, data: any, cb: (val: any) => void) {
    live.pushEventTo(`#${parent}`, event, data, cb);
  }

  export let metrics_loading: boolean;
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

  // Track SVG elements for proper cleanup
  let chartSvg;
  let bytesSvg;

  let hovered_messages_processed_throughput: number | null = null;
  let hovered_messages_processed_bytes: number | null = null;
  let hoveredXValue: number | null = null;

  let showBackfillDialog = false;
  let backfillForm = {
    startPosition: "beginning" as "beginning" | "specific",
    sortColumnAttnum: null,
    initialSortColumnValue: null,
  };
  let backfillFormErrors: Record<string, string> = {};
  let isSubmittingBackfill = false;

  onMount(() => {
    // Initial chart creation
    if (metrics.messages_processed_throughput_timeseries.length > 0) {
      const chartResult = createThroughputChart(
        chartElement,
        metrics.messages_processed_throughput_timeseries,
        {
          lineColor: "rgb(59, 130, 246)", // blue-500
          lineOpacity: 0.75,
          areaColor: "rgb(59, 130, 246)",
          areaOpacity: 0.05,
        },
      );

      updateChart = chartResult.update;
      chartSvg = chartResult.svg;

      // Create resize observer - only update dimensions, don't recreate chart
      resizeObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          if (entry.target === chartElement && chartSvg) {
            // Update dimensions only
            const width = entry.contentRect.width;
            const height = entry.contentRect.height;

            chartSvg.attr("width", width).attr("height", height);

            // Update chart with new dimensions
            updateChart(metrics.messages_processed_throughput_timeseries, {
              width,
              height,
            });
          }
        }
      });

      // Start observing the chart element
      resizeObserver.observe(chartElement);
    }

    if (metrics.messages_processed_bytes_timeseries.length > 0) {
      const bytesChartResult = createBytesChart(
        bytesChartElement,
        metrics.messages_processed_bytes_timeseries,
        {
          lineColor: "rgb(59, 130, 246)", // blue-500
          lineOpacity: 0.75,
          areaColor: "rgb(59, 130, 246)",
          areaOpacity: 0.05,
        },
      );

      updateBytesChart = bytesChartResult.update;
      bytesSvg = bytesChartResult.svg;

      // Create resize observer - only update dimensions, don't recreate chart
      resizeBytesObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          if (entry.target === bytesChartElement && bytesSvg) {
            // Update dimensions only
            const width = entry.contentRect.width;
            const height = entry.contentRect.height;

            bytesSvg.attr("width", width).attr("height", height);

            // Update chart with new dimensions
            updateBytesChart(metrics.messages_processed_bytes_timeseries, {
              width,
              height,
            });
          }
        }
      });

      // Start observing the chart element
      resizeBytesObserver.observe(bytesChartElement);
    }

    // Cleanup on component destruction
    return () => {
      // Disconnect observers
      if (resizeObserver) {
        resizeObserver.disconnect();
        resizeObserver = null;
      }
      if (resizeBytesObserver) {
        resizeBytesObserver.disconnect();
        resizeBytesObserver = null;
      }

      // Clean up D3 elements and event listeners
      if (chartSvg) {
        chartSvg.selectAll("*").remove();
        chartSvg.remove();
        chartSvg = null;
      }

      if (bytesSvg) {
        bytesSvg.selectAll("*").remove();
        bytesSvg.remove();
        bytesSvg = null;
      }

      // Clear update functions
      updateChart = null;
      updateBytesChart = null;
    };
  });

  // Fix the reactive statement for metrics updates
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

  // Fix the reactive statement for hover sync
  $: if (hoveredXValue === null) {
    hovered_messages_processed_throughput = null;
    hovered_messages_processed_bytes = null;
    if (updateChart?.updateHover) updateChart.updateHover(null);
    if (updateBytesChart?.updateHover) updateBytesChart.updateHover(null);
  } else {
    hovered_messages_processed_throughput =
      metrics.messages_processed_throughput_timeseries[hoveredXValue];
    hovered_messages_processed_bytes =
      metrics.messages_processed_bytes_timeseries[hoveredXValue];
    if (updateChart?.updateHover) updateChart.updateHover(hoveredXValue);
    if (updateBytesChart?.updateHover)
      updateBytesChart.updateHover(hoveredXValue);
  }

  function createThroughputChart(element, data, options = {}) {
    const MAX_DATA_POINTS = 300; // Keep 5 minutes of data at 1 second intervals

    // Trim data to max length if needed
    if (data.length > MAX_DATA_POINTS) {
      data = data.slice(-MAX_DATA_POINTS);
    }

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
      .domain([0, d3.max(data) * 1.1 || 1])
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

    const areaPath = chartGroup
      .append("path")
      .datum(data)
      .attr("fill", config.areaColor)
      .attr("fill-opacity", config.areaOpacity)
      .attr("d", area);

    const linePath = chartGroup
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
        focus
          .style("display", null)
          .attr(
            "transform",
            `translate(${x(xValue)},${y(currentData[xValue])})`,
          );

        hoveredXValue = xValue;
      }
    };

    // Add default state dot (visible when not hovering)
    const defaultDot = chartGroup
      .append("circle")
      .attr("r", 4)
      .attr("fill", config.lineColor)
      .attr(
        "transform",
        `translate(${x(data.length - 1)},${y(data[data.length - 1] || 0)})`,
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
        hoveredXValue = null;
      })
      .on("mousemove", handleMouseMove);

    // Add function to update hover state externally
    const updateHover = (xValue: number | null) => {
      if (xValue === null) {
        focus.style("display", "none");
        defaultDot.style("display", null);
        hovered_messages_processed_throughput = null;
      } else if (xValue >= 0 && xValue < currentData.length) {
        const yValue = currentData[xValue];
        focus
          .style("display", null)
          .attr("transform", `translate(${x(xValue)},${y(yValue)})`);
        defaultDot.style("display", "none");
        hovered_messages_processed_throughput = yValue;
      }
    };

    // Improved update function that can handle dimension changes
    const update = function (newData, newDimensions = null) {
      // Trim incoming data if needed
      if (newData.length > MAX_DATA_POINTS) {
        newData = newData.slice(-MAX_DATA_POINTS);
      }
      currentData = newData;

      // Update scales if dimensions changed
      if (newDimensions) {
        const newWidth = newDimensions.width;
        const newHeight = newDimensions.height;

        const newChartWidth =
          newWidth - config.margin.left - config.margin.right;
        const newChartHeight =
          newHeight - config.margin.top - config.margin.bottom;

        x.range([0, newChartWidth]);
        y.range([newChartHeight, 0]);

        // Update mouse area dimensions
        mouseArea.attr("width", newWidth).attr("height", newHeight);

        // Update chart group position
        chartGroup.attr(
          "transform",
          `translate(${config.margin.left},${config.margin.top})`,
        );
      }

      // Update y domain based on new data
      y.domain([0, d3.max(newData) * 1.1 || 1]);

      // Update default dot position
      if (newData.length > 0) {
        defaultDot.attr(
          "transform",
          `translate(${x(newData.length - 1)},${y(newData[newData.length - 1] || 0)})`,
        );
      }

      // Update area and line paths
      areaPath.datum(newData).attr("d", area);

      linePath.datum(newData).attr("d", line);

      if (lastMouseX !== null) {
        handleMouseMove(null);
      }
    };

    return {
      update,
      updateHover,
      svg, // Return the svg element for cleanup
    };
  }

  function createBytesChart(element, data, options = {}) {
    const MAX_DATA_POINTS = 300; // Keep 5 minutes of data at 1 second intervals

    // Trim data to max length if needed
    if (data.length > MAX_DATA_POINTS) {
      data = data.slice(-MAX_DATA_POINTS);
    }

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
      .domain([0, d3.max(data) * 1.1 || 1])
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

    const areaPath = chartGroup
      .append("path")
      .datum(data)
      .attr("fill", config.areaColor)
      .attr("fill-opacity", config.areaOpacity)
      .attr("d", area);

    const linePath = chartGroup
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
        focus
          .style("display", null)
          .attr(
            "transform",
            `translate(${x(xValue)},${y(currentData[xValue])})`,
          );

        hoveredXValue = xValue;
      }
    };

    // Add default state dot (visible when not hovering)
    const defaultDot = chartGroup
      .append("circle")
      .attr("r", 4)
      .attr("fill", config.lineColor)
      .attr(
        "transform",
        `translate(${x(data.length - 1)},${y(data[data.length - 1] || 0)})`,
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
        hoveredXValue = null;
      })
      .on("mousemove", handleMouseMove);

    // Add function to update hover state externally
    const updateHover = (xValue: number | null) => {
      if (xValue === null) {
        focus.style("display", "none");
        defaultDot.style("display", null);
        hovered_messages_processed_bytes = null;
      } else if (xValue >= 0 && xValue < currentData.length) {
        const yValue = currentData[xValue];
        focus
          .style("display", null)
          .attr("transform", `translate(${x(xValue)},${y(yValue)})`);
        defaultDot.style("display", "none");
        hovered_messages_processed_bytes = yValue;
      }
    };

    // Improved update function that can handle dimension changes
    const update = function (newData, newDimensions = null) {
      // Trim incoming data if needed
      if (newData.length > MAX_DATA_POINTS) {
        newData = newData.slice(-MAX_DATA_POINTS);
      }
      currentData = newData;

      // Update scales if dimensions changed
      if (newDimensions) {
        const newWidth = newDimensions.width;
        const newHeight = newDimensions.height;

        const newChartWidth =
          newWidth - config.margin.left - config.margin.right;
        const newChartHeight =
          newHeight - config.margin.top - config.margin.bottom;

        x.range([0, newChartWidth]);
        y.range([newChartHeight, 0]);

        // Update mouse area dimensions
        mouseArea.attr("width", newWidth).attr("height", newHeight);

        // Update chart group position
        chartGroup.attr(
          "transform",
          `translate(${config.margin.left},${config.margin.top})`,
        );
      }

      // Update y domain based on new data
      y.domain([0, d3.max(newData) * 1.1 || 1]);

      // Update default dot position
      if (newData.length > 0) {
        defaultDot.attr(
          "transform",
          `translate(${x(newData.length - 1)},${y(newData[newData.length - 1] || 0)})`,
        );
      }

      // Update area and line paths
      areaPath.datum(newData).attr("d", area);

      linePath.datum(newData).attr("d", line);

      if (lastMouseX !== null) {
        handleMouseMove(null);
      }
    };

    return {
      update,
      updateHover,
      svg, // Return the svg element for cleanup
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

  // Function to handle backfill form submission
  function handleBackfillSubmit() {
    isSubmittingBackfill = true;
    let position;

    if (backfillForm.startPosition === "beginning") {
      position = null;
    } else {
      if (
        backfillForm.sortColumnAttnum &&
        backfillForm.initialSortColumnValue
      ) {
        // If it's a timestamp and the value is a Date object, convert to ISO string
        if (
          typeof backfillForm.initialSortColumnValue === "object" &&
          backfillForm.initialSortColumnValue instanceof Date
        ) {
          position = backfillForm.initialSortColumnValue.toISOString();
        } else {
          position = backfillForm.initialSortColumnValue;
        }
      } else {
        // Handle validation error
        backfillFormErrors = {
          initialSortColumnValue: "Please select a valid position value",
        };
        isSubmittingBackfill = false;
        return;
      }
    }

    showBackfillDialog = false;

    live.pushEventTo("#" + parent, "run-backfill", backfillForm, (reply) => {
      isSubmittingBackfill = false;
      if (!reply.ok) {
        // Handle error
        backfillFormErrors = reply.errors || {};
      }
    });
  }

  function openBackfillDialog() {
    // Reset form state
    backfillForm = {
      startPosition: "beginning",
      sortColumnAttnum: null,
      initialSortColumnValue: null,
    };
    backfillFormErrors = {};
    showBackfillDialog = true;
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
              {#if metrics_loading}
                <div class="text-md font-light">-</div>
              {:else}
                <div class="text-md font-bold">
                  {metrics.messages_pending_count
                    ? formatNumberWithCommas(metrics.messages_pending_count)
                    : "0"}
                </div>
              {/if}
              <div class="text-xs text-gray-500">pending</div>
            </span>
            <span>
              {#if metrics_loading}
                <div class="text-md font-light">-</div>
              {:else}
                <div class="text-md font-bold">
                  {metrics.messages_processed_count
                    ? formatNumberWithCommas(metrics.messages_processed_count)
                    : "0"}
                </div>
              {/if}
              <div class="text-xs text-gray-500">processed</div>
            </span>
            <span>
              {#if metrics_loading}
                <div class="text-md font-light">-</div>
              {:else}
                <div
                  class="text-md font-bold {metrics.messages_failing_count > 0
                    ? 'text-red-500'
                    : 'text-black'}"
                >
                  {formatNumberWithCommas(metrics.messages_failing_count)}
                </div>
              {/if}
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
                {#if metrics_loading}
                  <div class="text-md font-light">-</div>
                {:else}
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
                {/if}
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
                {#if metrics_loading}
                  <div class="text-md font-light">-</div>
                {:else if (hovered_messages_processed_bytes ?? metrics.messages_processed_bytes) !== null}
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
        onRun={() => openBackfillDialog()}
        onCancel={(callback) => {
          live.pushEventTo("#" + parent, "cancel-backfill", {}, (reply) =>
            callback(reply),
          );
        }}
      />

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <h2 class="text-lg font-semibold flex items-center gap-2">
              Transform
              <Beta size="sm" variant="subtle" />
            </h2>
            {#if transform && !isRedisConsumer(consumer)}
              <a
                href="/transforms/{transform.id}"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <Button variant="outline" size="sm">
                  <ExternalLink class="h-4 w-4 mr-2" />
                  View Transform
                </Button>
              </a>
            {/if}
          </div>
          {#if isRedisConsumer(consumer)}
            <p class="text-sm text-muted-foreground">
              Transforms are coming soon for Redis sinks. <a
                href="https://github.com/sequinstream/sequin/issues/1186"
                target="_blank"
                rel="noopener noreferrer"
                class="text-primary underline">Let us know</a
              > if you want this.
            </p>
          {:else if transform}
            <div class="grid grid-cols-1 gap-4">
              <div>
                <span class="text-sm text-gray-500">Name</span>
                <div class="mt-2">
                  <span class="">{transform.name}</span>
                </div>
              </div>

              {#if transform.description}
                <div>
                  <span class="text-sm text-gray-500">Description</span>
                  <div class="mt-2">
                    <span class="">{transform.description}</span>
                  </div>
                </div>
              {/if}

              {#if transform.transform.type === "path"}
                <div>
                  <span class="text-sm text-gray-500">Path</span>
                  <div class="mt-2">
                    <span
                      class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
                      >{transform.transform.path}</span
                    >
                  </div>
                </div>
              {:else if transform.transform.type === "function"}
                <div>
                  <span class="text-sm text-gray-500">Code</span>
                  <div class="mt-2">
                    <pre
                      class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
                        >{transform.transform.code}</code
                      ></pre>
                  </div>
                </div>
              {/if}
            </div>
          {:else}
            <p class="text-sm text-muted-foreground">
              No transform in use. Messages will be sent as-is to the sink
              destination.
            </p>
          {/if}
        </CardContent>
      </Card>

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

<Dialog.Root bind:open={showBackfillDialog}>
  <Dialog.Content class="md:max-w-4xl overflow-visible">
    <Dialog.Header>
      <Dialog.Title>Start backfill</Dialog.Title>
      <Dialog.Description>
        Choose where you want to start the backfill from.
      </Dialog.Description>
    </Dialog.Header>
    <div class="grid gap-4 py-4">
      {#if consumer.table}
        <BackfillForm
          table={consumer.table}
          form={backfillForm}
          formErrors={backfillFormErrors}
        />
      {:else}
        <p class="text-sm text-muted-foreground">
          Table information is not available.
        </p>
      {/if}
    </div>
    <Dialog.Footer>
      <Button
        type="submit"
        on:click={handleBackfillSubmit}
        disabled={isSubmittingBackfill}
      >
        {#if isSubmittingBackfill}
          <Loader2 class="mr-2 h-4 w-4 animate-spin" />
        {/if}
        <ArrowDownSquare class="mr-2 h-4 w-4" />
        Start Backfill
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
