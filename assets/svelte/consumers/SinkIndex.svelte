<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import {
    Dialog,
    DialogContent,
    DialogDescription,
    DialogFooter,
    DialogHeader,
    DialogTitle,
  } from "$lib/components/ui/dialog";
  import { Label } from "$lib/components/ui/label";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { ChevronRight } from "lucide-svelte";
  import { Database, Plug, Webhook, Pause, StopCircle } from "lucide-svelte";
  import { ArrowDownSquare } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";
  import HealthPill from "../health/HealthPill.svelte";
  import AzureEventHubIcon from "../sinks/azure_event_hub/AzureEventHubIcon.svelte";
  import DatabaseConnectionAlert from "../components/DatabaseConnectionAlert.svelte";
  import LinkPatchNavigate from "$lib/components/LinkPatchNavigate.svelte";
  import SqsIcon from "../sinks/sqs/SqsIcon.svelte";
  import RedisIcon from "../sinks/redis/RedisIcon.svelte";
  import KafkaIcon from "../sinks/kafka/KafkaIcon.svelte";
  import GcpPubsubIcon from "../sinks/gcp_pubsub/GcpPubsubIcon.svelte";
  import SequinStreamIcon from "../sinks/sequin_stream/SequinStreamIcon.svelte";
  import NatsIcon from "../sinks/nats/NatsIcon.svelte";
  import RabbitMqIcon from "../sinks/rabbitmq/RabbitMqIcon.svelte";
  import { Badge } from "$lib/components/ui/badge";
  import * as d3 from "d3";
  import { onMount } from "svelte";

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type:
      | "http_push"
      | "sqs"
      | "redis"
      | "kafka"
      | "gcp_pubsub"
      | "sequin_stream"
      | "nats"
      | "rabbitmq";
    status: "active" | "disabled" | "paused";
    database_name: string;
    active_backfill: boolean;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
    metrics: {
      messages_processed_throughput_timeseries: number[];
    };
  }>;
  export let live: any;
  export let hasDatabases: boolean;
  export let selfHosted: boolean;
  let selectedDestination: string;
  let dialogOpen = false;

  const hasConsumers = consumers.length > 0;

  const sinks = [
    {
      id: "http_push",
      name: "Webhook",
      icon: Webhook,
    },
    {
      id: "sqs",
      name: "Amazon SQS",
      icon: SqsIcon,
    },
    {
      id: "redis",
      name: "Redis",
      icon: RedisIcon,
    },
    {
      id: "kafka",
      name: "Kafka",
      icon: KafkaIcon,
    },
    {
      id: "gcp_pubsub",
      name: "GCP Pub/Sub",
      icon: GcpPubsubIcon,
    },
    {
      id: "sequin_stream",
      name: "Sequin Stream",
      icon: SequinStreamIcon,
    },
    {
      id: "nats",
      name: "NATS",
      icon: NatsIcon,
    },
    {
      id: "rabbitmq",
      name: "RabbitMQ",
      icon: RabbitMqIcon,
    },
    {
      id: "azure_event_hub",
      name: "Azure EventHub",
      icon: AzureEventHubIcon,
    },
  ];

  function handleConsumerClick(id: string, type: string) {
    live.pushEvent("consumer_clicked", { id, type });
  }

  function createMiniChart(element, data, options = {}) {
    const config = {
      width: element.clientWidth,
      height: element.clientHeight,
      margin: { top: 2, right: 2, bottom: 2, left: 2 },
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

    const chartGroup = svg
      .append("g")
      .attr(
        "transform",
        `translate(${config.margin.left},${config.margin.top})`,
      );

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
      .attr("stroke-width", 1)
      .attr("d", line);

    return svg;
  }

  let chartElements = {};

  function bindChartElement(node, consumerId) {
    chartElements[consumerId] = node;
    return {
      destroy() {
        delete chartElements[consumerId];
      },
    };
  }

  onMount(() => {
    // Create charts for each consumer
    consumers.forEach((consumer) => {
      const element = chartElements[consumer.id];
      if (
        element &&
        consumer.metrics.messages_processed_throughput_timeseries.length > 0
      ) {
        createMiniChart(
          element,
          consumer.metrics.messages_processed_throughput_timeseries,
        );
      }
    });
  });

  $: if (consumers) {
    // Update charts when consumers data changes
    consumers.forEach((consumer) => {
      const element = chartElements[consumer.id];
      if (
        element &&
        consumer.metrics.messages_processed_throughput_timeseries.length > 0
      ) {
        createMiniChart(
          element,
          consumer.metrics.messages_processed_throughput_timeseries,
        );
      }
    });
  }
</script>

<div class="container mx-auto py-10">
  <DatabaseConnectionAlert
    show={!hasDatabases}
    entityName="Sink"
    showTryWithLocalDatabaseCTA={!selfHosted}
  />

  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Plug class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Sinks</h1>
    </div>
    {#if hasDatabases && hasConsumers}
      <div class="relative inline-block text-left">
        <Button variant="default" on:click={() => (dialogOpen = true)}
          >Create Sink</Button
        >
      </div>
    {/if}
  </div>

  {#if !hasConsumers}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No Sinks</h2>
        <p class="text-gray-600 mb-6">
          Sinks filter, transform, and send messages from a table in your
          database to your application or another service.
        </p>
        <div class="relative inline-block text-left">
          <Button
            variant="default"
            disabled={!hasDatabases}
            on:click={() => (dialogOpen = true)}>Create Sink</Button
          >
        </div>
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head></Table.Head>
          <Table.Head>Name</Table.Head>
          <Table.Head>Status</Table.Head>
          <Table.Head>
            <div class="flex items-center">
              <Database class="h-4 w-4 mr-2" />
              <span>Database</span>
            </div>
          </Table.Head>
          <Table.Head>Throughput</Table.Head>
          <Table.Head>Created</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each consumers as consumer}
          <Table.Row
            on:click={() => handleConsumerClick(consumer.id, consumer.type)}
            class="cursor-pointer"
          >
            <Table.Cell>
              {#each sinks.filter((d) => d.id === consumer.type) as dest}
                <svelte:component this={dest.icon} class="h-6 w-6" />
              {/each}
            </Table.Cell>
            <Table.Cell>{consumer.name}</Table.Cell>
            <Table.Cell>
              <div class="flex items-center gap-2">
                {#if consumer.status === "paused"}
                  <Badge variant="warning">
                    <Pause class="h-4 w-4 mr-1" />
                    <span>Paused</span>
                  </Badge>
                {:else if consumer.status === "disabled"}
                  <Badge variant="secondary">
                    <StopCircle class="h-4 w-4 mr-1" />
                    <span>Disabled</span>
                  </Badge>
                {:else}
                  <HealthPill status={consumer.health.status} />
                {/if}
                {#if consumer.active_backfill}
                  <Badge variant="secondary">
                    <ArrowDownSquare class="h-4 w-4 mr-1" />
                    <span>Backfilling</span>
                  </Badge>
                {/if}
              </div>
            </Table.Cell>
            <Table.Cell>{consumer.database_name}</Table.Cell>
            <Table.Cell>
              <div use:bindChartElement={consumer.id} class="w-48 h-8" />
            </Table.Cell>
            <Table.Cell
              >{formatRelativeTimestamp(consumer.insertedAt)}</Table.Cell
            >
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>

<Dialog bind:open={dialogOpen}>
  <DialogContent class="sm:max-w-[800px] flex flex-col">
    <DialogHeader>
      <DialogTitle>Choose Sink Type</DialogTitle>
      <DialogDescription>
        Where do you want to stream Postgres to?
      </DialogDescription>
    </DialogHeader>
    <div class="flex-1 overflow-y-auto">
      <RadioGroup
        bind:value={selectedDestination}
        class="grid grid-cols-3 gap-4 py-4"
      >
        {#each sinks as dest}
          <Label
            for={dest.id}
            class="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground [&:has([data-state=checked])]:border-primary text-center leading-tight cursor-pointer"
          >
            <RadioGroupItem value={dest.id} id={dest.id} class="sr-only" />
            <svelte:component this={dest.icon} class="mb-3 h-12 w-12" />
            {dest.name}
          </Label>
        {/each}
      </RadioGroup>
    </div>
    <DialogFooter class="mt-4">
      {#if selectedDestination}
        <LinkPatchNavigate
          class="w-full"
          href={`/sinks/new?kind=${selectedDestination}`}
        >
          <Button type="submit" class="w-full">
            Continue
            <ChevronRight class="ml-2 h-4 w-4" />
          </Button>
        </LinkPatchNavigate>
      {:else}
        <Button type="submit" class="w-full" disabled>
          Continue
          <ChevronRight class="ml-2 h-4 w-4" />
        </Button>
      {/if}
    </DialogFooter>
  </DialogContent>
</Dialog>

<style>
  :global(.alert) {
    animation: fadeIn 0.3s ease-out;
  }

  @keyframes fadeIn {
    from {
      opacity: 0;
      transform: translateY(-10px);
    }
    to {
      opacity: 1;
      transform: translateY(0);
    }
  }
</style>
