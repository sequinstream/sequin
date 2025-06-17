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
  import { ChevronRight, ChevronLeft } from "lucide-svelte";
  import { Database, Plug, Webhook, Pause, StopCircle } from "lucide-svelte";
  import { ArrowDownSquare } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";
  import HealthPill from "../health/HealthPill.svelte";
  import AzureEventHubIcon from "../sinks/azure_event_hub/AzureEventHubIcon.svelte";
  import DatabaseConnectionAlert from "../components/DatabaseConnectionAlert.svelte";
  import LinkPatchNavigate from "$lib/components/LinkPatchNavigate.svelte";
  import SqsIcon from "../sinks/sqs/SqsIcon.svelte";
  import SnsIcon from "../sinks/sns/SnsIcon.svelte";
  import KinesisIcon from "../sinks/kinesis/KinesisIcon.svelte";
  import S2Icon from "../sinks/s2/S2Icon.svelte";
  import RedisIcon from "../sinks/redis_shared/RedisIcon.svelte";
  import KafkaIcon from "../sinks/kafka/KafkaIcon.svelte";
  import GcpPubsubIcon from "../sinks/gcp_pubsub/GcpPubsubIcon.svelte";
  import SequinStreamIcon from "../sinks/sequin_stream/SequinStreamIcon.svelte";
  import NatsIcon from "../sinks/nats/NatsIcon.svelte";
  import RabbitMqIcon from "../sinks/rabbitmq/RabbitMqIcon.svelte";

  import TypesenseIcon from "../sinks/typesense/TypesenseIcon.svelte";
  import ElasticsearchIcon from "../sinks/elasticsearch/ElasticsearchIcon.svelte";

  import { Badge } from "$lib/components/ui/badge";
  import * as d3 from "d3";
  import { onMount } from "svelte";
  import { Skeleton } from "$lib/components/ui/skeleton";
  import { pageStore } from "../stores/pageStore";

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type:
      | "http_push"
      | "sqs"
      | "sns"
      | "kinesis"
      | "redis_stream"
      | "kafka"
      | "gcp_pubsub"
      | "sequin_stream"
      | "nats"
      | "rabbitmq"
      | "typesense"
      | "elasticsearch";

    status: "active" | "disabled" | "paused";
    database_name: string;
    active_backfill: boolean;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
    metrics: {
      messages_processed_throughput_timeseries: number[];
    };
  }> | null;
  export let live: any;
  export let hasDatabases: boolean;
  export let selfHosted: boolean;
  export let page: number;
  export let pageSize: number;
  export let totalCount: number;
  let selectedDestination: string;
  let dialogOpen = false;

  $: pageLoading = consumers === null;
  $: hasConsumers = totalCount > 0;
  $: pageCount = Math.ceil(totalCount / pageSize);
  $: startIndex = page * pageSize + 1;
  $: endIndex = Math.min((page + 1) * pageSize, totalCount);

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
      id: "sns",
      name: "Amazon SNS",
      icon: SnsIcon,
    },
    {
      id: "kinesis",
      name: "Amazon Kinesis",
      icon: KinesisIcon,
    },
    {
      id: "s2",
      name: "S2",
      icon: S2Icon,
    },
    {
      id: "redis_stream",
      name: "Redis Stream",
      icon: RedisIcon,
    },
    {
      id: "redis_string",
      name: "Redis String",
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
    {
      id: "typesense",
      name: "Typesense",
      icon: TypesenseIcon,
    },
    {
      id: "elasticsearch",
      name: "Elasticsearch",
      icon: ElasticsearchIcon,
    },
  ];

  function handleConsumerClick(id: string, type: string) {
    // Store current page before navigation
    pageStore.set(page.toString());
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
    if (consumers !== null) {
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
  });

  $: {
    if (consumers !== null) {
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
  }

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < pageCount) {
      page = newPage;
      pageLoading = true;
      live.pushEvent("change_page", { page });
    }
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
        <Table.Row class="text-xs text-gray-500 uppercase tracking-wide">
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
        {#if pageLoading}
          {#each Array(pageSize) as _, i}
            <Table.Row>
              <Table.Cell class="w-8">
                <Skeleton class="h-6 w-6 rounded" />
              </Table.Cell>
              <Table.Cell class="w-32">
                <Skeleton class="h-4 w-24" />
              </Table.Cell>
              <Table.Cell class="w-24">
                <Skeleton class="h-6 w-24" />
              </Table.Cell>
              <Table.Cell class="w-40">
                <Skeleton class="h-4 w-40" />
              </Table.Cell>
              <Table.Cell class="w-48">
                <Skeleton class="h-8 w-48" />
              </Table.Cell>
              <Table.Cell class="w-24">
                <Skeleton class="h-4 w-24" />
              </Table.Cell>
            </Table.Row>
          {/each}
        {:else}
          {#each consumers as consumer}
            <Table.Row class="cursor-pointer">
              <LinkPatchNavigate
                href={`/sinks/${consumer.type}/${consumer.id}`}
                class="contents"
              >
                <Table.Cell class="w-8">
                  {#each sinks.filter((d) => d.id === consumer.type) as dest}
                    <svelte:component this={dest.icon} class="h-6 w-6" />
                  {/each}
                </Table.Cell>
                <Table.Cell class="w-32">{consumer.name}</Table.Cell>
                <Table.Cell class="w-24">
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
                <Table.Cell class="w-40">{consumer.database_name}</Table.Cell>
                <Table.Cell class="w-48">
                  <div use:bindChartElement={consumer.id} class="w-48 h-8" />
                </Table.Cell>
                <Table.Cell class="w-24"
                  >{formatRelativeTimestamp(consumer.insertedAt)}</Table.Cell
                >
              </LinkPatchNavigate>
            </Table.Row>
          {/each}
        {/if}
      </Table.Body>
    </Table.Root>
    {#if totalCount > pageSize}
      <div class="flex justify-between items-center mt-4">
        <div>
          Showing {startIndex} to {endIndex} of {totalCount} sinks
        </div>
        <div class="flex gap-2">
          <Button
            variant="outline"
            on:click={() => changePage(page - 1)}
            disabled={page === 0 || pageLoading}
          >
            <ChevronLeft class="h-4 w-4 mr-1" />
            Previous
          </Button>
          <Button
            variant="outline"
            on:click={() => changePage(page + 1)}
            disabled={page >= pageCount - 1 || pageLoading}
          >
            Next
            <ChevronRight class="h-4 w-4 ml-1" />
          </Button>
        </div>
      </div>
    {/if}
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
      <div class="grid grid-cols-3 gap-4 py-4">
        {#each sinks as dest}
          <LinkPatchNavigate href={`/sinks/new?kind=${dest.id}`}>
            <Label
              for={dest.id}
              class="flex flex-col items-center justify-between rounded-md border-2 border-muted bg-popover p-4 hover:bg-accent hover:text-accent-foreground text-center leading-tight cursor-pointer"
            >
              <svelte:component this={dest.icon} class="mb-3 h-12 w-12" />
              {dest.name}
            </Label>
          </LinkPatchNavigate>
        {/each}
      </div>
    </div>
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
