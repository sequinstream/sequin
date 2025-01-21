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
  import { Database, Plug, Webhook } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";
  import HealthPill from "../health/HealthPill.svelte";
  import DatabaseConnectionAlert from "../components/DatabaseConnectionAlert.svelte";
  import LinkPatchNavigate from "$lib/components/LinkPatchNavigate.svelte";
  import SqsIcon from "../sinks/sqs/SqsIcon.svelte";
  import RedisIcon from "../sinks/redis/RedisIcon.svelte";
  import KafkaIcon from "../sinks/kafka/KafkaIcon.svelte";
  import GcpPubsubIcon from "../sinks/gcp_pubsub/GcpPubsubIcon.svelte";
  import SequinStreamIcon from "../../icons/sequin_stream.svelte";
  import NatsIcon from "../sinks/nats/NatsIcon.svelte";
  import RabbitMqIcon from "../sinks/rabbitmq/RabbitMqIcon.svelte";

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
    status: "active" | "disabled";
    database_name: string;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
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
  ];

  function handleConsumerClick(id: string, type: string) {
    live.pushEvent("consumer_clicked", { id, type });
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
              <HealthPill status={consumer.health.status} />
            </Table.Cell>
            <Table.Cell>{consumer.database_name}</Table.Cell>
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
  <DialogContent class="sm:max-w-[425px]">
    <DialogHeader>
      <DialogTitle>Choose Sink Type</DialogTitle>
      <DialogDescription>
        Where do you want to stream Postgres to?
      </DialogDescription>
    </DialogHeader>
    <RadioGroup
      bind:value={selectedDestination}
      class="grid grid-cols-2 gap-4 py-4"
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
    <DialogFooter>
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
