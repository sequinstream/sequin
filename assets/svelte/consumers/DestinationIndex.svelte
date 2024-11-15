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
    DialogTrigger,
  } from "$lib/components/ui/dialog";
  import { Label } from "$lib/components/ui/label";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { ChevronRight } from "lucide-svelte";
  import {
    AlertCircle,
    ListOrdered,
    CirclePlay,
    CircleStop,
    Database,
    SendHorizontal,
    Webhook,
  } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";
  import {
    Alert,
    AlertDescription,
    AlertTitle,
  } from "$lib/components/ui/alert";
  import HealthPill from "../health/HealthPill.svelte";
  import { Badge } from "$lib/components/ui/badge";
  import DatabaseConnectionAlert from "../components/DatabaseConnectionAlert.svelte";
  import LinkPushNavigate from "$lib/components/LinkPushNavigate.svelte";
  import LinkPatchNavigate from "$lib/components/LinkPatchNavigate.svelte";
  import SQSIcon from "../../icons/sqs.svelte";
  import RedisIcon from "../../icons/redis.svelte";
  import KafkaIcon from "../../icons/kafka.svelte";

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type: "http_push" | "sqs";
    status: "active" | "disabled";
    database_name: string;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
  }>;
  export let live: any;
  export let hasDatabases: boolean;
  export let hasSequences: boolean;

  let selectedDestination: string;
  let dialogOpen = false;

  const hasConsumers = consumers.length > 0;

  const destinations = [
    {
      id: "http_push",
      name: "HTTP Endpoint",
      icon: Webhook,
    },
    {
      id: "sqs",
      name: "Amazon SQS",
      icon: SQSIcon,
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
  ];

  function handleConsumerClick(id: string, type: string) {
    live.pushEvent("consumer_clicked", { id, type });
  }
</script>

<div class="container mx-auto py-10">
  <DatabaseConnectionAlert
    show={!hasDatabases}
    entityName="Destination Consumers"
  />

  {#if hasDatabases && !hasSequences}
    <Alert class="bg-carbon-50 border-carbon-200 text-carbon-900 w-full mb-8">
      <div class="grid grid-cols-[auto_1fr] gap-2 items-center">
        <AlertCircle class="h-5 w-5 text-carbon-600" />
        <AlertTitle class="text-lg font-semibold text-carbon-900">
          First, you need to create a Stream
        </AlertTitle>
        <AlertDescription class="text-carbon-600 col-start-2">
          Sequin must have at least one Stream before you can create a
          Destination Consumer.
        </AlertDescription>

        <div class="flex mt-2 gap-4 col-start-2">
          <LinkPushNavigate href="/streams/new">
            <Button
              variant="default"
              class="bg-blue-600 text-white border-blue-700 hover:bg-blue-700 hover:text-white transition-colors duration-200 shadow-lg hover:shadow-xl"
            >
              <ListOrdered class="inline-block h-4 w-4 mr-2" />
              Create Stream
            </Button>
          </LinkPushNavigate>
        </div>
      </div>
    </Alert>
  {/if}

  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <SendHorizontal class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Destination Consumers</h1>
    </div>
    {#if hasDatabases && hasConsumers}
      <div class="relative inline-block text-left">
        <Button variant="default" on:click={() => (dialogOpen = true)}
          >Create Destination Consumer</Button
        >
      </div>
    {/if}
  </div>

  {#if !hasConsumers}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No Destination Consumers</h2>
        <p class="text-gray-600 mb-6">
          Destination Consumers filter, transform, and send messages from a
          table in your database to your application or another service.
        </p>
        <div class="relative inline-block text-left">
          {#if hasSequences}
            <Button variant="default" on:click={() => (dialogOpen = true)}
              >Create Destination Consumer</Button
            >
          {:else}
            <Button disabled>Create Destination Consumer</Button>
          {/if}
        </div>
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head></Table.Head>
          <Table.Head>Name</Table.Head>
          <Table.Head>Health</Table.Head>
          <Table.Head>
            <div class="flex items-center">
              <Database class="h-4 w-4 mr-2" />
              <span>Database</span>
            </div>
          </Table.Head>
          <Table.Head>Status</Table.Head>
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
              {#each destinations.filter((d) => d.id === consumer.type) as dest}
                <svelte:component this={dest.icon} class="h-6 w-6" />
              {/each}
            </Table.Cell>
            <Table.Cell>{consumer.name}</Table.Cell>
            <Table.Cell>
              <HealthPill status={consumer.health.status} />
            </Table.Cell>
            <Table.Cell>{consumer.database_name}</Table.Cell>
            <Table.Cell>
              {#if consumer.status === "active"}
                <Badge variant="default"
                  ><CirclePlay class="h-4 w-4 mr-1" />Active</Badge
                >
              {:else}
                <Badge variant="disabled"
                  ><CircleStop class="h-4 w-4 mr-1" />Disabled</Badge
                >
              {/if}
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
  <DialogContent class="sm:max-w-[425px]">
    <DialogHeader>
      <DialogTitle>Choose Destination</DialogTitle>
      <DialogDescription>
        Where do you want to stream Postgres to?
      </DialogDescription>
    </DialogHeader>
    <RadioGroup
      bind:value={selectedDestination}
      class="grid grid-cols-2 gap-4 py-4"
    >
      {#each destinations as dest}
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
          href={`/consumers/new?kind=${selectedDestination}`}
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
