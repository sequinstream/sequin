<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import {
    AlertCircle,
    Radio,
    ListOrdered,
    CirclePlay,
    CircleStop,
    Database,
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

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type: "pull" | "push";
    status: "active" | "disabled";
    database_name: string;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
  }>;
  export let live: any;
  export let hasDatabases: boolean;
  export let hasSequences: boolean;
  export let consumerKind: "push" | "pull";

  const hasConsumers = consumers.length > 0;

  function handleConsumerClick(id: string) {
    live.pushEvent("consumer_clicked", { id });
  }
</script>

<div class="container mx-auto py-10">
  <DatabaseConnectionAlert
    show={!hasDatabases}
    entityName={consumerKind === "push"
      ? "Webhook Subscription"
      : "Consumer Group"}
  />

  {#if hasDatabases && !hasSequences}
    <Alert class="bg-carbon-50 border-carbon-200 text-carbon-900 w-full mb-8">
      <div class="grid grid-cols-[auto_1fr] gap-2 items-center">
        <AlertCircle class="h-5 w-5 text-carbon-600" />
        <AlertTitle class="text-lg font-semibold text-carbon-900">
          First, you need to create a Stream
        </AlertTitle>
        <AlertDescription class="text-carbon-600 col-start-2">
          Sequin must have at least one Stream before you can create {consumerKind ===
          "push"
            ? "a Webhook Subscription"
            : "a Consumer Group"}.
        </AlertDescription>

        <div class="flex mt-2 gap-4 col-start-2">
          <a
            href="/streams/new"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button
              variant="default"
              class="bg-blue-600 text-white border-blue-700 hover:bg-blue-700 hover:text-white transition-colors duration-200 shadow-lg hover:shadow-xl"
            >
              <ListOrdered class="inline-block h-4 w-4 mr-2" />
              Create Stream
            </Button>
          </a>
        </div>
      </div>
    </Alert>
  {/if}

  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      {#if consumerKind === "pull"}
        <Radio class="h-6 w-6 mr-2" />
      {:else}
        <Webhook class="h-6 w-6 mr-2" />
      {/if}
      <h1 class="text-2xl font-bold">
        {consumerKind === "push" ? "Webhook Subscriptions" : "Consumer Groups"}
      </h1>
    </div>
    {#if hasDatabases}
      {#if hasConsumers}
        <div class="relative inline-block text-left">
          <a
            href={`/consumers/new?kind=${consumerKind}`}
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button variant="default">
              Create {consumerKind === "push"
                ? "Subscription"
                : "Consumer Group"}
            </Button>
          </a>
        </div>
      {/if}
    {/if}
  </div>

  {#if !hasConsumers}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">
          {consumerKind === "push"
            ? "No Webhook Subscriptions"
            : "No Consumer Groups"}
        </h2>
        <p class="text-gray-600 mb-6">
          {#if consumerKind === "push"}
            Webhook Subscriptions filter, transform, and send messages from a
            table in your database to your application or another service.
          {:else}
            Consumer Groups let you filter, transform, and pull messages from
            your tables into your application with exactly-once processing.
          {/if}
        </p>
        <div class="relative inline-block text-left">
          {#if hasSequences}
            <a
              href={`/consumers/new?kind=${consumerKind}`}
              data-phx-link="redirect"
              data-phx-link-state="push"
            >
              <Button variant="default">
                Create {consumerKind === "push"
                  ? "Subscription"
                  : "Consumer Group"}
              </Button>
            </a>
          {:else}
            <Button disabled
              >Create {consumerKind === "push"
                ? "Subscription"
                : "Consumer Group"}</Button
            >
          {/if}
        </div>
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
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
            on:click={() => handleConsumerClick(consumer.id)}
            class="cursor-pointer"
          >
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
