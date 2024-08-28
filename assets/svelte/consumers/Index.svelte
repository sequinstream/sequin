<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import {
    AlertCircle,
    ChevronDown,
    Radio,
    ArrowRightToLine,
    ArrowLeftFromLine,
    CirclePlay,
    CircleStop,
    Database,
  } from "lucide-svelte";
  import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
  } from "$lib/components/ui/dropdown-menu";
  import { formatRelativeTimestamp } from "$lib/utils";
  import {
    Alert,
    AlertDescription,
    AlertTitle,
  } from "$lib/components/ui/alert";

  export let consumers: Array<{
    id: string;
    name: string;
    insertedAt: string;
    type: "pull" | "push";
    status: "active" | "disabled";
    database_name: string;
  }>;
  export let live: any;
  export let hasDatabases: boolean;

  function handleConsumerClick(id: string) {
    live.pushEvent("consumer_clicked", { id });
  }
</script>

<div class="container mx-auto py-10">
  {#if !hasDatabases}
    <Alert class="bg-[#0A0A0A] border-[#1F1F1F] text-white w-full mb-8">
      <div class="grid grid-cols-[auto_1fr] gap-2 items-center">
        <AlertCircle class="h-5 w-5" style="color: white" />
        <AlertTitle class="text-lg font-semibold">
          First, you need to connect to a database
        </AlertTitle>
        <AlertDescription class="text-[#A1A1AA] col-start-2">
          Sequin must be connected to at least one Postgres database before you
          can create a consumer.
        </AlertDescription>

        <a
          href="/databases/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
          class="col-start-2"
        >
          <Button
            variant="default"
            class="bg-matcha-600 text-white border-matcha-700 hover:bg-matcha-700 hover:text-white transition-colors duration-200 shadow-lg hover:shadow-xl"
          >
            Connect database
          </Button>
        </a>
      </div>
    </Alert>
  {/if}

  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Radio class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Consumers</h1>
    </div>
    {#if hasDatabases}
      <div class="relative inline-block text-left">
        <div class="inline-flex rounded-md shadow-sm">
          <a
            href="/consumers/new"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button class="rounded-r-none" variant="default"
              >Create Consumer</Button
            >
          </a>
          <DropdownMenu>
            <DropdownMenuTrigger asChild let:builder>
              <Button
                variant="default"
                builders={[builder]}
                class="px-2 rounded-l-none border-l border-primary/20"
              >
                <ChevronDown class="h-4 w-4" />
                <span class="sr-only">Open options</span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <a
                href="/consumers/new?kind=push"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <DropdownMenuItem class="cursor-pointer">
                  Quick create Push Consumer
                </DropdownMenuItem>
              </a>
              <a
                href="/consumers/new?kind=pull"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <DropdownMenuItem class="cursor-pointer">
                  Quick create Pull Consumer
                </DropdownMenuItem>
              </a>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </div>
    {/if}
  </div>

  <Table.Root>
    <Table.Header>
      <Table.Row>
        <Table.Head>Name</Table.Head>
        <Table.Head>
          <div class="flex items-center">
            <Database class="h-4 w-4 mr-2" />
            <span>Database</span>
          </div>
        </Table.Head>
        <Table.Head>Type</Table.Head>
        <Table.Head>Status</Table.Head>
        <Table.Head>Created at</Table.Head>
      </Table.Row>
    </Table.Header>
    <Table.Body>
      {#each consumers as consumer}
        <Table.Row
          on:click={() => handleConsumerClick(consumer.id)}
          class="cursor-pointer"
        >
          <Table.Cell>{consumer.name}</Table.Cell>
          <Table.Cell>{consumer.database_name}</Table.Cell>
          <Table.Cell>
            <div class="flex items-center">
              <span
                class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium border border-black"
              >
                {#if consumer.type === "pull"}
                  <ArrowLeftFromLine class="h-4 w-4 mr-1 text-black" />
                {:else if consumer.type === "push"}
                  <ArrowRightToLine class="h-4 w-4 mr-1 text-black" />
                {/if}
                {consumer.type[0].toUpperCase() + consumer.type.slice(1)} consumer
              </span>
            </div>
          </Table.Cell>
          <Table.Cell>
            <span
              class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium"
              class:text-purple-500={consumer.status === "active"}
              class:text-gray-400={consumer.status === "disabled"}
            >
              {#if consumer.status === "active"}
                <CirclePlay class="h-4 w-4 mr-1" />
              {:else if consumer.status === "disabled"}
                <CircleStop class="h-4 w-4 mr-1" />
              {/if}
              {consumer.status[0].toUpperCase() + consumer.status.slice(1)}
            </span>
          </Table.Cell>
          <Table.Cell>{formatRelativeTimestamp(consumer.insertedAt)}</Table.Cell
          >
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
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
