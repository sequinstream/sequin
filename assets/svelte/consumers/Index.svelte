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
    Zap,
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
  import HealthPill from "../health/HealthPill.svelte";
  import {
    Popover,
    PopoverTrigger,
    PopoverContent,
  } from "$lib/components/ui/popover";
  import { Badge } from "$lib/components/ui/badge";

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

  const hasConsumers = consumers.length > 0;

  function handleConsumerClick(id: string) {
    live.pushEvent("consumer_clicked", { id });
  }
</script>

<div class="container mx-auto py-10">
  {#if !hasDatabases}
    <Alert class="bg-carbon-50 border-carbon-200 text-carbon-900 w-full mb-8">
      <div class="grid grid-cols-[auto_1fr] gap-2 items-center">
        <AlertCircle class="h-5 w-5 text-carbon-600" />
        <AlertTitle class="text-lg font-semibold text-carbon-900">
          First, you need to connect to a database
        </AlertTitle>
        <AlertDescription class="text-carbon-600 col-start-2">
          Sequin must be connected to at least one Postgres database before you
          can create a consumer.
        </AlertDescription>

        <div class="flex mt-2 gap-4 col-start-2">
          <a
            href="/databases/new"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button
              variant="default"
              class="bg-blue-600 text-white border-blue-700 hover:bg-blue-700 hover:text-white transition-colors duration-200 shadow-lg hover:shadow-xl"
              ><Database class="inline-block h-4 w-4 mr-2" />
              Connect database
            </Button>
          </a>
          <Popover>
            <PopoverTrigger>
              <Button variant="magic">
                <Zap class="inline-block h-4 w-4 mr-2" /> Try with test database
              </Button>
            </PopoverTrigger>
            <PopoverContent class="w-80">
              <div class="space-y-2">
                <h4 class="font-medium">Need a test database?</h4>
                <p class="text-sm text-muted-foreground">
                  We recommend setting up a free database with Supabase to get
                  started.
                </p>
                <Button
                  variant="outline"
                  href="https://supabase.com/dashboard"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  Go to Supabase Dashboard
                </Button>
              </div>
            </PopoverContent>
          </Popover>
        </div>
      </div>
    </Alert>
  {/if}

  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Radio class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Consumers</h1>
    </div>
    {#if hasDatabases}
      {#if hasConsumers}
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
                  href="/consumers/new"
                  data-phx-link="redirect"
                  data-phx-link-state="push"
                >
                  <DropdownMenuItem class="cursor-pointer">
                    Create Consumer wizard
                  </DropdownMenuItem>
                </a>
                <a
                  href="/consumers/new?kind=push"
                  data-phx-link="redirect"
                  data-phx-link-state="push"
                >
                  <DropdownMenuItem class="cursor-pointer">
                    Create Push Consumer
                  </DropdownMenuItem>
                </a>
                <a
                  href="/consumers/new?kind=pull"
                  data-phx-link="redirect"
                  data-phx-link-state="push"
                >
                  <DropdownMenuItem class="cursor-pointer">
                    Create Pull Consumer
                  </DropdownMenuItem>
                </a>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>
        </div>
      {/if}
    {/if}
  </div>

  {#if !hasConsumers}
    {#if !hasDatabases}
      <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
        <div class="text-center py-12 w-1/2 mx-auto my-auto">
          <h2 class="text-xl font-semibold mb-4">No consumers</h2>
          <p class="text-gray-600 mb-6">
            Consumers filter, transform, and send messages from a table in your
            database to your application or another service.
          </p>
          <p class="text-gray-600 mb-6">
            You need to connect a database to Sequin before you can create a
            consumer.
          </p>
          <Button disabled={!hasDatabases}>Create consumer</Button>
        </div>
      </div>
    {/if}
    {#if hasDatabases && !hasConsumers}
      <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
        <div class="text-center py-12 w-1/2 mx-auto my-auto">
          <h2 class="text-xl font-semibold mb-4">No consumers</h2>
          <p class="text-gray-600 mb-6">
            Consumers filter, transform, and send messages from a table in your
            database to your application or another service.
          </p>
          <p class="text-gray-600 mb-6">
            Create your first consumer to start capturing changes.
          </p>
          <a
            href="/consumers/new"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button>Create consumer</Button>
          </a>
        </div>
      </div>
    {/if}
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
            <Table.Cell>
              <HealthPill status={consumer.health.status} />
            </Table.Cell>
            <Table.Cell>{consumer.database_name}</Table.Cell>
            <Table.Cell>
              {#if consumer.type === "pull"}
                <Badge variant="default">
                  <ArrowLeftFromLine class="h-4 w-4 mr-1" />
                  Pull consumer
                </Badge>
              {:else if consumer.type === "push"}
                <Badge variant="default">
                  <ArrowRightToLine class="h-4 w-4 mr-1" />
                  Push consumer
                </Badge>
              {/if}
            </Table.Cell>
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
