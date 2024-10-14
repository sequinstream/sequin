<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import {
    Table as TableIcon,
    AlertCircle,
    Database,
    Zap,
    ArrowUpRight,
  } from "lucide-svelte";
  import * as Dialog from "$lib/components/ui/dialog";
  import NewSequenceForm from "./Form.svelte";
  import {
    Alert,
    AlertDescription,
    AlertTitle,
  } from "$lib/components/ui/alert";
  import {
    Popover,
    PopoverTrigger,
    PopoverContent,
  } from "$lib/components/ui/popover";
  import { createEventDispatcher } from "svelte";

  export let live;
  export let live_action: string;
  export let parent: string;
  export let sequences: Array<{
    id: string;
    table_name: string;
    table_schema: string;
    sort_column_name: string;
    inserted_at: string;
    consumer_count: number;
    postgres_database: {
      id: string;
      name: string;
    };
  }>;
  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<{
      oid: number;
      schema: string;
      name: string;
      columns: Array<{
        attnum: number;
        name: string;
        type: string;
      }>;
    }>;
  }>;

  let dialogOpen = live_action === "new";
  $: dialogOpen = live_action === "new";

  function closeDialog() {
    dialogOpen = false;
    window.history.pushState({}, "", "/sequences");
    dispatchEvent(new PopStateEvent("popstate"));
  }

  let newSequence = {
    table_name: "",
    table_schema: "",
    sort_column_name: "",
    postgres_database_id: "",
  };

  function handleFormSubmit(event: CustomEvent) {
    console.log("Form submitted:", event.detail);
    closeDialog();
  }

  export let hasDatabases = databases.length > 0;

  const dispatch = createEventDispatcher();

  function pushEvent(event, payload = {}, callback = () => {}) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleDelete(sequenceId) {
    if (confirm("Are you sure you want to delete this sequence?")) {
      pushEvent("delete_sequence", { id: sequenceId }, (reply) => {
        if (reply.ok) {
          console.log("Sequence deleted successfully");
          sequences = sequences.filter((seq) => seq.id !== sequenceId);
        } else {
          console.error("Failed to delete sequence. Please try again.");
          alert("Failed to delete sequence. Please try again.");
        }
      });
    }
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
          can create a sequence.
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
            >
              <Database class="inline-block h-4 w-4 mr-2" />
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
      <TableIcon class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Sequences</h1>
    </div>
    {#if sequences.length > 0 && hasDatabases}
      <a
        href="/sequences/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create Sequence</Button>
      </a>
    {/if}
  </div>

  {#if sequences.length === 0}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No sequences found</h2>
        <p class="text-gray-600 mb-6">
          Sequences allow you to stream data from specific tables in your
          PostgreSQL database.
        </p>
        {#if hasDatabases}
          <a
            href="/sequences/new"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button>Create your first sequence</Button>
          </a>
        {:else}
          <Button disabled>Create your first sequence</Button>
          <p class="text-gray-600 mt-4">
            You need to connect a database to Sequin before you can create a
            sequence.
          </p>
        {/if}
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Table</Table.Head>
          <Table.Head>Sort Column</Table.Head>
          <Table.Head>Consumer Count</Table.Head>
          <Table.Head>Database</Table.Head>
          <Table.Head>Created at</Table.Head>
          <Table.Head class="text-right">Actions</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each sequences as sequence}
          <Table.Row>
            <Table.Cell
              >{sequence.table_schema}.{sequence.table_name}</Table.Cell
            >
            <Table.Cell><code>{sequence.sort_column_name}</code></Table.Cell>
            <Table.Cell>
              {sequence.consumer_count}
            </Table.Cell>
            <Table.Cell>
              <a
                href="/databases/{sequence.postgres_database.id}"
                data-phx-link="redirect"
                data-phx-link-state="push"
              >
                <Button variant="outline" size="sm" class="w-full">
                  {sequence.postgres_database.name}
                  <ArrowUpRight class="h-4 w-4 ml-2" />
                </Button>
              </a>
            </Table.Cell>
            <Table.Cell>
              {formatRelativeTimestamp(sequence.inserted_at)}
            </Table.Cell>
            <Table.Cell class="text-right">
              <Popover>
                <PopoverTrigger>
                  <Button
                    variant="destructive"
                    size="sm"
                    on:click={() => handleDelete(sequence.id)}
                    disabled={sequence.consumer_count > 0}
                  >
                    Delete
                  </Button>
                </PopoverTrigger>
                {#if sequence.consumer_count > 0}
                  <PopoverContent class="w-60">
                    <p class="text-sm text-muted-foreground">
                      This sequence cannot be deleted because it has active
                      consumers.
                    </p>
                  </PopoverContent>
                {/if}
              </Popover>
            </Table.Cell>
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}

  {#if dialogOpen}
    <NewSequenceForm
      {databases}
      on:submit={handleFormSubmit}
      {live_action}
      {live}
      {parent}
    />
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
