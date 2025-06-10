<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import { Logs } from "lucide-svelte";
  import HealthPill from "../health/HealthPill.svelte";
  import DatabaseConnectionAlert from "../components/DatabaseConnectionAlert.svelte";

  export let walPipelines: Array<{
    id: string;
    name: string;
    source_table: {
      schema_name: string;
      table_name: string;
    };
    destination_table: {
      schema_name: string;
      table_name: string;
    };
    inserted_at: string;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
  }>;

  export let hasDatabases: boolean;
  export let selfHosted: boolean;
</script>

<div class="container mx-auto py-10">
  <DatabaseConnectionAlert
    show={!hasDatabases}
    entityName="Change Retention"
    showTryWithLocalDatabaseCTA={!selfHosted}
  />

  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Logs class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Change Retention</h1>
    </div>
    {#if walPipelines.length > 0 && hasDatabases}
      <a
        href="/change-capture-pipelines/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Setup Change Retention</Button>
      </a>
    {/if}
  </div>

  {#if walPipelines.length === 0}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No Change Retention setup</h2>
        <p class="text-gray-600 mb-6">
          With
          <a
            href="https://sequinstream.com/docs/reference/change-retention"
            target="_blank"
            class="text-blue-500 hover:underline">Change Retention</a
          >, Sequin captures all of a table's insert, update, and delete events
          and stores them in a change table.
        </p>
        {#if hasDatabases}
          <a
            href="/change-capture-pipelines/new"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button>Setup Change Retention</Button>
          </a>
        {:else}
          <Button disabled>Setup Change Retention</Button>
          <p class="text-gray-600 mt-4">
            You need to connect a database to Sequin before you can setup Change
            Retention.
          </p>
        {/if}
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Status</Table.Head>
          <Table.Head>Source</Table.Head>
          <Table.Head>Destination</Table.Head>
          <Table.Head>Created at</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each walPipelines as pipeline}
          <Table.Row
            class="cursor-pointer"
            on:click={() => {
              const url = `/change-capture-pipelines/${pipeline.id}`;
              window.history.pushState({}, "", url);
              dispatchEvent(new PopStateEvent("popstate"));
            }}
          >
            <Table.Cell>{pipeline.name}</Table.Cell>
            <Table.Cell>
              <HealthPill status={pipeline.health.status} />
            </Table.Cell>
            <Table.Cell>
              {#if pipeline.source_table}
                {pipeline.source_table.schema_name}.{pipeline.source_table
                  .table_name}
              {:else}
                <span class="text-gray-500">Table not found</span>
              {/if}
            </Table.Cell>
            <Table.Cell>
              {pipeline.destination_table.schema_name}.{pipeline
                .destination_table.table_name}
            </Table.Cell>
            <Table.Cell>
              {formatRelativeTimestamp(pipeline.inserted_at)}
            </Table.Cell>
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
