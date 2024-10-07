<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import { Logs } from "lucide-svelte";
  import HealthPill from "../health/HealthPill.svelte";

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
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Logs class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">WAL Pipelines</h1>
    </div>
    {#if walPipelines.length > 0}
      <a
        href="/wal-pipelines/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create WAL Pipeline</Button>
      </a>
    {/if}
  </div>

  {#if walPipelines.length === 0}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No WAL Pipelines found</h2>
        <p class="text-gray-600 mb-6">
          WAL Pipelines allow you to replicate data from one table to another
          using the WAL.
        </p>
        <a
          href="/wal-pipelines/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button>Create your first WAL Pipeline</Button>
        </a>
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Health</Table.Head>
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
              const url = `/wal-pipelines/${pipeline.id}`;
              window.history.pushState({}, "", url);
              dispatchEvent(new PopStateEvent("popstate"));
            }}
          >
            <Table.Cell>{pipeline.name}</Table.Cell>
            <Table.Cell>
              <HealthPill status={pipeline.health.status} />
            </Table.Cell>
            <Table.Cell>
              {pipeline.source_table.schema_name}.{pipeline.source_table
                .table_name}
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
