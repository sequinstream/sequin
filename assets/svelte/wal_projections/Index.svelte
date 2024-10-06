<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import { Logs } from "lucide-svelte";
  import HealthPill from "../health/HealthPill.svelte";

  export let walProjections: Array<{
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
      <h1 class="text-2xl font-bold">WAL Projections</h1>
    </div>
    {#if walProjections.length > 0}
      <a
        href="/wal-projections/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>New WAL Projection</Button>
      </a>
    {/if}
  </div>

  {#if walProjections.length === 0}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No WAL Projections found</h2>
        <p class="text-gray-600 mb-6">
          WAL Projections allow you to replicate data from one table to another
          using the WAL.
        </p>
        <a
          href="/wal-projections/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button>Create your first WAL Projection</Button>
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
        {#each walProjections as projection}
          <Table.Row
            class="cursor-pointer"
            on:click={() => {
              const url = `/wal-projections/${projection.id}`;
              window.history.pushState({}, "", url);
              dispatchEvent(new PopStateEvent("popstate"));
            }}
          >
            <Table.Cell>{projection.name}</Table.Cell>
            <Table.Cell>
              <HealthPill status={projection.health.status} />
            </Table.Cell>
            <Table.Cell>
              {projection.source_table.schema_name}.{projection.source_table
                .table_name}
            </Table.Cell>
            <Table.Cell>
              {projection.destination_table.schema_name}.{projection
                .destination_table.table_name}
            </Table.Cell>
            <Table.Cell>
              {formatRelativeTimestamp(projection.inserted_at)}
            </Table.Cell>
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>
