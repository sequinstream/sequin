<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { Plus } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";

  export let walProjections: Array<{
    id: string;
    name: string;
    source_table: {
      table_name: string;
      schema_name: string;
    } | null;
    destination_table: {
      table_name: string;
      schema_name: string;
    } | null;
    inserted_at: string;
  }>;
  export let live: any;
  export let parent: string;

  function pushEvent(event: string, params = {}) {
    live.pushEventTo("#" + parent, event, params);
  }
</script>

<div class="container mx-auto px-4 py-8">
  <div class="flex justify-between items-center mb-6">
    <h2 class="text-2xl font-semibold">WAL Projections</h2>
    <Button on:click={() => pushEvent("new_wal_projection", {})}>
      <Plus class="h-4 w-4 mr-2" />
      Create WAL Projection
    </Button>
  </div>

  {#if walProjections.length === 0}
    <div class="text-center py-12">
      <p class="text-gray-600 mb-4">No WAL Projections found</p>
      <Button on:click={() => pushEvent("new_wal_projection", {})}>
        Create your first WAL Projection
      </Button>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Source table</Table.Head>
          <Table.Head>Destination table</Table.Head>
          <Table.Head>Created at</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each walProjections as projection}
          <Table.Row>
            <Table.Cell>{projection.name}</Table.Cell>
            <Table.Cell>
              {#if projection.source_table}
                {projection.source_table.schema_name}.{projection.source_table
                  .table_name}
              {:else}
                Error - N/A
              {/if}
            </Table.Cell>
            <Table.Cell>
              {#if projection.destination_table}
                {projection.destination_table.schema_name}.{projection
                  .destination_table.table_name}
              {:else}
                Error - N/A
              {/if}
            </Table.Cell>
            <Table.Cell
              >{formatRelativeTimestamp(projection.inserted_at)}</Table.Cell
            >
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>
