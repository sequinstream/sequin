<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { Plus } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";

  export let walPipelines: Array<{
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
    <h2 class="text-2xl font-semibold">Change Capture Pipelines</h2>
    <Button on:click={() => pushEvent("new_wal_pipeline", {})}>
      <Plus class="h-4 w-4 mr-2" />
      Create Change Capture Pipeline
    </Button>
  </div>

  {#if walPipelines.length === 0}
    <div class="text-center py-12">
      <p class="text-gray-600 mb-4">No Change Capture Pipelines found</p>
      <Button on:click={() => pushEvent("new_wal_pipeline", {})}>
        Create Change Capture Pipeline
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
        {#each walPipelines as pipeline}
          <Table.Row>
            <Table.Cell>{pipeline.name}</Table.Cell>
            <Table.Cell>
              {#if pipeline.source_table}
                {pipeline.source_table.schema_name}.{pipeline.source_table
                  .table_name}
              {:else}
                Error - N/A
              {/if}
            </Table.Cell>
            <Table.Cell>
              {#if pipeline.destination_table}
                {pipeline.destination_table.schema_name}.{pipeline
                  .destination_table.table_name}
              {:else}
                Error - N/A
              {/if}
            </Table.Cell>
            <Table.Cell
              >{formatRelativeTimestamp(pipeline.inserted_at)}</Table.Cell
            >
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>
