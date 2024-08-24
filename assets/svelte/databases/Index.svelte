<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";

  export let databases: Array<{
    id: string;
    name: string;
    insertedAt: string;
    hostname: string;
    port: number;
  }>;
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Databases</h1>
    {#if databases.length > 0}
      <a
        href="/databases/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create Database</Button>
      </a>
    {/if}
  </div>

  {#if databases.length === 0}
    <div class="text-center py-12">
      <h2 class="text-xl font-semibold mb-4">No databases found</h2>
      <p class="text-gray-600 mb-6">
        Get started by creating your first database
      </p>
      <a
        href="/databases/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create Your First Database</Button>
      </a>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Hostname</Table.Head>
          <Table.Head>Port</Table.Head>
          <Table.Head>Created at</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each databases as database}
          <a
            href={`/databases/${database.id}`}
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Table.Row class="cursor-pointer">
              <Table.Cell>{database.name}</Table.Cell>
              <Table.Cell>{database.hostname}</Table.Cell>
              <Table.Cell>{database.port}</Table.Cell>
              <Table.Cell
                >{formatRelativeTimestamp(database.insertedAt)}</Table.Cell
              >
            </Table.Row>
          </a>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>
