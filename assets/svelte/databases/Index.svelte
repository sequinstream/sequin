<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";

  export let databases: Array<{
    id: string;
    name: string;
    insertedAt: string;
    hostname: string;
    port: number;
  }>;
  export let live: any;

  function handleDatabaseClick(id: string) {
    live.pushEvent("database_clicked", { id });
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">Databases</h1>
    <a
      href="/databases/new"
      data-phx-link="redirect"
      data-phx-link-state="push"
    >
      <Button>Create Database</Button>
    </a>
  </div>

  <Table.Root>
    <Table.Header>
      <Table.Row>
        <Table.Head>Name</Table.Head>
        <Table.Head>Hostname</Table.Head>
        <Table.Head>Port</Table.Head>
        <Table.Head>Created At</Table.Head>
      </Table.Row>
    </Table.Header>
    <Table.Body>
      {#each databases as database}
        <Table.Row
          on:click={() => handleDatabaseClick(database.id)}
          class="cursor-pointer"
        >
          <Table.Cell>{database.name}</Table.Cell>
          <Table.Cell>{database.hostname}</Table.Cell>
          <Table.Cell>{database.port}</Table.Cell>
          <Table.Cell
            >{new Date(database.insertedAt).toLocaleString()}</Table.Cell
          >
        </Table.Row>
      {/each}
    </Table.Body>
  </Table.Root>
</div>
