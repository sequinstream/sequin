<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import { Database, Radio } from "lucide-svelte";
  import HealthPill from "../health/HealthPill.svelte";

  export let databases: Array<{
    id: string;
    name: string;
    insertedAt: string;
    hostname: string;
    port: number;
    consumers: number;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
  }>;
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Database class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Databases</h1>
    </div>
    {#if databases.length > 0}
      <a
        href="/databases/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Connect Database</Button>
      </a>
    {/if}
  </div>

  {#if databases.length === 0}
    <div class="text-center py-12">
      <h2 class="text-xl font-semibold mb-4">No databases found</h2>
      <p class="text-gray-600 mb-6">
        Get started by connecting your first database
      </p>
      <a
        href="/databases/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Connect your first database</Button>
      </a>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Health</Table.Head>
          <Table.Head>Hostname</Table.Head>
          <Table.Head>Created at</Table.Head>
          <Table.Head>
            <div class="flex items-center">
              <Radio class="h-4 w-4 mr-2" />
              <span>Consumers</span>
            </div>
          </Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each databases as database}
          <Table.Row
            class="cursor-pointer"
            on:click={() => {
              const url = `/databases/${database.id}`;
              window.history.pushState({}, "", url);
              dispatchEvent(new PopStateEvent("popstate"));
            }}
          >
            <Table.Cell>{database.name}</Table.Cell>
            <Table.Cell>
              <HealthPill status={database.health.status} />
            </Table.Cell>
            <Table.Cell>
              {database.hostname}
            </Table.Cell>
            <Table.Cell>
              {formatRelativeTimestamp(database.insertedAt)}
            </Table.Cell>
            <Table.Cell>
              {#if database.consumers === 0}
                <span class="text-gray-400">No consumers</span>
              {:else}
                {database.consumers}
              {/if}
            </Table.Cell>
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>
