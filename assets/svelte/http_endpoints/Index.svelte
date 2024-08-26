<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";

  export let httpEndpoints: Array<{
    id: string;
    name: string;
    baseUrl: string;
    insertedAt: string;
    httpPushConsumersCount: number;
  }>;
  export let live: any;

  function handleHttpEndpointClick(id: string) {
    live.pushEvent("http_endpoint_clicked", { id });
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <h1 class="text-2xl font-bold">HTTP Endpoints</h1>
    {#if httpEndpoints.length > 0}
      <a
        href="/http-endpoints/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create HTTP Endpoint</Button>
      </a>
    {/if}
  </div>

  {#if httpEndpoints.length === 0}
    <div class="text-center py-12">
      <h2 class="text-xl font-semibold mb-4">No HTTP endpoints found</h2>
      <p class="text-gray-600 mb-6">
        Get started by creating your first HTTP endpoint
      </p>
      <a
        href="/http-endpoints/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create Your First HTTP Endpoint</Button>
      </a>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Base URL</Table.Head>
          <Table.Head>Created at</Table.Head>
          <Table.Head>Consumers</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each httpEndpoints as endpoint}
          <Table.Row
            on:click={() => handleHttpEndpointClick(endpoint.id)}
            class="cursor-pointer"
          >
            <Table.Cell>{endpoint.name}</Table.Cell>
            <Table.Cell>
              <span class="font-mono underline decoration-dotted">
                {endpoint.baseUrl}
              </span>
            </Table.Cell>
            <Table.Cell
              >{formatRelativeTimestamp(endpoint.insertedAt)}</Table.Cell
            >
            <Table.Cell>
              {#if endpoint.httpPushConsumersCount === 0}
                <span class="text-gray-400">No consumers</span>
              {:else}
                {endpoint.httpPushConsumersCount}
              {/if}
            </Table.Cell>
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}
</div>
