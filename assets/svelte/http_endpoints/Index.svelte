<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import { ChevronsLeftRightEllipsis, Webhook } from "lucide-svelte";
  import HealthPill from "../health/HealthPill.svelte";

  export let httpEndpoints: Array<{
    id: string;
    name: string;
    baseUrl: string;
    insertedAt: string;
    httpPushConsumersCount: number;
    health: {
      status: "healthy" | "warning" | "error" | "initializing";
    };
  }>;
  export let live: any;

  function handleHttpEndpointClick(id: string) {
    live.pushEvent("http_endpoint_clicked", { id });
  }
</script>

<div class="container mx-auto py-10">
  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <ChevronsLeftRightEllipsis class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">HTTP Endpoints</h1>
    </div>
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
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No HTTP endpoints found</h2>
        <p class="text-gray-600 mb-6">
          Sequin can push changes from your database to HTTP endpoints in your
          application or another service.
        </p>
        <a
          href="/http-endpoints/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button>Create HTTP Endpoint</Button>
        </a>
      </div>
    </div>
  {:else}
    <Table.Root>
      <Table.Header>
        <Table.Row>
          <Table.Head>Name</Table.Head>
          <Table.Head>Health</Table.Head>
          <Table.Head>Base URL</Table.Head>
          <Table.Head>Created at</Table.Head>
          <Table.Head>
            <div class="flex items-center">
              <Webhook class="h-4 w-4 mr-2" />
              <span>Webhooks</span>
            </div>
          </Table.Head>
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
              <HealthPill status={endpoint.health.status} />
            </Table.Cell>
            <Table.Cell>
              {endpoint.baseUrl}
            </Table.Cell>
            <Table.Cell
              >{formatRelativeTimestamp(endpoint.insertedAt)}</Table.Cell
            >
            <Table.Cell>
              {#if endpoint.httpPushConsumersCount === 0}
                <span class="text-gray-400">No webhook sinks</span>
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
