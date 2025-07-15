<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Badge } from "$lib/components/ui/badge";
  import { Button } from "$lib/components/ui/button";
  import { ExternalLink, Code, FilePlus, Pencil, Trash2 } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";
  import Beta from "$lib/components/Beta.svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import DeleteFunctionDialog from "$lib/components/DeleteFunctionDialog.svelte";
  import LinkPushNavigate from "$lib/components/LinkPushNavigate.svelte";

  export let functions: Array<{
    id: string;
    name: string;
    type: string;
    snippet: string;
    insertedAt: string;
    updatedAt: string;
    consumerCount: number;
    consumers: Array<{ name: string }>;
  }>;
  export let live;
  export let parent: string;

  let docBase = "https://sequinstream.com/docs";
  let typeToDocPath = {
    transform: "/reference/transforms",
    path: "/reference/transforms",
    routing: "/reference/routing",
    filter: "/reference/filters#filter-functions",
    sql_enrichment: "/reference/sql-enrichment",
  };

  let getHref = (transform) => docBase + typeToDocPath[transform.type];
  let deleteDialogOpen = false;
  let selectedTransform: (typeof functions)[0] | null = null;

  // Optionally override the function type display name
  let function_type_display_names = {
    sql_enrichment: "Enrichment",
  };

  function pushEvent(
    event: string,
    payload = {},
    callback = (event: any) => {},
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }
</script>

<div class="container mx-auto py-8">
  <div class="flex justify-between items-center mb-4">
    <div class="flex items-center">
      <Code class="h-6 w-6 mr-2" />
      <h1 class="text-2xl font-bold">Functions</h1>
    </div>
    {#if functions.length > 0}
      <a
        href="/functions/new"
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        <Button>Create Function</Button>
      </a>
    {/if}
  </div>

  {#if functions.length === 0}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto my-auto">
        <h2 class="text-xl font-semibold mb-4">No functions</h2>
        <p class="text-gray-600 mb-6">
          Functions allow you to modify and restructure your data as it flows
          through your pipelines.
        </p>
        <a
          href="/functions/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button>Create your first function</Button>
        </a>
      </div>
    </div>
  {:else}
    <Table.Root class="text-sm">
      <Table.Header>
        <Table.Row class="text-xs text-gray-500 uppercase tracking-wide">
          <Table.Head>Name</Table.Head>
          <Table.Head>Type</Table.Head>
          <Table.Head>Consumers</Table.Head>
          <Table.Head>Created</Table.Head>
          <Table.Head>Updated</Table.Head>
          <Table.Head></Table.Head>
          <Table.Head></Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each functions as func}
          <Table.Row class="hover:bg-gray-50 transition">
            <Table.Cell class="py-4 font-medium">{func.name}</Table.Cell>

            <Table.Cell class="py-4">
              <a href={getHref(func)} on:click|stopPropagation target="_blank">
                <Badge
                  variant="secondary"
                  class="text-xs capitalize inline-flex items-center gap-1"
                >
                  {function_type_display_names[func.type] || func.type}
                  <ExternalLink class="h-3 w-3" />
                </Badge>
              </a>
            </Table.Cell>

            <Table.Cell class="py-4">
              <Tooltip.Root>
                <Tooltip.Trigger>
                  <div class="flex gap-2">
                    <span>{func.consumerCount}</span>
                  </div>
                </Tooltip.Trigger>
                <Tooltip.Content>
                  {#if func.consumers && func.consumers.length > 0}
                    <div class="max-w-xs">
                      <p class="font-medium mb-1">Used by:</p>
                      <ul class="text-sm space-y-1">
                        {#each func.consumers as consumer}
                          <li class="text-gray-600">{consumer.name}</li>
                        {/each}
                      </ul>
                    </div>
                  {:else}
                    <p class="text-sm text-gray-600">
                      Not used by any consumers
                    </p>
                  {/if}
                </Tooltip.Content>
              </Tooltip.Root>
            </Table.Cell>

            <Table.Cell class="py-4" title={func.insertedAt}>
              {formatRelativeTimestamp(func.insertedAt)}
            </Table.Cell>

            <Table.Cell class="py-4" title={func.updatedAt}>
              {formatRelativeTimestamp(func.updatedAt)}
            </Table.Cell>

            <Table.Cell class="py-4">
              <LinkPushNavigate href={`/functions/${func.id}`}>
                <Button
                  variant="link"
                  size="sm"
                  class="text-blue-600 hover:underline p-0 h-auto"
                >
                  Edit
                </Button>
              </LinkPushNavigate>
            </Table.Cell>

            <Table.Cell class="py-4">
              <Button
                variant="link"
                size="sm"
                class="text-red-600 hover:underline p-0 h-auto"
                on:click={() => {
                  selectedTransform = func;
                  deleteDialogOpen = true;
                }}
              >
                Delete
              </Button>
            </Table.Cell>
          </Table.Row>
        {/each}
      </Table.Body>
    </Table.Root>
  {/if}

  <DeleteFunctionDialog
    bind:open={deleteDialogOpen}
    consumers={selectedTransform?.consumers || []}
    onDelete={() => {
      if (selectedTransform) {
        pushEvent("delete", { id: selectedTransform.id });
        selectedTransform = null;
      }
    }}
  />
</div>
