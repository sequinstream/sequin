<script lang="ts">
  import * as Table from "$lib/components/ui/table";
  import { Badge } from "$lib/components/ui/badge";
  import { Button } from "$lib/components/ui/button";
  import {
    ExternalLink,
    Code,
    FilePlus,
    FolderOpen,
    Pencil,
    Trash2,
  } from "lucide-svelte";
  import { formatRelativeTimestamp } from "$lib/utils";
  import Beta from "$lib/components/Beta.svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import DeleteFunctionDialog from "$lib/components/DeleteFunctionDialog.svelte";

  export let transforms: Array<{
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
    function: "/reference/transforms",
    path: "/reference/transforms",
    routing: "/reference/routing",
  };

  let getHref = (transform) => docBase + typeToDocPath[transform.type];
  let deleteDialogOpen = false;
  let selectedTransform: (typeof transforms)[0] | null = null;

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
    <div class="flex items-center space-x-2">
      <Code class="h-5 w-5 text-muted" />
      <h1 class="text-xl font-semibold">Functions</h1>
      <Beta size="sm" variant="subtle" />
    </div>
    <a
      href="/functions/new"
      data-phx-link="redirect"
      data-phx-link-state="push"
    >
      <Button size="sm" class="gap-2">
        <FilePlus class="h-4 w-4" />
        Create Function
      </Button>
    </a>
  </div>

  {#if transforms.length === 0}
    <div class="w-full rounded-lg border-2 border-dashed border-gray-300">
      <div class="text-center py-12 w-1/2 mx-auto">
        <FolderOpen class="h-10 w-10 mx-auto text-gray-400 mb-4" />
        <h2 class="text-lg font-semibold mb-2">No transforms found</h2>
        <p class="text-gray-600 mb-6 text-sm">
          Functions allow you to modify and restructure your data as it flows
          through your pipelines.
        </p>
        <a
          href="/functions/new"
          data-phx-link="redirect"
          data-phx-link-state="push"
        >
          <Button size="sm">
            <FilePlus class="h-4 w-4 mr-2" />
            Create your first function
          </Button>
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
          <Table.Head>Edit</Table.Head>
          <Table.Head>Delete</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#each transforms as transform}
          <Table.Row class="hover:bg-gray-50 transition">
            <Table.Cell class="py-4 font-medium">{transform.name}</Table.Cell>

            <Table.Cell class="py-4">
              <a
                href={getHref(transform)}
                on:click|stopPropagation
                target="_blank"
              >
                <Badge
                  variant="secondary"
                  class="text-xs capitalize inline-flex items-center gap-1"
                >
                  {transform.type}
                  <ExternalLink class="h-3 w-3" />
                </Badge>
              </a>
            </Table.Cell>

            <Table.Cell class="py-4">
              <Tooltip.Root>
                <Tooltip.Trigger>
                  <div class="flex gap-2">
                    <span>{transform.consumerCount}</span>
                  </div>
                </Tooltip.Trigger>
                <Tooltip.Content>
                  {#if transform.consumers && transform.consumers.length > 0}
                    <div class="max-w-xs">
                      <p class="font-medium mb-1">Used by:</p>
                      <ul class="text-sm space-y-1">
                        {#each transform.consumers as consumer}
                          <li class="text-gray-600">{consumer.name}</li>
                        {/each}
                      </ul>
                    </div>
                  {:else}
                    <p class="text-sm text-gray-600">
                      Not used by any consumer
                    </p>
                  {/if}
                </Tooltip.Content>
              </Tooltip.Root>
            </Table.Cell>

            <Table.Cell class="py-4" title={transform.insertedAt}>
              {formatRelativeTimestamp(transform.insertedAt)}
            </Table.Cell>

            <Table.Cell class="py-4" title={transform.updatedAt}>
              {formatRelativeTimestamp(transform.updatedAt)}
            </Table.Cell>

            <Table.Cell class="py-4">
              <Button
                variant="ghost"
                size="sm"
                class="h-8 w-8 p-0"
                on:click={() => {
                  const url = `/functions/${transform.id}`;
                  window.history.pushState({}, "", url);
                  dispatchEvent(new PopStateEvent("popstate"));
                }}
              >
                <Pencil class="h-4 w-4" />
              </Button>
            </Table.Cell>

            <Table.Cell class="py-4">
              <Button
                variant="ghost"
                size="sm"
                class="h-8 w-8 p-0 text-destructive hover:text-destructive"
                on:click={() => {
                  if (transform.consumers.length > 0) {
                    selectedTransform = transform;
                    deleteDialogOpen = true;
                  } else {
                    pushEvent("delete", { id: transform.id });
                  }
                }}
              >
                <Trash2 class="h-4 w-4" />
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
