<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import {
    ArrowLeft,
    Database,
    ExternalLink,
    Clock,
    RefreshCw,
    Loader2,
    MoreHorizontal,
    Logs,
  } from "lucide-svelte";
  import { formatRelativeTimestamp, getColorFromName } from "$lib/utils";
  import * as Dialog from "$lib/components/ui/dialog";
  import HealthComponent from "$lib/health/HealthComponent.svelte";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import * as Popover from "$lib/components/ui/popover";

  export let walPipeline;
  export let live;
  export let metrics;

  let showDeleteConfirmDialog = false;
  let deleteConfirmDialogLoading = false;
  let deleteErrorMessage: string | null = null;

  function handleDelete() {
    showDeleteConfirmDialog = true;
  }

  function confirmDelete() {
    deleteConfirmDialogLoading = true;
    deleteErrorMessage = null;
    live.pushEvent("delete_wal_pipeline", {}, (res: any) => {
      deleteConfirmDialogLoading = false;
      if (res.error) {
        deleteErrorMessage = res.error;
      } else {
        showDeleteConfirmDialog = false;
      }
    });
  }

  function cancelDelete() {
    showDeleteConfirmDialog = false;
    deleteErrorMessage = null;
  }
</script>

<div class="min-h-screen font-sans">
  <div class="bg-white border-b sticky top-0 z-10">
    <div class="container mx-auto px-4 py-4">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
          <a href="/wal-pipelines">
            <Button variant="ghost" size="sm">
              <ArrowLeft class="h-4 w-4" />
            </Button>
          </a>
          <div class="flex items-center">
            <Logs class="h-6 w-6 mr-2" />
            <h1 class="text-xl font-semibold">{walPipeline.name}</h1>
          </div>
        </div>
        <div class="flex items-center space-x-4">
          <div
            class="hidden lg:flex flex-col items-left gap-1 text-xs text-gray-500"
          >
            <div class="flex items-center gap-2">
              <Clock class="h-4 w-4" />
              <span
                >Created {formatRelativeTimestamp(
                  walPipeline.inserted_at
                )}</span
              >
            </div>
            <div class="flex items-center gap-2">
              <RefreshCw class="h-4 w-4" />
              <span
                >Updated {formatRelativeTimestamp(walPipeline.updated_at)}</span
              >
            </div>
          </div>
          <a
            href="/wal-pipelines/{walPipeline.id}/edit"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button variant="outline" size="sm">Edit</Button>
          </a>
          <Button
            variant="outline"
            size="sm"
            class="text-red-600 hover:text-red-700"
            on:click={handleDelete}
          >
            Delete
          </Button>
        </div>
      </div>
    </div>
  </div>

  <main class="container mx-auto px-4 py-8">
    <div class="grid gap-6 md:grid-cols-2 mb-6">
      <HealthComponent health={walPipeline.health} />

      <Card>
        <CardContent class="p-6">
          <Popover.Root>
            <div class="flex justify-between items-center mb-4">
              <h2 class="text-lg font-semibold">Metrics</h2>
              <Popover.Trigger asChild let:builder>
                <Button builders={[builder]} variant="ghost" size="icon">
                  <MoreHorizontal class="h-4 w-4" />
                </Button>
              </Popover.Trigger>
              <Popover.Content class="w-56">
                <div class="space-y-2">
                  <div>
                    <span class="text-sm text-gray-500">Pending events</span>
                    <div class="text-lg font-bold">{metrics.count}</div>
                  </div>
                  <div>
                    <span class="text-sm text-gray-500">Earliest pending</span>
                    <div class="text-sm">{metrics.min ?? "-"}</div>
                  </div>
                  <div>
                    <span class="text-sm text-gray-500"
                      >Most recent pending</span
                    >
                    <div class="text-sm">{metrics.max ?? "-"}</div>
                  </div>
                </div>
              </Popover.Content>
            </div>
          </Popover.Root>
          <div class="space-y-2">
            <div>
              <span class="text-sm text-gray-500">Pending events</span>
              <div
                class="text-lg font-bold"
                class:text-green-500={walPipeline.health.status === "healthy"}
              >
                {metrics.count}
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>

    <Card class="mb-6">
      <CardContent class="p-6">
        <h2 class="text-lg font-semibold mb-4">Details</h2>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <p class="text-sm text-gray-500">Source Database</p>
            <p class="font-medium">{walPipeline.source_database.name}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Source Table</p>
            <p class="font-medium">{walPipeline.source_table}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Destination Database</p>
            <p class="font-medium">{walPipeline.destination_database.name}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Destination Table</p>
            <p class="font-medium">{walPipeline.destination_table}</p>
          </div>
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardContent class="p-6">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-lg font-semibold">Source table</h2>
          <a
            href="/databases/{walPipeline.source_database.id}"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button variant="outline" size="sm">
              <ExternalLink class="h-4 w-4 mr-2" />
              View Database
            </Button>
          </a>
        </div>
        <div class="mb-4 flex items-center space-x-2">
          <Database class="h-5 w-5 text-gray-400" />
          <pre class="font-medium">{walPipeline.source_database.name}</pre>
        </div>
        <div class="mb-4 flex items-center space-x-2">
          <icon
            class="hero-table-cells w-6 h-6 rounded {getColorFromName(
              walPipeline.source_table
            )}"
          ></icon>
          <pre class="font-medium">{walPipeline.source_table}</pre>
        </div>
        <div class="mb-4">
          <h3 class="text-md font-semibold mb-2">Filters</h3>
          {#if walPipeline.source_filters.length > 0}
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Column</TableHead>
                  <TableHead>Operator</TableHead>
                  <TableHead>Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {#each walPipeline.source_filters as filter}
                  <TableRow>
                    <TableCell><code>{filter.column}</code></TableCell>
                    <TableCell><code>{filter.operator}</code></TableCell>
                    <TableCell><code>{filter.value}</code></TableCell>
                  </TableRow>
                {/each}
              </TableBody>
            </Table>
          {:else}
            <div
              class="bg-gray-50 border border-gray-200 rounded-lg p-6 text-center"
            >
              <h4 class="text-sm font-medium text-gray-900 mb-1">
                No filters applied
              </h4>
              <p class="text-sm text-gray-500 mb-4">
                This WAL Pipeline will process all data from the source table.
              </p>
            </div>
          {/if}
        </div>
      </CardContent>
    </Card>
  </main>
</div>

<Dialog.Root bind:open={showDeleteConfirmDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title
        >Are you sure you want to delete this WAL Pipeline?</Dialog.Title
      >
      <Dialog.Description>This action cannot be undone.</Dialog.Description>
    </Dialog.Header>
    {#if deleteErrorMessage}
      <p class="text-destructive text-sm mt-2 mb-4">{deleteErrorMessage}</p>
    {/if}
    <Dialog.Footer>
      <Button variant="outline" on:click={cancelDelete}>Cancel</Button>
      <Button
        variant="destructive"
        on:click={confirmDelete}
        disabled={deleteConfirmDialogLoading}
      >
        {#if deleteConfirmDialogLoading}
          <Loader2 class="mr-2 h-4 w-4 animate-spin" />
          Deleting...
        {:else}
          Delete
        {/if}
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
