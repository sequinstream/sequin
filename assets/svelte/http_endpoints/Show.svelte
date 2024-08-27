<script lang="ts">
  import { ArrowLeft, Globe, Clock, RefreshCw } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import { formatRelativeTimestamp } from "$lib/utils";

  export let http_endpoint;
  export let live;
  export let parent_id;

  let showDeleteConfirmDialog = false;
  let showDeleteErrorDialog = false;
  let deleteErrorDialogMessage: string | null = null;

  function handleEdit() {
    live.pushEventTo(`#${parent_id}`, "edit", {});
  }

  function confirmDelete() {
    deleteErrorDialogMessage = null;
    showDeleteConfirmDialog = false;
    live.pushEventTo(`#${parent_id}`, "delete", {}, (res: any) => {
      if (res.error) {
        showDeleteErrorDialog = true;
        deleteErrorDialogMessage = res.error;
      }
    });
  }
</script>

<div class="min-h-screen font-sans bg-white">
  <header class="bg-white border-b sticky top-0 z-10">
    <div class="container mx-auto px-4 py-4">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
          <a href="/http-endpoints">
            <Button variant="ghost" size="sm">
              <ArrowLeft class="h-4 w-4" />
            </Button>
          </a>
          <h1 class="text-xl font-semibold">{http_endpoint.name}</h1>
        </div>
        <div class="flex items-center space-x-4">
          <div
            class="hidden lg:flex flex-col items-left gap-1 text-xs text-gray-500"
          >
            <div class="flex items-center gap-2">
              <Clock class="h-4 w-4" />
              <span
                >Created {formatRelativeTimestamp(
                  http_endpoint.inserted_at
                )}</span
              >
            </div>
            <div class="flex items-center gap-2">
              <RefreshCw class="h-4 w-4" />
              <span
                >Updated {formatRelativeTimestamp(
                  http_endpoint.updated_at
                )}</span
              >
            </div>
          </div>
          <Button variant="outline" size="sm" on:click={handleEdit}>Edit</Button
          >
          <Button
            variant="outline"
            size="sm"
            class="text-red-600 hover:text-red-700"
            on:click={() => (showDeleteConfirmDialog = true)}
          >
            Delete
          </Button>
        </div>
      </div>
    </div>
  </header>

  <div class="container mx-auto px-4 py-8">
    <div class="space-y-6">
      <Card>
        <CardContent class="p-6">
          <h2 class="text-lg font-semibold mb-4">Configuration</h2>
          <div class="grid md:grid-cols-2 gap-4">
            <div>
              <span class="text-sm text-gray-500">ID</span>
              <p class="font-medium">{http_endpoint.id}</p>
            </div>
            <div>
              <span class="text-sm text-gray-500">Name</span>
              <p class="font-medium">{http_endpoint.name}</p>
            </div>
            <div class="md:col-span-2">
              <span class="text-sm text-gray-500">Base URL</span>
              <p class="font-medium">{http_endpoint.baseUrl}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <h2 class="text-lg font-semibold mb-4">Headers</h2>
          {#if Object.keys(http_endpoint.headers).length > 0}
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Key</TableHead>
                  <TableHead>Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {#each Object.entries(http_endpoint.headers) as [key, value]}
                  <TableRow>
                    <TableCell class="font-medium">{key}</TableCell>
                    <TableCell>{value}</TableCell>
                  </TableRow>
                {/each}
              </TableBody>
            </Table>
          {:else}
            <div class="text-center py-6 bg-gray-50 rounded-lg">
              <Globe class="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <h3 class="text-sm font-medium text-gray-900 mb-1">
                No headers configured
              </h3>
              <p class="text-sm text-gray-500">
                This HTTP endpoint doesn't have any custom headers set.
              </p>
            </div>
          {/if}
        </CardContent>
      </Card>
    </div>
  </div>
</div>

<AlertDialog.Root bind:open={showDeleteConfirmDialog}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>
        Are you sure you want to delete this HTTP endpoint?
      </AlertDialog.Title>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel on:click={() => (showDeleteConfirmDialog = false)}
        >Cancel</AlertDialog.Cancel
      >
      <AlertDialog.Action on:click={confirmDelete}>Delete</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>

<AlertDialog.Root bind:open={showDeleteErrorDialog}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>Error deleting the HTTP endpoint</AlertDialog.Title>
      <AlertDialog.Description
        >{deleteErrorDialogMessage}</AlertDialog.Description
      >
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel
        on:click={() => {
          showDeleteErrorDialog = false;
          deleteErrorDialogMessage = null;
        }}>Close</AlertDialog.Cancel
      >
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
