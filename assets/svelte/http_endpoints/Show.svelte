<script lang="ts">
  import {
    ArrowLeft,
    Globe,
    Braces,
    Clock,
    RotateCw,
    Activity,
    Eye,
    EyeOff,
    ExternalLink,
    Loader2,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Dialog from "$lib/components/ui/dialog";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import { formatRelativeTimestamp } from "$lib/utils";
  import HealthSummary from "../health/HealthSummary.svelte";
  import LinkPushNavigate from "$lib/components/LinkPushNavigate.svelte";
  export let http_endpoint;
  export let metrics;
  export let live;
  export let parent_id;
  export let sink_consumer_count: number;

  let showDeleteConfirmDialog = false;
  let deleteConfirmDialogLoading = false;
  let deleteErrorMessage: string | null = null;
  let showEncryptedValues = {};

  function pushEvent(event: string, data: any, cb?: (val: any) => void) {
    live.pushEventTo(`#${parent_id}`, event, data, cb);
  }

  function handleEdit() {
    pushEvent("edit", {});
  }

  function confirmDelete() {
    deleteConfirmDialogLoading = true;
    deleteErrorMessage = null;
    live.pushEventTo(`#${parent_id}`, "delete", {}, (res: any) => {
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

  function toggleEncryptedValue(key) {
    showEncryptedValues[key] = !showEncryptedValues[key];
    showEncryptedValues = showEncryptedValues;
  }

  function isWebhookSiteUrl(url: string): boolean {
    return url.startsWith("https://webhook.site/");
  }

  function getWebhookSiteViewUrl(url: string): string {
    const uuid = url.split("/").pop();
    return `https://webhook.site/#!/view/${uuid}`;
  }
</script>

<div class="min-h-screen font-sans bg-white">
  {#if sink_consumer_count === 0}
    <div class="container mx-auto px-4 pt-4">
      <div class="p-4 bg-blue-50 border border-blue-200 rounded-lg">
        <div class="flex items-center justify-between">
          <div>
            <h3 class="font-semibold">Create a Webhook sink</h3>
            <p>
              Create a webhook sink to push records or changes to your HTTP
              Endpoint
            </p>
          </div>
          <a
            href="/sinks/new?kind=http_push"
            data-phx-link="redirect"
            data-phx-link-state="push"
          >
            <Button variant="outline">Create Webhook Sink</Button>
          </a>
        </div>
      </div>
    </div>
  {/if}

  <header class="bg-white border-b sticky top-0 z-10">
    <div class="container mx-auto px-4 py-4">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
          <LinkPushNavigate href="/http-endpoints">
            <Button variant="ghost" size="sm">
              <ArrowLeft class="h-4 w-4" />
            </Button>
          </LinkPushNavigate>
          <div class="flex items-center">
            <Globe class="h-6 w-6 mr-2" />
            <h1 class="text-xl font-semibold">{http_endpoint.name}</h1>
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
                  http_endpoint.inserted_at,
                )}</span
              >
            </div>
            <div class="flex items-center gap-2">
              <RotateCw class="h-4 w-4" />
              <span
                >Updated {formatRelativeTimestamp(
                  http_endpoint.updated_at,
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
    <div class="grid gap-6 md:grid-cols-2 mb-8">
      <HealthSummary health={http_endpoint.health} {pushEvent} />
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Throughput</span>
            <Activity class="h-5 w-5 text-blue-500" />
          </div>
          <span class="text-2xl font-bold">{metrics.throughput} req/min</span>
        </CardContent>
      </Card>
    </div>

    <div class="space-y-6">
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between">
            <h2 class="text-lg font-semibold mb-4">Base URL</h2>
            {#if isWebhookSiteUrl(http_endpoint.baseUrl)}
              <a
                href={getWebhookSiteViewUrl(http_endpoint.baseUrl)}
                target="_blank"
                rel="noopener noreferrer"
              >
                <Button variant="outline" size="sm">
                  <ExternalLink class="h-4 w-4 mr-2" />
                  View on Webhook.site
                </Button>
              </a>
            {/if}
          </div>
          <div class="flex items-center space-x-2 overflow-x-auto">
            <p
              class="w-fit font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >
              {http_endpoint.baseUrl}
            </p>
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
              <Braces class="h-12 w-12 text-gray-300 mx-auto mb-3" />
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

      <Card>
        <CardContent class="p-6">
          <h2 class="text-lg font-semibold mb-4">Encrypted Headers</h2>
          {#if Object.keys(http_endpoint.encryptedHeaders).length > 0}
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Key</TableHead>
                  <TableHead>Value</TableHead>
                  <TableHead class="w-[100px]">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {#each Object.entries(http_endpoint.encryptedHeaders) as [key, value]}
                  <TableRow>
                    <TableCell class="font-medium">{key}</TableCell>
                    <TableCell>
                      {#if showEncryptedValues[key]}
                        {value}
                      {:else}
                        ••••••••
                      {/if}
                    </TableCell>
                    <TableCell>
                      <Button
                        variant="ghost"
                        size="sm"
                        on:click={() => toggleEncryptedValue(key)}
                      >
                        {#if showEncryptedValues[key]}
                          <EyeOff class="h-4 w-4" />
                        {:else}
                          <Eye class="h-4 w-4" />
                        {/if}
                      </Button>
                    </TableCell>
                  </TableRow>
                {/each}
              </TableBody>
            </Table>
          {:else}
            <div class="text-center py-6 bg-gray-50 rounded-lg">
              <Braces class="h-12 w-12 text-gray-300 mx-auto mb-3" />
              <h3 class="text-sm font-medium text-gray-900 mb-1">
                No encrypted headers configured
              </h3>
              <p class="text-sm text-gray-500">
                This HTTP endpoint doesn't have any encrypted headers set.
              </p>
            </div>
          {/if}
        </CardContent>
      </Card>
    </div>
  </div>
</div>

<Dialog.Root bind:open={showDeleteConfirmDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title
        >Are you sure you want to delete this HTTP endpoint?</Dialog.Title
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
