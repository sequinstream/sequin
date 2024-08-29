<script lang="ts">
  import {
    ArrowLeft,
    Activity,
    CheckCircle,
    Clock,
    RefreshCw,
    Database,
    Radio,
    ExternalLink,
    SquareStack,
    ArrowRightToLine,
    ArrowLeftFromLine,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { getColorFromName, formatRelativeTimestamp } from "$lib/utils";
  import HealthDisplay from "$lib/health/HealthComponent.svelte";
  import HealthComponent from "$lib/health/HealthComponent.svelte";
  import type { Health } from "$lib/health/Types";

  interface Table {
    schema_name: string;
    table_name: string;
  }

  interface Consumer {
    id: string;
    name: string;
    message_kind: string;
    consumer_kind: string;
    source_tables: Table[];
  }

  interface PostgresDatabase {
    id: string;
    name: string;
    hostname: string;
    port: number;
    database: string;
    username: string;
    ssl: boolean;
    pool_size: number;
    queue_interval: number;
    queue_target: number;
    tables: Table[];
    tables_refreshed_at: string;
    inserted_at: string;
    updated_at: string;
    consumers: Consumer[];
    health: Health;
  }

  export let database: PostgresDatabase;
  export let live: any;
  export let parent: string;
  export let metrics: {
    avg_latency: number;
  };

  function pushEvent(event: string, params = {}, callback: any = () => {}) {
    live.pushEventTo("#" + parent, event, params, callback);
  }

  let showDeleteConfirmDialog = false;
  let showDeleteErrorDialog = false;
  let deleteErrorDialogMessage: string | null = null;

  function handleEdit() {
    pushEvent("edit");
  }

  function confirmDelete() {
    deleteErrorDialogMessage = null;
    showDeleteConfirmDialog = false;
    pushEvent("delete_database", {}, (res: any) => {
      if (res.error) {
        showDeleteErrorDialog = true;
        deleteErrorDialogMessage = res.error;
      }
    });
  }
</script>

<div class="min-h-screen font-sans">
  <header class="bg-white border-b sticky top-0 z-10">
    <div class="max-w-6xl mx-auto px-4 py-4">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-4">
          <a href="/databases">
            <Button variant="ghost" size="sm">
              <ArrowLeft class="h-4 w-4" />
            </Button>
          </a>
          <div class="flex items-center">
            <Database class="h-6 w-6 mr-2" />
            <h1 class="text-xl font-semibold">{database.name}</h1>
          </div>
        </div>
        <div class="flex items-center space-x-4">
          <div
            class="hidden lg:flex flex-col items-left gap-1 text-xs text-gray-500"
          >
            <div class="flex items-center gap-2">
              <Clock class="h-4 w-4" />
              <span
                >Created {formatRelativeTimestamp(database.inserted_at)}</span
              >
            </div>
            <div class="flex items-center gap-2">
              <RefreshCw class="h-4 w-4" />
              <span>Updated {formatRelativeTimestamp(database.updated_at)}</span
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

  <main class="max-w-6xl mx-auto px-4 py-8">
    <div class="grid gap-6 md:grid-cols-3 mb-6">
      <HealthComponent health={database.health} />

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Tables</span>
            <CheckCircle class="h-5 w-5 text-green-500" />
          </div>
          <div class="text-4xl font-bold">{database.tables.length}</div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Avg. Latency</span>
            <Clock class="h-5 w-5 text-blue-500" />
          </div>
          <div class="text-4xl font-bold">{metrics.avg_latency} ms</div>
        </CardContent>
      </Card>
    </div>

    <Card class="mb-6">
      <CardContent class="p-6">
        <h2 class="text-lg font-semibold mb-4">Configuration</h2>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <p class="text-sm text-gray-500">Hostname</p>
            <p class="font-medium text-wrap break-all">{database.hostname}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Port</p>
            <p class="font-medium">{database.port}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Database</p>
            <p class="font-medium">{database.database}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Username</p>
            <p class="font-medium">{database.username}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">SSL</p>
            <p class="font-medium">{database.ssl ? "Enabled" : "Disabled"}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Pool Size</p>
            <p class="font-medium">{database.pool_size}</p>
          </div>
          <!-- <div>
            <p class="text-sm text-gray-500">Queue Interval</p>
            <p class="font-medium">{database.queue_interval} ms</p>
          </div> -->
        </div>
      </CardContent>
    </Card>

    <Card class="mb-6">
      <CardContent class="p-6">
        <h2 class="text-lg font-semibold mb-4">Consumers</h2>
        {#if database.consumers && database.consumers.length > 0}
          <div class="flex flex-wrap gap-4">
            {#each database.consumers as consumer}
              <Card class="w-full">
                <CardContent class="p-4">
                  <div class="flex items-center justify-between mb-6">
                    <div class="flex items-center">
                      <Radio class="h-4 w-4 mr-2" />
                      <span class="font-medium">{consumer.name}</span>
                    </div>
                    <Button
                      variant="outline"
                      size="sm"
                      href={`/consumers/${consumer.id}`}
                    >
                      <ExternalLink class="h-4 w-4 mr-2" />
                      View Consumer
                    </Button>
                  </div>
                  <div
                    class="grid grid-cols-[auto_1fr] gap-x-2 gap-y-3 mb-2 auto-rows-fr"
                  >
                    <p
                      class="text-sm text-gray-500 justify-self-end self-center"
                    >
                      Tables:
                    </p>
                    <div>
                      {#each consumer.source_tables as table}
                        <div
                          class="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-white border border-black text-black"
                        >
                          <icon
                            class="hero-table-cells w-4 h-4 mr-1 rounded {getColorFromName(
                              `${table.schema_name}.${table.table_name}`
                            )}"
                          ></icon>

                          <pre
                            class="font-medium">{table.schema_name}.{table.table_name}</pre>
                        </div>
                      {/each}
                    </div>
                    <p
                      class="text-sm text-gray-500 justify-self-end self-center"
                    >
                      Stream type:
                    </p>
                    <div
                      class="w-fit inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-white border border-black text-black"
                    >
                      <SquareStack class="h-4 w-4 mr-1 text-black" />
                      <p class="font-medium">
                        {#if consumer.message_kind === "event"}
                          Change stream
                        {:else if consumer.message_kind === "record"}
                          Sync stream
                        {/if}
                      </p>
                    </div>
                    <p
                      class="text-sm text-gray-500 justify-self-end self-center"
                    >
                      Consumer type:
                    </p>
                    <div class="flex items-center">
                      <span
                        class="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-white border border-black text-black"
                      >
                        {#if consumer.consumer_kind === "http_push"}
                          <ArrowRightToLine class="h-4 w-4 mr-1 text-black" />
                          Push consumer
                        {:else if consumer.consumer_kind === "http_pull"}
                          <ArrowLeftFromLine class="h-4 w-4 mr-1 text-black" />
                          Pull consumer
                        {/if}
                      </span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            {/each}
          </div>
        {:else}
          <div
            class="bg-gray-100 p-6 rounded-lg flex flex-col items-center justify-center"
          >
            <Radio class="h-12 w-12 mb-4 text-gray-400" />
            <p class="text-sm font-medium text-gray-900 mb-3">
              No consumers attached to database
            </p>
            <Button href="/consumers/new" variant="outline"
              >Add a consumer</Button
            >
          </div>
        {/if}
      </CardContent>
    </Card>
  </main>
</div>

<AlertDialog.Root bind:open={showDeleteConfirmDialog}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title
        >Are you sure you want to delete this database?</AlertDialog.Title
      >
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
      <AlertDialog.Title>Error deleting the database</AlertDialog.Title>
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
