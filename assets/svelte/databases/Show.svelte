<script lang="ts">
  import {
    ArrowLeft,
    Activity,
    CheckCircle,
    Clock,
    Database,
    ExternalLink,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Badge } from "$lib/components/ui/badge";
  import { formatDistanceToNow } from "date-fns";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";

  interface Table {
    schema: string;
    name: string;
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
  }

  export let database: PostgresDatabase;
  export let live: any;

  let showDeleteConfirmDialog = false;

  function handleEdit() {
    live.pushEventTo("#database-show", "edit", {});
  }

  function handleDelete() {
    showDeleteConfirmDialog = true;
  }

  function confirmDelete() {
    showDeleteConfirmDialog = false;
    live.pushEventTo("#database-show", "delete_database", {});
  }

  function cancelDelete() {
    showDeleteConfirmDialog = false;
  }

  function handleRefreshTables() {
    live.pushEventTo("#database-show", "refresh_tables", {});
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
          <h1 class="text-xl font-semibold">{database.name}</h1>
          <Badge variant="secondary" class="bg-gray-900 text-white">
            active
          </Badge>
        </div>
        <div class="flex items-center space-x-4 text-sm text-gray-500">
          <span
            >Created {formatDistanceToNow(new Date(database.inserted_at), {
              addSuffix: true,
            })}</span
          >
          <span
            >Updated {formatDistanceToNow(new Date(database.updated_at), {
              addSuffix: true,
            })}</span
          >
          <Button variant="outline" size="sm" on:click={handleEdit}>
            Edit
          </Button>
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
  </header>

  <main class="max-w-6xl mx-auto px-4 py-8">
    <div class="grid gap-6 md:grid-cols-3 mb-6">
      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Health</span>
            <Activity class="h-5 w-5 text-green-500" />
          </div>
          <div class="text-4xl font-bold text-green-500">98%</div>
        </CardContent>
      </Card>

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
          <div class="text-4xl font-bold">{database.queue_target} ms</div>
        </CardContent>
      </Card>
    </div>

    <Card class="mb-6">
      <CardContent class="p-6">
        <h2 class="text-lg font-semibold mb-4">Configuration</h2>
        <div class="grid grid-cols-2 gap-4">
          <div>
            <p class="text-sm text-gray-500">ID</p>
            <p class="font-medium">{database.id}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Hostname</p>
            <p class="font-medium">{database.hostname}</p>
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
          <div>
            <p class="text-sm text-gray-500">Queue Interval</p>
            <p class="font-medium">{database.queue_interval} ms</p>
          </div>
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardContent class="p-6">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-lg font-semibold">Source Tables</h2>
          <Button variant="outline" size="sm" on:click={handleRefreshTables}>
            <ExternalLink class="h-4 w-4 mr-2" />
            Refresh Tables
          </Button>
        </div>
        <div class="space-y-2">
          {#each database.tables as table}
            <div class="flex items-center space-x-2">
              <Database class="h-4 w-4 text-gray-400" />
              <span class="font-medium">{table.schema}</span>
              <span class="text-gray-500">{table.name}</span>
            </div>
          {/each}
        </div>
        {#if database.tables.length === 0}
          <div class="text-center py-6 bg-gray-50 rounded-lg">
            <Database class="h-12 w-12 text-gray-300 mx-auto mb-3" />
            <h3 class="text-sm font-medium text-gray-900 mb-1">
              No tables found
            </h3>
            <p class="text-sm text-gray-500">
              This database doesn't have any tables yet, or they haven't been
              refreshed.
            </p>
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
      <AlertDialog.Description>
        This action cannot be undone. All data associated with this database
        will be permanently removed.
      </AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel on:click={cancelDelete}>Cancel</AlertDialog.Cancel>
      <AlertDialog.Action on:click={confirmDelete}>Delete</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
