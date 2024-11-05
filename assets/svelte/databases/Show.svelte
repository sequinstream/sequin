<script lang="ts">
  import {
    Clock,
    RefreshCw,
    Radio,
    ExternalLink,
    SquareStack,
    ArrowRightToLine,
    ArrowLeftFromLine,
    Loader2,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { getColorFromName } from "$lib/utils";
  import HealthComponent from "$lib/health/HealthComponent.svelte";
  import type { Health } from "$lib/health/Types";
  import { Badge } from "$lib/components/ui/badge";
  import { writable } from "svelte/store";

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
    href: string;
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
    database_ping_ms: number;
    replication_lag_ms: number;
    replication_active: boolean;
  };

  let refreshingTables = writable(false);

  function pushEvent(event: string, params = {}, callback: any = () => {}) {
    live.pushEventTo("#" + parent, event, params, callback);
  }

  function handleRefreshTables() {
    $refreshingTables = true;
    pushEvent("refresh_tables", {}, (res) => {
      $refreshingTables = false;
    });
  }
</script>

<div class="min-h-screen font-sans">
  <main class="container mx-auto px-4 py-8">
    <!-- Existing overview content -->
    <div class="grid gap-6 lg:grid-cols-3 mb-6">
      <HealthComponent health={database.health} />

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Tables</span>
            <Button
              variant="ghost"
              size="sm"
              on:click={handleRefreshTables}
              disabled={$refreshingTables}
            >
              {#if $refreshingTables}
                <span class="sr-only">Refreshing tables</span>
                <Loader2 class="h-4 w-4 animate-spin" />
              {:else}
                <span class="sr-only">Refresh tables</span>
                <RefreshCw class="h-4 w-4 text-gray-500" />
              {/if}
            </Button>
          </div>
          <div class="text-4xl font-bold">{database.tables.length}</div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="grid grid-cols-2 gap-6">
            <!-- Database Ping -->
            <div class="flex flex-col justify-between">
              <div class="flex justify-between items-center mb-4">
                <span class="text-sm font-medium text-gray-500"
                  >Database Ping</span
                >
              </div>
              {#if metrics.database_ping_ms}
                <div class="text-4xl font-bold">
                  {metrics.database_ping_ms} ms
                </div>
              {:else}
                <div class="flex items-center">
                  <Loader2 class="h-8 w-8 text-gray-400 animate-spin mr-2" />
                  <span class="text-4xl font-bold text-gray-500">ms</span>
                </div>
              {/if}
            </div>

            <!-- Replication Lag -->
            <div class="flex flex-col justify-between">
              <div class="flex justify-between items-center mb-4">
                <span class="text-sm font-medium text-gray-500"
                  >Replication Lag</span
                >
                <Clock
                  class={metrics.replication_lag_ms > 300000
                    ? "h-5 w-5 text-yellow-500"
                    : "h-5 w-5 text-blue-500"}
                />
              </div>
              {#if !metrics.replication_active}
                <div class="text-4xl font-bold text-gray-500">N/A</div>
              {:else if metrics.replication_lag_ms != null}
                <div class="relative group">
                  <div
                    class="text-4xl font-bold {metrics.replication_lag_ms >
                    300000
                      ? 'text-yellow-600'
                      : ''}"
                  >
                    {#if metrics.replication_lag_ms < 1000}
                      {metrics.replication_lag_ms}ms
                    {:else if metrics.replication_lag_ms < 60000}
                      {(metrics.replication_lag_ms / 1000).toFixed(0)}s
                    {:else if metrics.replication_lag_ms < 3600000}
                      {(metrics.replication_lag_ms / 60000).toFixed(0)}m
                    {:else if metrics.replication_lag_ms < 86400000}
                      {(metrics.replication_lag_ms / 3600000).toFixed(0)}h
                    {:else}
                      {(metrics.replication_lag_ms / 86400000).toFixed(0)}d
                    {/if}
                  </div>
                  {#if metrics.replication_lag_ms > 300000}
                    <div
                      class="invisible group-hover:visible absolute z-10 w-64 p-2 mt-2 text-sm text-white bg-gray-800 rounded-lg"
                    >
                      High replication lag detected. This indicates that Sequin
                      is behind the primary database. Messages may be delayed.
                    </div>
                  {/if}
                </div>
              {:else}
                <div class="flex items-center">
                  <Loader2 class="h-8 w-8 text-gray-400 animate-spin mr-2" />
                  <span class="text-4xl font-bold text-gray-500">ms</span>
                </div>
              {/if}
            </div>
          </div>
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
                    <Button variant="outline" size="sm" href={consumer.href}>
                      <ExternalLink class="h-4 w-4 mr-2" />
                      View Consumer
                    </Button>
                  </div>
                  <div
                    class="grid grid-cols-[auto_1fr] gap-x-2 gap-y-3 mb-2 auto-rows-fr items-center"
                  >
                    <p class="text-sm text-gray-500 justify-self-end">
                      Tables:
                    </p>
                    <div class="flex flex-wrap align-center">
                      {#each consumer.source_tables as table}
                        <div
                          class="inline-flex items-center text-sm font-medium text-black"
                        >
                          <icon
                            class="hero-table-cells w-5 h-5 mr-1 rounded {getColorFromName(
                              `${table.schema_name}.${table.table_name}`,
                            )}"
                          ></icon>

                          <pre
                            class="font-medium">{table.schema_name}.{table.table_name}</pre>
                        </div>
                      {/each}
                    </div>
                    <p class="text-sm text-gray-500 justify-self-end">
                      Stream type:
                    </p>
                    <div class="flex flex-wrap align-center">
                      {#if consumer.message_kind === "event"}
                        <Badge variant="default">
                          <SquareStack class="h-4 w-4 mr-1" />
                          Changes
                        </Badge>
                      {:else if consumer.message_kind === "record"}
                        <Badge variant="default">
                          <SquareStack class="h-4 w-4 mr-1" />
                          Rows
                        </Badge>
                      {/if}
                    </div>
                    <p class="text-sm text-gray-500 justify-self-end">
                      Consumer type:
                    </p>
                    <div class="flex flex-wrap align-center">
                      {#if consumer.consumer_kind === "http_push"}
                        <Badge variant="default">
                          <ArrowRightToLine class="h-4 w-4 mr-1" />
                          Push consumer
                        </Badge>
                      {:else if consumer.consumer_kind === "http_pull"}
                        <Badge variant="default">
                          <ArrowLeftFromLine class="h-4 w-4 mr-1" />
                          Pull consumer
                        </Badge>
                      {/if}
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
          </div>
        {/if}
      </CardContent>
    </Card>
  </main>
</div>
