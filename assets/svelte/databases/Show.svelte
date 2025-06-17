<script lang="ts">
  import {
    Clock,
    RotateCw,
    Radio,
    ExternalLink,
    SquareStack,
    ArrowRightToLine,
    ArrowLeftFromLine,
    Loader2,
    CircleGauge,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { getColorFromName } from "$lib/utils";
  import HealthSummary from "$lib/health/HealthSummary.svelte";
  import type { Health } from "$lib/health/Types";
  import { Badge } from "$lib/components/ui/badge";
  import { writable } from "svelte/store";
  import { formatRelativeTimestamp } from "$lib/utils";
  import HealthAlerts from "$lib/health/HealthAlerts.svelte";
  import CodeWithCopy from "$lib/components/CodeWithCopy.svelte";

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
    paused: boolean;
    hostname: string;
    port: number;
    database: string;
    username: string;
    ssl: boolean;
    pool_size: number;
    queue_interval: number;
    queue_target: number;
    pg_major_version: number;
    tables: Table[];
    tables_refreshed_at: string;
    inserted_at: string;
    updated_at: string;
    consumers: Consumer[];
    health: Health;
    publication_name: string;
    slot_name: string;
  }

  export let database: PostgresDatabase;
  export let live: any;
  export let parent: string;
  export let metrics: {
    avg_latency: number;
    replication_lag_bytes: number;
  };

  let refreshingPostgresInfo = writable(false);

  function pushEvent(event: string, params = {}, callback: any = () => {}) {
    live.pushEventTo("#" + parent, event, params, callback);
  }

  function handleRefreshPostgresInfo() {
    $refreshingPostgresInfo = true;
    pushEvent("refresh_postgres_info", {}, (res) => {
      $refreshingPostgresInfo = false;
    });
  }
</script>

<div class="min-h-screen font-sans">
  <main class="container mx-auto px-4 py-8">
    <!-- Existing overview content -->
    <div class="grid gap-6 lg:grid-cols-3 mb-6">
      <HealthSummary health={database.health} {pushEvent} />

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-2">
            <span class="text-sm font-medium text-gray-500">Postgres Info</span>
            <Button
              variant="outline"
              size="sm"
              on:click={handleRefreshPostgresInfo}
              disabled={$refreshingPostgresInfo}
            >
              {#if $refreshingPostgresInfo}
                <Loader2 class="h-4 w-4 animate-spin" />
              {:else}
                <RotateCw class="h-4 w-4" />
              {/if}
            </Button>
          </div>
          <div class="space-y-3">
            <div>
              <div class="text-sm text-gray-500">Version</div>
              {#if database.pg_major_version}
                <div class="text-xl font-bold">{database.pg_major_version}</div>
              {:else}
                <div class="flex items-center">
                  <Loader2 class="h-4 w-4 text-gray-400 animate-spin mr-2" />
                </div>
              {/if}
            </div>
            <div>
              <div class="text-sm text-gray-500">Tables</div>
              <div class="text-xl font-bold">{database.tables.length}</div>
            </div>
          </div>
          <div
            class="text-sm text-gray-500 mt-2"
            class:hidden={!database.tables_refreshed_at}
          >
            Last updated {formatRelativeTimestamp(database.tables_refreshed_at)}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent class="p-6">
          <div class="flex justify-between items-center mb-4">
            <span class="text-sm font-medium text-gray-500">Performance</span>
            <CircleGauge class="h-5 w-5 text-blue-500" />
          </div>
          <div class="space-y-4">
            <div>
              <div class="text-sm text-gray-500 mb-1">Avg. Latency</div>
              {#if metrics.avg_latency !== null}
                <div class="text-l font-bold">{metrics.avg_latency} ms</div>
              {:else}
                <div class="flex items-center">
                  <Loader2 class="h-4 w-4 text-gray-400 animate-spin mr-2" />
                  <span class="text-l font-bold text-gray-500">ms</span>
                </div>
              {/if}
            </div>
            <div>
              <div class="text-sm text-gray-500 mb-1">Replication Lag</div>
              {#if metrics.replication_lag_bytes !== null}
                <div class="text-l font-bold">
                  {Math.round(metrics.replication_lag_bytes / 1024 / 1024)} mb
                </div>
              {:else}
                <div class="flex items-center">
                  <Loader2 class="h-4 w-4 text-gray-400 animate-spin mr-2" />
                  <span class="text-l font-bold text-gray-500">mb</span>
                </div>
              {/if}
            </div>
          </div>
        </CardContent>
      </Card>
    </div>

    <HealthAlerts checks={database.health.checks} {pushEvent} />

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
          <div>
            <p class="text-sm text-gray-500">Publication Name</p>
            <p class="font-medium">{database.publication_name}</p>
          </div>
          <div>
            <p class="text-sm text-gray-500">Slot Name</p>
            <p class="font-medium">{database.slot_name}</p>
          </div>
          <!-- <div>
              <p class="text-sm text-gray-500">Queue Interval</p>
              <p class="font-medium">{database.queue_interval} ms</p>
            </div> -->
        </div>
      </CardContent>
    </Card>
  </main>
</div>
