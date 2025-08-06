<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { MysqlConsumer } from "../../consumers/types";

  export let consumer: MysqlConsumer;

  function getRoutingCode(consumer: MysqlConsumer): string | null {
    if (!consumer.routing || !consumer.routing.function) return null;
    const func = consumer.routing.function;
    if (func.type === "routing") {
      return (func as any).code || null;
    }
    return null;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>MySQL Configuration</CardTitle>
  </CardHeader>
  <CardContent class="p-6">
    <div class="grid gap-4">
      <div>
        <span class="text-sm text-muted-foreground">Host</span>
        <div class="mt-2">
          <div
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            <span>{consumer.sink.host}:{consumer.sink.port}</span>
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">Database</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {consumer.sink.database}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">Username</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {consumer.sink.username}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">SSL Enabled</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {consumer.sink.ssl ? "Yes" : "No"}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">Batch Size</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {consumer.sink.batch_size}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">Upsert Mode</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {consumer.sink.upsert_on_duplicate ? "Enabled" : "Disabled"}
          </span>
        </div>
      </div>
    </div>
  </CardContent>
</Card>

<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent>
    <div>
      <span class="text-sm text-muted-foreground">Table</span>
      <div class="mt-2">
        <span
          class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
        >
          {#if consumer.routing_id}
            Determined by <a
              href={`/functions/${consumer.routing_id}`}
              data-phx-link="redirect"
              data-phx-link-state="push"
              class="underline">router</a
            >
            <ExternalLink class="h-4 w-4 inline" />
          {:else}
            {consumer.sink.table_name}
          {/if}
        </span>
      </div>
    </div>
    {#if consumer.routing}
      {#if getRoutingCode(consumer)}
        <div class="mt-2">
          <span class="text-sm text-muted-foreground">Router</span>
          <div class="mt-2">
            <pre
              class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
                >{getRoutingCode(consumer)}</code
              ></pre>
          </div>
        </div>
      {/if}
    {/if}
  </CardContent>
</Card>
