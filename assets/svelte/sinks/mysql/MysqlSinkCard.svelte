<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { MysqlConsumer } from "../../consumers/types";

  export let consumer: MysqlConsumer;
</script>

<Card>
  <CardHeader>
    <CardTitle>MySQL Configuration</CardTitle>
  </CardHeader>
  <CardContent class="p-6">
    <div class="grid grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Host</span>
        <div class="mt-2">
          <div
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            <span>{consumer.sink.host}:{consumer.sink.port}</span>
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Database</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            {consumer.sink.database}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Username</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            {consumer.sink.username}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Connection</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            {consumer.sink.ssl ? "SSL Enabled" : "SSL Disabled"}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Batch Size</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            {consumer.sink.batch_size}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Upsert Mode</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
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
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Table Name</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            {#if consumer.routing_id}
              determined-by-router
            {:else}
              {consumer.sink.table_name}
            {/if}
          </span>
        </div>
      </div>
    </div>

    {#if consumer.routing}
      <div class="mt-4">
        <span class="text-sm text-gray-500">Router</span>
        <div class="mt-2">
          <pre
            class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto w-full"><code
              >{consumer.routing.function.code}</code
            ></pre>
        </div>
      </div>
    {/if}
  </CardContent>
</Card>
