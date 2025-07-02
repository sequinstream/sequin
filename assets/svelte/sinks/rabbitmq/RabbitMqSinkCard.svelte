<script lang="ts">
  import {
    Card,
    CardHeader,
    CardTitle,
    CardContent,
  } from "$lib/components/ui/card";
  import type { RabbitMqConsumer } from "../../consumers/types";

  export let consumer: RabbitMqConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">RabbitMQ Configuration</h2>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Host</span>
        <div class="mt-2">
          <div class="overflow-x-auto">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md inline-block whitespace-nowrap"
              >{consumer.sink.host}</span
            >
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Port</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.port}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Virtual Host</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.virtual_host}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Username</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.username || "-"}</span
          >
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
        <span class="text-sm text-gray-500">Exchange</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {#if consumer.routing_id}
              determined-by-router
            {:else}
              {consumer.sink.exchange}
            {/if}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Headers</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit whitespace-nowrap"
          >
            {#if consumer.routing_id}
              determined-by-router
            {:else}
              {JSON.stringify(consumer.sink.headers || {})}
            {/if}
          </span>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Routing Key</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {#if consumer.routing_id}
              determined-by-router
            {:else}
              sequin.database_name.table_schema.table_name.action
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
            class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
              >{consumer.routing.function.code}</code
            ></pre>
        </div>
      </div>
    {/if}
  </CardContent>
</Card>
