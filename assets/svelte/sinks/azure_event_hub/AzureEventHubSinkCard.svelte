<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { AzureEventHubConsumer } from "../../consumers/types";

  export let consumer: AzureEventHubConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Azure EventHub Configuration</h2>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Namespace</span>
        <div class="mt-2">
          <div class="overflow-x-auto">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md inline-block whitespace-nowrap"
              >{consumer.sink.namespace}</span
            >
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Shared Access Key Name</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.shared_access_key_name}</span
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
  <CardContent class="p-6">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Event Hub Name</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {#if consumer.routing_id}
              determined-by-router
            {:else}
              {consumer.sink.event_hub_name}
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
