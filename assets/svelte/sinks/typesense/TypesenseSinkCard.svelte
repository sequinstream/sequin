<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { TypesenseConsumer } from "../../consumers/types";

  export let consumer: TypesenseConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Typesense Configuration</h2>
    </div>

    <div class="grid grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Endpoint URL</span>
        <div class="mt-2">
          <div
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md break-all w-fit"
          >
            <span>{consumer.sink.endpoint_url}</span>
          </div>
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
        <span class="text-sm text-gray-500">Collection Name</span>
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
              {consumer.sink.collection_name}
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
