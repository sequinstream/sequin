<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { MeilisearchConsumer } from "../../consumers/types";

  export let consumer: MeilisearchConsumer;
</script>

<Card>
  <CardHeader>
    <CardTitle>Meilisearch Configuration</CardTitle>
  </CardHeader>
  <CardContent class="p-6">
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

      <div>
        <span class="text-sm text-gray-500">Primary Key</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.primary_key}</span
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
        <span class="text-sm text-gray-500">Index Name</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {#if consumer.routing_id}
              determined-by-router
            {:else}
              {consumer.sink.index_name}
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
