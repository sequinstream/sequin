<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Code, ExternalLink } from "lucide-svelte";
  import type { RedisStringConsumer } from "../../consumers/types";

  export let consumer: RedisStringConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Redis Configuration</h2>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">URL</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.url}</span
          >
        </div>
      </div>

      {#if consumer.sink.expireMs !== null}
        <div>
          <span class="text-sm text-gray-500">Expiration</span>
          <div class="mt-2">
            <span
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
              >{consumer.sink.expireMs}ms</span
            >
          </div>
        </div>
      {/if}

      <div>
        <span class="text-sm text-gray-500">Key Pattern</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
          >
            {#if consumer.routing_id}
              Determined by <a
                href={`/functions/${consumer.routing_id}`}
                data-phx-link="redirect"
                data-phx-link-state="push"
                class="underline">router</a
              >
              <ExternalLink class="h-4 w-4 inline" />
            {:else if consumer.sequence.table_name}
              sequin:{consumer.sequence.table_name}:&lt;primary-keys&gt;
            {:else}
              sequin:&lt;table-name&gt;:&lt;primary-keys&gt;
            {/if}
          </span>
        </div>
      </div>
    </div>
    {#if consumer.routing}
      <div class="mt-2">
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
