<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { S2Consumer } from "../../consumers/types";

  export let consumer: S2Consumer;
</script>

<Card>
  <CardHeader>
    <CardTitle>
      <div class="flex justify-between items-center">
        <h2>S2 Configuration</h2>
        <div class="flex space-x-2">
          <a
            href={consumer.sink.dashboard_url}
            target="_blank"
            rel="noopener noreferrer"
          >
            <Button variant="outline" size="sm">
              <ExternalLink class="h-4 w-4 mr-2" />
              View Basin in S2
            </Button>
          </a>
        </div>
      </div>
    </CardTitle>
  </CardHeader>
  <CardContent>
    <div class="space-y-4">
      <div class="grid grid-cols-2 gap-4">
        <div>
          <span class="text-sm text-gray-500">Basin</span>
          <div class="mt-2">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >
              {#if consumer.routing_id}
                determined-by-router
              {:else}
                {consumer.sink.basin}
              {/if}
            </span>
          </div>
        </div>
        <div>
          <span class="text-sm text-gray-500">Stream</span>
          <div class="mt-2">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >
              {#if consumer.routing_id}
                determined-by-router
              {:else}
                {consumer.sink.stream}
              {/if}
            </span>
          </div>
        </div>
      </div>
    </div>
  </CardContent>
</Card>

{#if consumer.routing}
  <Card class="mt-4">
    <CardHeader>
      <CardTitle>
        <div class="flex justify-between items-center">
          <h2>Routing</h2>
          {#if consumer.routing_id}
            <a
              href={`/functions/${consumer.routing_id}`}
              target="_blank"
              rel="noopener noreferrer"
            >
              <Button variant="outline" size="sm">
                <ExternalLink class="h-4 w-4 mr-2" />
                Routing function
              </Button>
            </a>
          {/if}
        </div>
      </CardTitle>
    </CardHeader>
    <CardContent>
      <pre
        class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
          >{consumer.routing.function.code}</code
        ></pre>
    </CardContent>
  </Card>
{/if}
