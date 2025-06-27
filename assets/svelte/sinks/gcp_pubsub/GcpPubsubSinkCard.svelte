<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { GcpPubsubConsumer } from "../../consumers/types";

  export let consumer: GcpPubsubConsumer;

  // Construct GCP Console URL for the topic
  $: consoleUrl = `https://console.cloud.google.com/cloudpubsub/topic/detail/${consumer.sink.topic_id}?project=${consumer.sink.project_id}`;
</script>

<Card>
  <CardHeader>
    <div class="flex justify-between items-center">
      <CardTitle>GCP Pub/Sub Configuration</CardTitle>
      <div class="flex space-x-2">
        <a href={consoleUrl} target="_blank" rel="noopener noreferrer">
          <Button variant="outline" size="sm">
            <ExternalLink class="h-4 w-4 mr-2" />
            View in GCP Console
          </Button>
        </a>
      </div>
    </div>
  </CardHeader>
  <CardContent class="p-6">
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Project ID</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.project_id}</span
          >
        </div>
      </div>
    </div></CardContent
  >
</Card>
<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent>
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Topic Path</span>
        <div class="mt-2">
          {#if consumer.routing_id}
            <span
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
              >projects/{consumer.sink.project_id}/topics/<span
                class="text-gray-500">determined-by-router</span
              ></span
            >
          {:else}
            <span
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
              >projects/{consumer.sink.project_id}/topics/{consumer.sink
                .topic_id}</span
            >
          {/if}
        </div>
      </div>
      <div>
        <span class="text-sm text-gray-500">Topic ID</span>
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
            {:else}
              {consumer.sink.topic_id}
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
