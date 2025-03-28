<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { GcpPubsubConsumer } from "../../consumers/types";

  export let consumer: GcpPubsubConsumer;

  // Construct GCP Console URL for the topic
  $: consoleUrl = `https://console.cloud.google.com/cloudpubsub/topic/detail/${consumer.sink.topic_id}?project=${consumer.sink.project_id}`;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">GCP Pub/Sub Configuration</h2>
      <div class="flex space-x-2">
        <a href={consoleUrl} target="_blank" rel="noopener noreferrer">
          <Button variant="outline" size="sm">
            <ExternalLink class="h-4 w-4 mr-2" />
            View in GCP Console
          </Button>
        </a>
      </div>
    </div>

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

      <div>
        <span class="text-sm text-gray-500">Topic ID</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.topic_id}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Topic Path</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >projects/{consumer.sink.project_id}/topics/{consumer.sink
              .topic_id}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
