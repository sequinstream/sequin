<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { SqsConsumer } from "../../consumers/types";

  export let consumer: SqsConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">SQS Configuration</h2>
      <div class="flex space-x-2">
        <a
          href={`https://${consumer.sink.region}.console.aws.amazon.com/sqs/v3/home?region=${consumer.sink.region}#/queues/${encodeURIComponent(consumer.sink.queue_url)}`}
          target="_blank"
          rel="noopener noreferrer"
        >
          <Button variant="outline" size="sm">
            <ExternalLink class="h-4 w-4 mr-2" />
            View in AWS Console
          </Button>
        </a>
      </div>
    </div>

    <div class="space-y-4">
      <div class="grid grid-cols-2 gap-4">
        <div>
          <span class="text-sm text-gray-500">Region</span>
          <div class="mt-2">
            <span>{consumer.sink.region}</span>
          </div>
        </div>

        <div>
          <span class="text-sm text-gray-500">Queue Type</span>
          <div class="mt-2">
            <span>{consumer.sink.is_fifo ? "FIFO" : "Standard"}</span>
          </div>
        </div>
      </div>
      <div>
        <span class="text-sm text-gray-500">Queue URL</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.queue_url}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
