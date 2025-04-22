<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { SnsConsumer } from "../../consumers/types";

  export let consumer: SnsConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">SNS Configuration</h2>
      <div class="flex gap-2">
        <a
          href={`https://${consumer.sink.region}.console.aws.amazon.com/sns/v3/home?region=${consumer.sink.region}#/topics/${encodeURIComponent(consumer.sink.topic_arn)}`}
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

    <div class="flex flex-col gap-4">
      <div class="grid grid-cols-2 gap-4">
        <div>
          <span class="text-sm text-gray-500">Region</span>
          <div class="mt-2">
            <span>{consumer.sink.region}</span>
          </div>
        </div>

        <div>
          <span class="text-sm text-gray-500">Topic Type</span>
          <div class="mt-2">
            <span>{consumer.sink.is_fifo ? "FIFO" : "Standard"}</span>
          </div>
        </div>
      </div>
      <div>
        <span class="text-sm text-gray-500">Topic ARN</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.topic_arn}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
