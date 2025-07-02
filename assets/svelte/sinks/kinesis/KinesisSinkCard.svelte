<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { KinesisConsumer } from "../../consumers/types";

  export let consumer: KinesisConsumer;

  var awsConsoleUrl = "#";

  $: {
    const arnMatch =
      consumer.sink.stream_arn &&
      consumer.sink.stream_arn.match(
        /^arn:aws:kinesis:([^:]+):[^:]+:stream\/(.+)$/,
      );
    if (arnMatch) {
      const [, region, streamName] = arnMatch;
      awsConsoleUrl = `https://${region}.console.aws.amazon.com/kinesis/home?region=${region}#/streams/details/${encodeURIComponent(streamName)}`;
    }
  }
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Kinesis Configuration</h2>
      <div class="flex space-x-2">
        <a href={awsConsoleUrl} target="_blank" rel="noopener noreferrer">
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
          <span class="text-sm text-gray-500">Stream ARN</span>
          <div class="mt-2">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >
              {consumer.sink.stream_arn}
            </span>
          </div>
        </div>
      </div>
    </div>
  </CardContent>
</Card>
