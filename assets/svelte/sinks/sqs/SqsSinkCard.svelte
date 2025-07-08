<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import CardHeader from "$lib/components/ui/card/card-header.svelte";
  import { Card, CardContent, CardTitle } from "$lib/components/ui/card";
  import type { SqsConsumer } from "../../consumers/types";

  export let consumer: SqsConsumer;
</script>

<Card>
  <CardHeader>
    <div class="flex justify-between items-center">
      <CardTitle>SQS Configuration</CardTitle>
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
  </CardHeader>
  <CardContent class="p-6">
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

      {#if consumer.sink.use_task_role}
        <div class="mt-4">
          <div>
            <span class="text-sm text-gray-500">AWS Credentials</span>
            <div class="mt-2">
              <span class="text-sm text-green-600"
                >Loaded from task role, environment, or AWS profile</span
              >
            </div>
          </div>
        </div>
      {/if}
    </div>
  </CardContent>
</Card>

<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent>
    <div>
      <span class="text-sm text-gray-500">Queue URL</span>
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
            {consumer.sink.queue_url}
          {/if}
        </span>
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
