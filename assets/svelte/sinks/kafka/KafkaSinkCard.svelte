<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent, CardTitle } from "$lib/components/ui/card";
  import CardHeader from "$lib/components/ui/card/card-header.svelte";
  import type { KafkaConsumer } from "../../consumers/types";

  export let consumer: KafkaConsumer;

  function getRoutingCode(consumer: KafkaConsumer): string | null {
    if (!consumer.routing || !consumer.routing.function) return null;
    const func = consumer.routing.function;
    if (func.type === "routing" && "code" in func) {
      return func.code;
    }
    return null;
  }
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Kafka Configuration</h2>
    </div>

    <div class="grid gap-4">
      <div>
        <span class="text-sm text-muted-foreground">Hosts</span>
        <div class="mt-2">
          <div
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md break-all w-fit"
          >
            <span>{consumer.sink.hosts}</span>
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">Topic</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.topic}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-muted-foreground">TLS Enabled</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.tls ? "Yes" : "No"}</span
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
    <div>
      <span class="text-sm text-gray-500">Topic</span>
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
            {consumer.sink.topic}
          {/if}
        </span>
      </div>
    </div>
    {#if consumer.routing}
      {#if getRoutingCode(consumer)}
        <div class="mt-2">
          <span class="text-sm text-gray-500">Router</span>
          <div class="mt-2">
            <pre
              class="font-mono bg-slate-50 p-2 border border-slate-100 rounded-md text-sm overflow-x-auto"><code
                >{getRoutingCode(consumer)}</code
              ></pre>
          </div>
        </div>
      {/if}
    {/if}
  </CardContent>
</Card>
