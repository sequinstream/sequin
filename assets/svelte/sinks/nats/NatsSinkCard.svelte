<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Code, ExternalLink } from "lucide-svelte";
  import type { NatsConsumer } from "../../consumers/types";

  export let consumer: NatsConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">NATS configuration</h2>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Host</span>
        <div class="mt-2">
          <div class="overflow-x-auto">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md inline-block whitespace-nowrap"
              >{consumer.sink.host}</span
            >
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Port</span>
        <div class="mt-2">
          <div class="overflow-x-auto">
            <span
              class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md inline-block whitespace-nowrap"
              >{consumer.sink.port}</span
            >
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Username</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.username || "-"}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">TLS Enabled</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.tls ? "Yes" : "No"}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">JWT</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.jwt ? "********" : "-"}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">NKey Seed</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 px-2 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.nkey_seed ? "********" : "-"}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Subject</span>
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
              sequin.changes.{consumer.database
                .name}.&lt;table_schema&gt;.&lt;table_name&gt;.&lt;action&gt;
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
