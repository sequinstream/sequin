<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import type { RedisConsumer } from "../types/consumer";

  export let consumer: RedisConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Redis Destination</h2>
    </div>

    <div class="space-y-4">
      <div class="grid grid-cols-2 gap-4">
        <div>
          <span class="text-sm text-gray-500">Host</span>
          <div class="mt-2">
            <span>{consumer.destination.host}</span>
          </div>
        </div>

        <div>
          <span class="text-sm text-gray-500">Port</span>
          <div class="mt-2">
            <span>{consumer.destination.port}</span>
          </div>
        </div>
      </div>

      <div class="grid grid-cols-2 gap-4">
        <div>
          <span class="text-sm text-gray-500">Database</span>
          <div class="mt-2">
            <span>{consumer.destination.database}</span>
          </div>
        </div>

        <div>
          <span class="text-sm text-gray-500">TLS Enabled</span>
          <div class="mt-2">
            <span>{consumer.destination.tls ? "Yes" : "No"}</span>
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Key</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.destination.key}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
