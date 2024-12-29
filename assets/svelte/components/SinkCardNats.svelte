<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import type { NatsConsumer } from "../types/consumer";

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
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md inline-block whitespace-nowrap"
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
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md inline-block whitespace-nowrap"
              >{consumer.sink.port}</span
            >
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Username</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.username || "-"}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
