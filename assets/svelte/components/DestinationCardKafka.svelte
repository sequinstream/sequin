<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import type { KafkaConsumer } from "../types/consumer";

  export let consumer: KafkaConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Kafka Destination</h2>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">Hosts</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.destination.hosts}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Topic</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.destination.topic}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">TLS Enabled</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.destination.tls ? "Yes" : "No"}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
