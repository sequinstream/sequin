<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import type { KafkaConsumer } from "../../consumers/types";

  export let consumer: KafkaConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Kafka Configuration</h2>
    </div>

    <div class="grid gap-4">
      <div>
        <span class="text-sm text-gray-500">Hosts</span>
        <div class="mt-2">
          <div
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md break-all w-fit"
          >
            <span>{consumer.sink.hosts}</span>
          </div>
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Topic</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.topic}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">TLS Enabled</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 py-1 px-2 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.tls ? "Yes" : "No"}</span
          >
        </div>
      </div>
    </div></CardContent
  >
</Card>
