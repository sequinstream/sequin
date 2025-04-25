<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import type { RedisConsumer } from "../../consumers/types";

  export let consumer: RedisConsumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Redis Configuration</h2>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
      <div>
        <span class="text-sm text-gray-500">URL</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.url}</span
          >
        </div>
      </div>

      <div>
        <span class="text-sm text-gray-500">Stream Key</span>
        <div class="mt-2">
          <span
            class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >{consumer.sink.streamKey}</span
          >
        </div>
      </div>
    </div>
  </CardContent>
</Card>
