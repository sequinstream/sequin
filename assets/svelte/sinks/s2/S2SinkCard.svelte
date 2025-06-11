<script lang="ts">
  import { ExternalLink } from "lucide-svelte";
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import type { S2Consumer } from "../../consumers/types";

  export let consumer: S2Consumer;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">S2 Configuration</h2>
      <div class="flex space-x-2">
        <a
          href={consumer.sink.dashboard_url}
          target="_blank"
          rel="noopener noreferrer"
        >
          <Button variant="outline" size="sm">
            <ExternalLink class="h-4 w-4 mr-2" />
            View Basin in S2
          </Button>
        </a>
      </div>
    </div>

    <div class="space-y-4">
      <div class="grid grid-cols-2 gap-4">
        <div>
          <span class="text-sm text-gray-500">Basin</span>
          <div class="mt-2">
            <span
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >
              {consumer.sink.basin}
            </span>
          </div>
        </div>
        <div>
          <span class="text-sm text-gray-500">Stream</span>
          <div class="mt-2">
            <span
              class="font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
            >
              {consumer.sink.stream}
            </span>
          </div>
        </div>
      </div>
    </div>
  </CardContent>
</Card>
