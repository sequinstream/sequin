<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import CodeWithSecret from "./CodeWithSecret.svelte";
  import type { SequinStreamConsumer } from "../types/consumer";

  export let consumer: SequinStreamConsumer;
  export let apiBaseUrl: string;
  export let apiTokens: any[];
</script>

<Card>
  <CardContent class="p-6">
    <h2 class="text-lg font-semibold mb-4">Receive and acknowledge messages</h2>
    <div class="space-y-4">
      <div>
        <h3 class="text-md font-semibold mb-2">Receive messages</h3>
        <CodeWithSecret
          tabs={[
            {
              name: "cURL",
              value: `curl -X GET "${apiBaseUrl}/api/sequin_streams/${consumer.name}/receive" \\
     -H "Authorization: Bearer {{secret}}"`,
            },
          ]}
          {apiTokens}
        />
      </div>
      <div>
        <h3 class="text-md font-semibold mb-2">Acknowledge messages</h3>
        <CodeWithSecret
          tabs={[
            {
              name: "cURL",
              value: `curl -X POST "${apiBaseUrl}/api/sequin_streams/${consumer.name}/ack" \\
     -H "Authorization: Bearer {{secret}}" \\
     -H "Content-Type: application/json" \\
     -d '{"ack_ids": ["<ack_id>"]}'`,
            },
          ]}
          {apiTokens}
        />
      </div>
    </div>
  </CardContent>
</Card>
