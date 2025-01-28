<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import type { S2Consumer } from "../../types/consumer";

  export let consumer: S2Consumer;
</script>

<Card>
  <CardContent class="flex flex-col gap-4 pt-6">
    <div class="flex flex-col gap-2">
      <Label>Basin</Label>
      <div class="text-sm">{consumer.sink.basin}</div>
    </div>

    <div class="flex flex-col gap-2">
      <Label>Stream</Label>
      <div class="text-sm">{consumer.sink.stream}</div>
    </div>
  </CardContent>
</Card>
