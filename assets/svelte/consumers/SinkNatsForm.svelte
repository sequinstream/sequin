<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";

  export let form;
  export let errors: any = {};
</script>

<Card>
  <CardHeader>
    <CardTitle>NATS configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="host">Host</Label>
      <Input id="host" bind:value={form.sink.host} placeholder="localhost" />
      {#if errors.sink?.host}
        <p class="text-destructive text-sm">{errors.sink.host}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="port">Port</Label>
      <Input
        id="port"
        type="number"
        bind:value={form.sink.port}
        placeholder="4222"
      />
      {#if errors.sink?.port}
        <p class="text-destructive text-sm">{errors.sink.port}</p>
      {/if}
    </div>
  </CardContent>
</Card>
