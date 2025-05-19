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
    <CardTitle>Kinesis Configuration</CardTitle>
  </CardHeader>
  <CardContent class="flex flex-col gap-4">
    <div class="flex flex-col gap-2">
      <Label for="stream-name">Stream Name</Label>
      <Input
        id="stream-name"
        bind:value={form.sink.stream_name}
        placeholder="my-stream"
      />
      {#if errors.sink?.stream_name}
        <p class="text-destructive text-sm">{errors.sink.stream_name}</p>
      {:else if errors.sink?.region}
        <p class="text-destructive text-sm">{errors.sink.region}</p>
      {/if}
    </div>

    <div class="flex flex-col gap-2">
      <Label for="region">Region</Label>
      <Input
        id="region"
        bind:value={form.sink.region}
        placeholder="us-east-1"
      />
    </div>

    <div class="flex flex-col gap-2">
      <Label for="access-key">AWS Access Key ID</Label>
      <Input
        id="access-key"
        data-1p-ignore
        bind:value={form.sink.access_key_id}
        placeholder="Enter your AWS access key ID"
        autocomplete="off"
      />
      {#if errors.sink?.access_key_id}
        <p class="text-destructive text-sm">{errors.sink.access_key_id}</p>
      {/if}
    </div>

    <div class="flex flex-col gap-2">
      <Label for="secret-key">AWS Secret Access Key</Label>
      <Input
        id="secret-key"
        data-1p-ignore
        type="password"
        bind:value={form.sink.secret_access_key}
        placeholder="Enter your AWS secret access key"
        autocomplete="off"
      />
      {#if errors.sink?.secret_access_key}
        <p class="text-destructive text-sm">{errors.sink.secret_access_key}</p>
      {/if}
    </div>
  </CardContent>
</Card>
