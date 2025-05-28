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
      <Label for="stream-arn">Stream ARN</Label>
      <Input
        id="stream-arn"
        bind:value={form.sink.stream_arn}
        placeholder="arn:aws:kinesis:..."
      />

      {#if errors.sink?.stream_arm}
        <p class="text-destructive text-sm">{errors.sink.stream_arn}</p>
      {/if}
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
