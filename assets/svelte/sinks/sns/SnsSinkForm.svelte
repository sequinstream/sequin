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

  // Update is_fifo based on topic ARN
  $: if (form.sink?.topic_arn) {
    form.sink.is_fifo = form.sink.topic_arn.endsWith(".fifo");
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>SNS Configuration</CardTitle>
  </CardHeader>
  <CardContent class="flex flex-col gap-4">
    <div class="flex flex-col gap-2">
      <Label for="topic-arn">Topic ARN</Label>
      <Input
        id="topic-arn"
        bind:value={form.sink.topic_arn}
        placeholder="arn:aws:sns:region:123456789012:topic-name"
      />
      {#if errors.sink?.topic_arn}
        <p class="text-destructive text-sm">{errors.sink.topic_arn}</p>
      {:else if errors.sink?.region}
        <p class="text-destructive text-sm">{errors.sink.region}</p>
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
        <p class="text-destructive text-sm">
          {errors.sink.access_key_id}
        </p>
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
        <p class="text-destructive text-sm">
          {errors.sink.secret_access_key}
        </p>
      {/if}
    </div>
  </CardContent>
</Card>
