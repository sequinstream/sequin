<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";

  export let form;
  export let errors: any = {};

  // Update is_fifo based on queue URL
  $: if (form.sink?.queue_url) {
    form.sink.is_fifo = form.sink.queue_url.endsWith(".fifo");
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>SQS Configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="queue-url">Queue URL</Label>
      <Input
        id="queue-url"
        bind:value={form.sink.queue_url}
        placeholder="https://sqs.region.amazonaws.com/123456789012/queue-name"
      />
      {#if errors.sink?.queue_url}
        <p class="text-destructive text-sm">{errors.sink.queue_url}</p>
      {:else if errors.sink?.region}
        <p class="text-destructive text-sm">{errors.sink.region}</p>
      {/if}
    </div>

    <div class="space-y-2">
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

    <div class="space-y-2">
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
