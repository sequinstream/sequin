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
  $: if (form.destination?.queue_url) {
    form.destination.is_fifo = form.destination.queue_url.endsWith(".fifo");
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
        bind:value={form.destination.queue_url}
        placeholder="https://sqs.region.amazonaws.com/123456789012/queue-name"
      />
      {#if errors.destination?.queue_url}
        <p class="text-destructive text-sm">{errors.destination.queue_url}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="access-key">AWS Access Key ID</Label>
      <Input
        id="access-key"
        data-1p-ignore
        bind:value={form.destination.access_key_id}
        placeholder="Enter your AWS access key ID"
        autocomplete="off"
      />
      {#if errors.destination?.access_key_id}
        <p class="text-destructive text-sm">
          {errors.destination.access_key_id}
        </p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="secret-key">AWS Secret Access Key</Label>
      <Input
        id="secret-key"
        data-1p-ignore
        type="password"
        bind:value={form.destination.secret_access_key}
        placeholder="Enter your AWS secret access key"
        autocomplete="off"
      />
      {#if errors.destination?.secret_access_key}
        <p class="text-destructive text-sm">
          {errors.destination.secret_access_key}
        </p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="batch-size">Batch size</Label>
      <div class="flex items-center space-x-2">
        <Input
          id="batch-size"
          type="number"
          bind:value={form.batchSize}
          class="w-24"
          min="1"
          max="10"
        />
        <span class="text-sm text-muted-foreground">records</span>
      </div>
      <p class="text-sm text-muted-foreground">
        The number of records to include in each SQS message. Maximum of 10.
      </p>
      {#if errors.batch_size}
        <p class="text-destructive text-sm">{errors.batch_size}</p>
      {/if}
    </div>

    <Accordion class="w-full">
      <AccordionItem value="advanced">
        <AccordionTrigger>Advanced configuration</AccordionTrigger>
        <AccordionContent>
          <div class="space-y-4 pt-4">
            <div class="space-y-2">
              <Label for="max-ack-pending">Max ack pending</Label>
              <Input
                id="max-ack-pending"
                type="number"
                bind:value={form.maxAckPending}
              />
              <p class="text-sm text-muted-foreground">
                Sets the maximum number of messages that can be pending
                acknowledgment at any time. This helps control the flow of
                messages and prevents overwhelming the consumer.
              </p>
              {#if errors.max_ack_pending}
                <p class="text-destructive text-sm">
                  {errors.max_ack_pending}
                </p>
              {/if}
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </CardContent>
</Card>
