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
</script>

<Card>
  <CardHeader>
    <CardTitle>Sequin Stream Configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="request-timeout">Visibility timeout</Label>
      <div class="flex items-center space-x-2">
        <Input
          id="request-timeout"
          type="number"
          bind:value={form.ackWaitMs}
          class="w-24"
        />
        <span class="text-sm text-muted-foreground">ms</span>
      </div>
      <p class="text-sm text-muted-foreground">
        The maximum duration allowed for processing a message. If the message
        isn't acknowledged within this time, it will be considered failed and
        may be retried.
      </p>
      {#if errors.ack_wait_ms}
        <p class="text-destructive text-sm">{errors.ack_wait_ms}</p>
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
                <p class="text-destructive text-sm">{errors.max_ack_pending}</p>
              {/if}
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  </CardContent>
</Card>
