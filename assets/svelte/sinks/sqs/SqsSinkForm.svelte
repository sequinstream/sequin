<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";
  import AwsAuthenticationForm from "$lib/components/AwsAuthenticationForm.svelte";

  export let form;
  export let errors: any = {};
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

  let isDynamicRouting = form.routingMode === "dynamic";

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
    <AwsAuthenticationForm bind:sink={form.sink} errors={errors.sink || {}} />
  </CardContent>
</Card>

<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <DynamicRoutingForm
      bind:form
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      routedSinkType="sqs"
      bind:selectedDynamic={isDynamicRouting}
      {errors}
    />

    <div class="space-y-2" hidden={isDynamicRouting}>
      <Label for="queue-url">Queue URL</Label>
      <Input
        id="queue-url"
        name="sink[queue_url]"
        bind:value={form.sink.queue_url}
        placeholder="https://sqs.region.amazonaws.com/123456789012/queue-name"
      />
      {#if errors.sink?.queue_url}
        <p class="text-destructive text-sm">{errors.sink.queue_url}</p>
      {/if}
    </div>

    {#if isDynamicRouting}
      <div class="space-y-2">
        <Label for="region">AWS Region</Label>
        <Input
          id="region"
          name="sink[region]"
          bind:value={form.sink.region}
          placeholder="us-east-1"
        />
        {#if errors.sink?.region}
          <p class="text-destructive text-sm">{errors.sink.region}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
