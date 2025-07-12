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

  // Update is_fifo based on topic ARN
  $: if (form.sink?.topic_arn) {
    form.sink.is_fifo = form.sink.topic_arn.endsWith(".fifo");
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>SNS Configuration</CardTitle>
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
      routedSinkType="sns"
      bind:selectedDynamic={isDynamicRouting}
      {errors}
    />

    <div class="space-y-2" hidden={isDynamicRouting}>
      <Label for="topic-arn">Topic ARN</Label>
      <Input
        id="topic-arn"
        name="sink[topic_arn]"
        bind:value={form.sink.topic_arn}
        placeholder="arn:aws:sns:region:123456789012:topic-name"
      />
      {#if errors.sink?.topic_arn}
        <p class="text-destructive text-sm">{errors.sink.topic_arn}</p>
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
