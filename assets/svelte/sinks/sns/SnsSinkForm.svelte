<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Info, ExternalLink } from "lucide-svelte";
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
  <CardHeader
    class="space-y-1.5 p-6 flex flex-row items-center justify-between"
  >
    <CardTitle>SNS Configuration</CardTitle>
    <div class="flex flex-row items-center gap-2">
      <Switch id="use-emulator" bind:checked={form.sink.use_emulator} />
      <Label for="use-emulator" class="align-text-top flex items-center gap-1">
        Use emulator
        <Tooltip.Root>
          <Tooltip.Trigger>
            <Info class="h-4 w-4 text-muted-foreground" />
          </Tooltip.Trigger>
          <Tooltip.Content class="max-w-xs">
            <p class="text-sm font-normal">
              Use LocalStack or another SNS emulator for local development and
              testing.
              <a
                href="https://docs.localstack.cloud/user-guide/aws/sns/"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center text-link hover:underline"
              >
                Learn more
                <ExternalLink class="w-3 h-3 ml-1" />
              </a>
            </p>
          </Tooltip.Content>
        </Tooltip.Root>
      </Label>
    </div>
  </CardHeader>
  <CardContent class="space-y-4">
    {#if form.sink.use_emulator}
      <div class="space-y-2">
        <Label for="emulator_base_url">Emulator Base URL</Label>
        <Input
          type="text"
          id="emulator_base_url"
          name="sink[emulator_base_url]"
          bind:value={form.sink.emulator_base_url}
          placeholder="http://localhost:4566"
        />
        {#if errors.sink?.emulator_base_url}
          <p class="text-destructive text-sm">
            {errors.sink.emulator_base_url}
          </p>
        {/if}
      </div>
    {/if}
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
