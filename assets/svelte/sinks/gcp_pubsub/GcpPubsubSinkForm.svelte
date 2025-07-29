<!-- GCP PubSub Sink Form -->
<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Switch } from "$lib/components/ui/switch";
  import { Label } from "$lib/components/ui/label";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Info, ExternalLink } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";
  import type { GcpPubsubConsumer } from "$lib/consumers/types";
  import { Textarea } from "$lib/components/ui/textarea";
  import GcpAuthenticationForm from "$lib/components/GcpAuthenticationForm.svelte";

  export let form;
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  export let errors: any = {};
  export let isSelfHosted: boolean = true;

  let selectedDynamic = form.routingMode === "dynamic";
</script>

<Card>
  <CardHeader
    class="space-y-1.5 p-6 flex flex-row items-center justify-between"
  >
    <CardTitle>GCP Pub/Sub Configuration</CardTitle>
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
              The GCP Pub/Sub emulator lets you run Pub/Sub locally for dev and
              testing purposes.
              <a
                href="https://cloud.google.com/pubsub/docs/emulator"
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
    <div class="space-y-2">
      <Label for="project_id">Project ID</Label>
      <Input
        type="text"
        id="project_id"
        name="sink[project_id]"
        bind:value={form.sink.project_id}
        placeholder="my-gcp-project"
      />
      <p class="text-sm text-muted-foreground">
        Your Google Cloud project ID (e.g., my-project-123)
      </p>
      {#if errors.sink?.project_id}
        <p class="text-destructive text-sm">{errors.sink.project_id}</p>
      {/if}
    </div>
    {#if form.sink.use_emulator}
      <div class="space-y-2">
        <Label for="emulator_base_url">Emulator Base URL</Label>
        <Input
          type="text"
          id="emulator_base_url"
          name="sink[emulator_base_url]"
          bind:value={form.sink.emulator_base_url}
          placeholder="http://localhost:8085"
        />
        {#if errors.sink?.emulator_base_url}
          <p class="text-destructive text-sm">
            {errors.sink.emulator_base_url}
          </p>
        {/if}
      </div>
    {/if}

    {#if !form.sink.use_emulator}
      <GcpAuthenticationForm
        bind:sink={form.sink}
        errors={errors.sink || {}}
        {isSelfHosted}
      />
    {/if}
  </CardContent>
</Card>

<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <DynamicRoutingForm
      bind:form
      bind:selectedDynamic
      {functions}
      {refreshFunctions}
      {functionRefreshState}
      {errors}
      routedSinkType="gcp_pubsub"
    />

    {#if !selectedDynamic}
      <div>
        <Label for="topic_id">Topic ID</Label>
        <Input
          type="text"
          id="topic_id"
          name="sink[topic_id]"
          bind:value={form.sink.topic_id}
          placeholder="my-topic"
        />
        {#if errors.sink?.topic_id}
          <p class="text-destructive text-sm">{errors.sink.topic_id}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
