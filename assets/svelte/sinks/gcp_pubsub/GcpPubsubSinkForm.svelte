<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { Textarea } from "$lib/components/ui/textarea";
  import { Info, ExternalLink } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";

  export let form;
  export let errors: any = {};
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
      <Label for="project-id">Project ID</Label>
      <Input
        id="project-id"
        bind:value={form.sink.project_id}
        placeholder="my-project-id"
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
        <Label for="emulator-url">Emulator connection URL</Label>
        <Input
          id="emulator-url"
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
    <div class="space-y-2">
      <Label for="topic-id">Topic ID</Label>
      <Input
        id="topic-id"
        bind:value={form.sink.topic_id}
        placeholder="my-topic"
      />
      <p class="text-sm text-muted-foreground">The ID of the Pub/Sub topic</p>
      {#if errors.sink?.topic_id}
        <p class="text-destructive text-sm">{errors.sink.topic_id}</p>
      {/if}
    </div>

    {#if !form.sink.use_emulator}
      <div class="space-y-2">
        <Label for="credentials">Service Account Credentials</Label>
        <Textarea
          id="credentials"
          bind:value={form.sink.credentials}
          placeholder="Paste your JSON service account key here"
          rows={8}
          data-1p-ignore
          autocomplete="off"
        />
        <p class="text-sm text-muted-foreground">
          JSON credentials from your Google Cloud service account key
        </p>
        {#if errors.sink?.credentials}
          <p class="text-destructive text-sm">{errors.sink.credentials}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
