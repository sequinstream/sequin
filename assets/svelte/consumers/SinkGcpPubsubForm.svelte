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

  export let form;
  export let errors: any = {};
</script>

<Card>
  <CardHeader
    class="space-y-1.5 p-6 flex flex-row items-center justify-between"
  >
    <CardTitle>GCP Pub/Sub Configuration</CardTitle>
    <div>
      <Switch id="use-emulator" bind:checked={form.sink.use_emulator} />
      <Label for="use-emulator" class="align-text-top">Use Emulator</Label>
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
        <Label for="project-id">Emulator Connection URL</Label>
        <Input
          id="emulator-url"
          bind:value={form.sink.emulator_url}
          placeholder="0.0.0.0:8432"
        />
        <p class="text-sm text-muted-foreground">
          Your Emulator Connection URL
        </p>
        {#if errors.sink?.emulator_url}
          <p class="text-destructive text-sm">{errors.sink.emulator_url}</p>
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
  </CardContent>
</Card>
