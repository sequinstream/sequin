<script lang="ts">
  import { Input } from "$lib/components/ui/input";
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
  <CardHeader>
    <CardTitle>GCP Pub/Sub Configuration</CardTitle>
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
