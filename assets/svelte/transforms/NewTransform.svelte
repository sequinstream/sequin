<script lang="ts">
  import * as Card from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Info } from "lucide-svelte";
  import {
    Popover,
    PopoverContent,
    PopoverTrigger,
  } from "$lib/components/ui/popover";

  interface Sequence {
    id: string;
    name: string;
    table_schema: string;
    table_name: string;
    sort_column_name: string;
  }

  interface FormData {
    path: string;
  }

  interface FormErrors {
    path?: string[];
  }

  interface TestMessage {
    original: any;
    transformed: any;
  }

  export let sequence: Sequence;
  export let formData: FormData;
  export let formErrors: FormErrors = {};
  export let testMessages: TestMessage[] = [];
  export let validating: boolean = false;
  export let parent: string;
  export let live;

  let form = { ...formData };
  let dialogOpen = true;
  let selectedMessageIndex = 0;

  function pushEvent(
    event: string,
    payload = {},
    callback = (event: any) => {},
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("save", { path_transform: form });
  }

  $: pushEvent("validate", { path_transform: form });
</script>

<div class="flex h-full gap-4">
  <!-- Left Rail: Test Messages and Original Message -->
  <div class="w-1/3 border rounded-lg">
    <div class="p-4 border-b">
      <h2 class="text-lg font-semibold">Test Messages</h2>
    </div>
    <div class="p-4 space-y-2">
      {#if testMessages.length === 0}
        <div class="text-center py-8">
          <p class="text-muted-foreground">No test messages available.</p>
          <p class="text-sm text-muted-foreground mt-2">
            Make changes to your database to generate test messages.
          </p>
        </div>
      {:else}
        {#each testMessages as message, i}
          <button
            class="w-full text-left p-3 rounded-lg border transition-colors {selectedMessageIndex ===
            i
              ? 'bg-primary/10 border-primary'
              : 'hover:bg-muted'}"
            on:click={() => (selectedMessageIndex = i)}
          >
            <div class="flex justify-between items-center">
              <span class="font-medium">Message {i + 1}</span>
              <span class="text-sm text-muted-foreground">
                {message.original.action}
              </span>
            </div>
            {#if selectedMessageIndex === i}
              <div class="mt-2">
                <h3 class="text-sm font-medium mb-2">Original Message</h3>
                <pre
                  class="text-sm bg-muted p-2 rounded overflow-auto">{JSON.stringify(
                    message.original,
                    null,
                    2,
                  )}</pre>
              </div>
            {/if}
          </button>
        {/each}
      {/if}
    </div>
  </div>

  <!-- Middle Rail: Transform Configuration -->
  <div class="w-1/3 border rounded-lg">
    <div class="p-4 border-b">
      <h2 class="text-lg font-semibold">Transform Configuration</h2>
    </div>
    <div class="p-4">
      <form on:submit={handleSubmit} class="space-y-4">
        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <Label for="path">Transform Path</Label>
            <Popover>
              <PopoverTrigger>
                <Info class="w-4 h-4 text-muted-foreground" />
              </PopoverTrigger>
              <PopoverContent>
                <div class="text-sm">
                  <p>
                    Enter a path to extract data from the message. Valid paths
                    include:
                  </p>
                  <ul class="list-disc pl-4 mt-2">
                    <li>record - The main record data</li>
                    <li>changes - The changes made to the record</li>
                    <li>action - The type of change (insert/update/delete)</li>
                    <li>metadata - Additional metadata about the change</li>
                  </ul>
                </div>
              </PopoverContent>
            </Popover>
          </div>
          <Input
            id="path"
            bind:value={form.path}
            placeholder="e.g. record.id or changes.name"
          />
          {#if formErrors.path}
            <p class="text-sm text-destructive">{formErrors.path[0]}</p>
          {/if}
        </div>

        <Button type="submit" loading={validating}>
          <span slot="loading">Validating...</span>
          Create Transform
        </Button>
      </form>
    </div>
  </div>

  <!-- Right Rail: Transformed Output -->
  <div class="w-1/3 border rounded-lg">
    <div class="p-4 border-b">
      <h2 class="text-lg font-semibold">Transformed Output</h2>
    </div>
    <div class="p-4">
      {#if testMessages.length === 0}
        <div class="text-center py-8">
          <p class="text-muted-foreground">
            Select a test message to view output
          </p>
        </div>
      {:else}
        <pre class="text-sm bg-muted p-2 rounded overflow-auto">{JSON.stringify(
            testMessages[selectedMessageIndex].transformed,
            null,
            2,
          )}</pre>
      {/if}
    </div>
  </div>
</div>
