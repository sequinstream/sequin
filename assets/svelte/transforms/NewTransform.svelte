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

  interface FormData {
    name: string;
    path: string;
  }

  interface FormErrors {
    name?: string[];
    path?: string[];
  }

  interface TestMessage {
    original: any;
    transformed: any;
  }

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

<div
  class="flex h-full gap-4 bg-white dark:bg-slate-950 text-slate-900 dark:text-slate-50"
>
  <!-- Left Rail: Test Messages and Original Message -->
  <div
    class="w-1/3 border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
  >
    <div class="p-4 border-b border-slate-200 dark:border-slate-800">
      <div class="flex items-center justify-between">
        <h2 class="text-lg font-semibold tracking-tight">Test Messages</h2>
        <div class="flex items-center gap-2">
          <div class="flex items-center gap-1.5">
            <div
              class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"
            ></div>
            <span class="text-sm text-slate-500 dark:text-slate-400 font-mono"
              >Database Synced</span
            >
          </div>
        </div>
      </div>
    </div>
    <div class="p-4 space-y-2">
      {#if testMessages.length === 0}
        <div class="text-center py-8">
          <div class="animate-pulse space-y-4">
            <div
              class="w-12 h-12 mx-auto rounded-full bg-slate-100 dark:bg-slate-800"
            ></div>
            <div class="space-y-2">
              <div
                class="h-4 w-48 mx-auto bg-slate-100 dark:bg-slate-800 rounded"
              ></div>
              <div
                class="h-3 w-64 mx-auto bg-slate-100 dark:bg-slate-800 rounded"
              ></div>
            </div>
          </div>
          <p class="text-slate-500 dark:text-slate-400 mt-4">
            Waiting for database events...
          </p>
          <p class="text-sm text-slate-400 dark:text-slate-500 mt-2 font-mono">
            Make changes to your database to generate test messages
          </p>
        </div>
      {:else}
        {#each testMessages as message, i}
          <button
            class="w-full text-left p-3 rounded-lg border border-slate-200 dark:border-slate-800 transition-all duration-200 {selectedMessageIndex ===
            i
              ? 'bg-blue-50 dark:bg-blue-950/50 border-blue-200 dark:border-blue-800 shadow-sm'
              : 'hover:bg-slate-50 dark:hover:bg-slate-800/50'}"
            on:click={() => (selectedMessageIndex = i)}
          >
            <div class="flex justify-between items-center">
              <span class="font-medium font-mono">Message {i + 1}</span>
              <span
                class="text-sm text-slate-500 dark:text-slate-400 font-mono"
              >
                {message.original.action}
              </span>
            </div>
            {#if selectedMessageIndex === i}
              <div
                class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-800"
              >
                <h3
                  class="text-sm font-medium mb-2 text-slate-500 dark:text-slate-400"
                >
                  Original Message
                </h3>
                <pre
                  class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300">{JSON.stringify(
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
  <div
    class="w-1/3 border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
  >
    <div class="p-4 border-b border-slate-200 dark:border-slate-800">
      <h2 class="text-lg font-semibold tracking-tight">
        Transform Configuration
      </h2>
    </div>
    <div class="p-4">
      <form on:submit={handleSubmit} class="space-y-4">
        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <Label for="name" class="font-mono">Transform Name</Label>
            <Popover>
              <PopoverTrigger>
                <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
              </PopoverTrigger>
              <PopoverContent
                class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
              >
                <div class="text-sm space-y-2">
                  <p class="text-slate-500 dark:text-slate-400">
                    Give your transform a descriptive name to help identify its
                    purpose.
                  </p>
                </div>
              </PopoverContent>
            </Popover>
          </div>
          <Input
            id="name"
            bind:value={form.name}
            placeholder="e.g. id-only-transform"
            class="font-mono bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
          />
          {#if formErrors.name}
            <p class="text-sm text-red-500 dark:text-red-400 font-mono">
              {formErrors.name[0]}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <Label for="path" class="font-mono">Transform Path</Label>
            <Popover>
              <PopoverTrigger>
                <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
              </PopoverTrigger>
              <PopoverContent
                class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
              >
                <div class="text-sm space-y-2">
                  <p class="text-slate-500 dark:text-slate-400">
                    Enter a path to extract data from the message. Valid paths
                    include:
                  </p>
                  <ul
                    class="list-disc pl-4 space-y-1 text-slate-500 dark:text-slate-400"
                  >
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
            class="font-mono bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
          />
          {#if formErrors.path}
            <p class="text-sm text-red-500 dark:text-red-400 font-mono">
              {formErrors.path[0]}
            </p>
          {/if}
        </div>

        <Button type="submit" loading={validating} class="font-mono">
          <span slot="loading">Validating...</span>
          Create Transform
        </Button>
      </form>
    </div>
  </div>

  <!-- Right Rail: Transformed Output -->
  <div
    class="w-1/3 border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
  >
    <div class="p-4 border-b border-slate-200 dark:border-slate-800">
      <h2 class="text-lg font-semibold tracking-tight">Transformed Output</h2>
    </div>
    <div class="p-4">
      {#if testMessages.length === 0}
        <div class="text-center py-8">
          <p class="text-slate-500 dark:text-slate-400 font-mono">
            Select a test message to view output
          </p>
        </div>
      {:else}
        <pre
          class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300">{JSON.stringify(
            testMessages[selectedMessageIndex].transformed,
            null,
            2,
          )}</pre>
      {/if}
    </div>
  </div>
</div>
