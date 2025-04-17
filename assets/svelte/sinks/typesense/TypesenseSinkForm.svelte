<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { Eye, EyeOff, Info } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Select } from "$lib/components/ui/select";

  export let form;
  export let errors: any = {};
  let showPassword = false;

  // Import action options based on the typesense_sink.ex Elixir module
  let importActionOptions = [
    { value: "create", label: "Create" },
    { value: "upsert", label: "Upsert" },
    { value: "update", label: "Update" },
    { value: "emplace", label: "Emplace" },
  ];

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Typesense Configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="endpoint_url">Endpoint URL</Label>
      <Input
        id="endpoint_url"
        bind:value={form.sink.endpoint_url}
        placeholder="https://your-typesense-server:8108"
      />
      {#if errors.sink?.endpoint_url}
        <p class="text-destructive text-sm">{errors.sink.endpoint_url}</p>
      {/if}
      <p class="text-sm text-muted-foreground">
        The URL of your Typesense server
      </p>
    </div>

    <div class="space-y-2">
      <Label for="collection_name">Collection Name</Label>
      <Input
        id="collection_name"
        bind:value={form.sink.collection_name}
        placeholder="my-collection"
      />
      {#if errors.sink?.collection_name}
        <p class="text-destructive text-sm">{errors.sink.collection_name}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="api_key">API Key</Label>
      <div class="relative">
        <Input
          id="api_key"
          type={showPassword ? "text" : "password"}
          bind:value={form.sink.api_key}
          placeholder="Typesense API Key"
          data-1p-ignore
          autocomplete="off"
        />
        <button
          type="button"
          class="absolute inset-y-0 right-0 flex items-center pr-3"
          on:click={togglePasswordVisibility}
        >
          {#if showPassword}
            <EyeOff class="h-4 w-4 text-gray-400" />
          {:else}
            <Eye class="h-4 w-4 text-gray-400" />
          {/if}
        </button>
      </div>
      {#if errors.sink?.api_key}
        <p class="text-destructive text-sm">{errors.sink.api_key}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="import_action">Import Action</Label>
      <select
        id="import_action"
        bind:value={form.sink.import_action}
        class="block w-full border border-gray-300 rounded-md p-2"
      >
        {#each importActionOptions as option}
          <option value={option.value}>{option.label}</option>
        {/each}
      </select>
      {#if errors.sink?.import_action}
        <p class="text-destructive text-sm">{errors.sink.import_action}</p>
      {/if}
      <div class="flex items-center text-sm text-muted-foreground">
        <Tooltip.Root>
          <Tooltip.Trigger class="cursor-default">
            <Info class="h-4 w-4 mr-1" />
          </Tooltip.Trigger>
          <Tooltip.Content>
            <p>
              <strong>Create:</strong> Add documents (fail if ID exists)<br />
              <strong>Upsert:</strong> Add or replace documents<br />
              <strong>Update:</strong> Update existing documents<br />
              <strong>Emplace:</strong> Add if ID doesn't exist, update if it does
            </p>
          </Tooltip.Content>
        </Tooltip.Root>
        How documents should be imported into Typesense
      </div>
    </div>

    <div class="space-y-2">
      <Label for="batch_size">Batch Size</Label>
      <Input
        id="batch_size"
        type="number"
        min="1"
        max="10000"
        bind:value={form.sink.batch_size}
        placeholder="40"
      />
      {#if errors.sink?.batch_size}
        <p class="text-destructive text-sm">{errors.sink.batch_size}</p>
      {/if}
      <p class="text-xs">
        Number of documents to batch together in a single import request to
        Typesense. Default is 40. Maximum is 10,000.
      </p>
    </div>

    <div class="space-y-2">
      <Label for="timeout_seconds">Timeout (seconds)</Label>
      <Input
        id="timeout_seconds"
        type="number"
        min="1"
        max="300"
        bind:value={form.sink.timeout_seconds}
        placeholder="5"
      />
      {#if errors.sink?.timeout_seconds}
        <p class="text-destructive text-sm">{errors.sink.timeout_seconds}</p>
      {/if}
      <p class="text-xs">
        Number of seconds to wait for a response from Typesense. Default is 5
        seconds. Maximum is 300 seconds.
      </p>
    </div>
  </CardContent>
</Card>
