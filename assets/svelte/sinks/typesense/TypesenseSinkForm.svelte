<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import * as Alert from "$lib/components/ui/alert";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { TypesenseConsumer } from "$lib/consumers/types";
  import { Label } from "$lib/components/ui/label";
  import { Eye, EyeOff, Info } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Select } from "$lib/components/ui/select";

  export let form: TypesenseConsumer;
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

    <Alert.Root variant="info">
      <Alert.Title>Transform Requirements</Alert.Title>
      <Alert.Description>
        <p class="mb-2">
          Your <a class="underline font-medium" href="https://sequinstream.com/docs/reference/transforms" target="_blank">transform</a> must return a document matching
          the schema of the <a class="underline font-medium" href="https://typesense.org/docs/28.0/api/collections.html#create-a-collection">Typesense collection</a>.
        </p>
        <p class="mb-2">
          This includes an <code>id</code> field which is mandatory.
        </p>
        <div class="mb-2">
          Sequin uses Typesense's <code>emplace</code> import action, which means:
          <ul class="ml-6 list-disc">
            <li>Typesense will create a new document or update an existing one based on the <code>id</code></li>
            <li>Your transform can supply either a complete document or a partial document for update</li>
          </ul>
        </div>
      </Alert.Description>
    </Alert.Root>

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
