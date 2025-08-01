<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import * as Alert from "$lib/components/ui/alert";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { MeilisearchConsumer } from "$lib/consumers/types";
  import { Label } from "$lib/components/ui/label";
  import { Eye, EyeOff, Info } from "lucide-svelte";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";

  export let form: MeilisearchConsumer;
  export let errors: any = {};
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  let showPassword = false;
  let isDynamicRouting = form.routingMode === "dynamic";

  // Import action options based on the meilisearch_sink.ex Elixir module

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Meilisearch configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <Alert.Root variant="info">
      <Alert.Title>Transform requirements</Alert.Title>
      <Alert.Description>
        <p class="mb-2">
          Your <a
            class="underline font-medium"
            href="https://sequinstream.com/docs/reference/transforms"
            target="_blank">transform</a
          >
          must return a document matching the schema of the
          <a
            class="underline font-medium"
            href="https://www.meilisearch.com/docs/learn/engine/datatypes#objects"
            >Meilisearch Index</a
          >.
        </p>
        <p class="mb-2">
          This includes an <code>id</code> field which is mandatory.
        </p>
        <div class="mb-2">
          Sequin uses Meilisearch's batch add/update action, which means:
          <ul class="ml-6 list-disc">
            <li>
              Meilisearch will create a new document or update an existing one
              based on the <code>id</code>
            </li>
            <li>
              Your transform can supply either a complete document or a partial
              document for update
            </li>
          </ul>
        </div>
        <div class="mb-2">
          <strong>Advanced:</strong> You can use routing functions to perform
          partial updates using Meilisearch's function capability:
          <ul class="ml-6 list-disc">
            <li>
              Return <code>action: :function</code> from your routing function
            </li>
            <li>
              Specify a filter expression and a <a
                class="underline font-medium"
                href="https://rhai.rs/book/engine/hello-world.html"
                target="_blank">Rhai</a
              > (a JavaScript-like language) function to update specific fields.
            </li>
            <li>
              This is useful for updating nested arrays or specific document
              properties
            </li>
          </ul>
        </div>
      </Alert.Description>
    </Alert.Root>

    <div class="space-y-2">
      <Label for="endpoint_url">Endpoint URL</Label>
      <Input
        id="endpoint_url"
        bind:value={form.sink.endpoint_url}
        placeholder="https://your-meilisearch-server:7700"
      />
      {#if errors.sink?.endpoint_url}
        <p class="text-destructive text-sm">{errors.sink.endpoint_url}</p>
      {/if}
      <p class="text-sm text-muted-foreground">
        The URL of your Meilisearch server
      </p>
    </div>

    <div class="space-y-2">
      <Label for="primary_key">Primary key</Label>
      <Input
        id="primary_key"
        bind:value={form.sink.primary_key}
        placeholder="my-primary-key"
      />
      {#if errors.sink?.primary_key}
        <p class="text-destructive text-sm">{errors.sink.primary_key}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="api_key">API key</Label>
      <div class="relative">
        <Input
          id="api_key"
          type={showPassword ? "text" : "password"}
          bind:value={form.sink.api_key}
          placeholder="Meilisearch API Key"
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
      routedSinkType="meilisearch"
      bind:selectedDynamic={isDynamicRouting}
      {errors}
    />

    {#if !isDynamicRouting}
      <div class="space-y-2">
        <Label for="index_name">Index name</Label>
        <Input
          id="index_name"
          bind:value={form.sink.index_name}
          placeholder="my-index"
        />
        {#if errors.sink?.index_name}
          <p class="text-destructive text-sm">{errors.sink.index_name}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
