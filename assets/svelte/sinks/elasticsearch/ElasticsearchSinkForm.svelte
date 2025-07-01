<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import * as Alert from "$lib/components/ui/alert";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { ElasticsearchConsumer } from "$lib/consumers/types";
  import { Label } from "$lib/components/ui/label";
  import { Eye, EyeOff, Info } from "lucide-svelte";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";

  export let form: ElasticsearchConsumer;
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  let selectedDynamic = form.routingMode === "dynamic";
  export let errors: any = {};
  let showPassword = false;

  const authTypeOptions = [
    { value: "none", label: "None" },
    { value: "api_key", label: "API Key" },
    { value: "basic", label: "Basic Auth" },
    { value: "bearer", label: "Bearer Token" },
  ];

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Elasticsearch configuration</CardTitle>
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
          must return a document matching the schema of your Elasticsearch index.
        </p>
        <p class="mb-2">
          Ensure your transform returns a JSON document structure that's
          compatible with your Elasticsearch mapping.
        </p>
      </Alert.Description>
    </Alert.Root>

    <div class="space-y-2">
      <Label for="endpoint_url">Endpoint URL</Label>
      <Input
        id="endpoint_url"
        bind:value={form.sink.endpoint_url}
        placeholder="https://your-elasticsearch-server:9200"
      />
      {#if errors.sink?.endpoint_url}
        <p class="text-destructive text-sm">{errors.sink.endpoint_url}</p>
      {/if}
      <p class="text-sm text-muted-foreground">
        The URL of your Elasticsearch server
      </p>
    </div>

    <div class="space-y-2">
      <Label for="auth_type">Authentication type</Label>
      <Select
        selected={{
          value: form.sink.auth_type,
          label:
            authTypeOptions.find((opt) => opt.value === form.sink.auth_type)
              ?.label || "Select authentication type",
        }}
        onSelectedChange={(event) => {
          form.sink.auth_value = "";
          form.sink.auth_type = event.value;
        }}
      >
        <SelectTrigger>
          <SelectValue placeholder="Select authentication type" />
        </SelectTrigger>
        <SelectContent>
          {#each authTypeOptions as option}
            <SelectItem value={option.value}>{option.label}</SelectItem>
          {/each}
        </SelectContent>
      </Select>
      {#if errors.sink?.auth_type}
        <p class="text-destructive text-sm">{errors.sink.auth_type}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="auth_value">
        {#if form.sink.auth_type === "api_key"}
          API Key
        {:else if form.sink.auth_type === "basic"}
          Username and password
        {:else if form.sink.auth_type === "bearer"}
          Bearer token
        {/if}
      </Label>
      <div class="relative">
        <Input
          id="auth_value"
          type={showPassword ? "text" : "password"}
          bind:value={form.sink.auth_value}
          disabled={form.sink.auth_type === "none"}
          placeholder={form.sink.auth_type === "none"
            ? "N/A"
            : form.sink.auth_type === "api_key"
              ? "API Key"
              : form.sink.auth_type === "basic"
                ? "username:password"
                : "Bearer Token"}
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
      {#if errors.sink?.auth_value}
        <p class="text-destructive text-sm">{errors.sink.auth_value}</p>
      {/if}
      <p class="text-sm text-muted-foreground">
        {#if form.sink.auth_type === "none"}
          No authentication required
        {:else if form.sink.auth_type === "api_key"}
          Your Elasticsearch API key
        {:else if form.sink.auth_type === "basic"}
          Basic auth credentials in format username:password
        {:else if form.sink.auth_type === "bearer"}
          Your bearer token (without the "Bearer" prefix)
        {:else}
          Authentication credentials for your Elasticsearch server
        {/if}
      </p>
    </div>

    <div class="space-y-2">
      <Label for="batch_size">Batch Size</Label>
      <Input
        id="batch_size"
        type="number"
        bind:value={form.sink.batch_size}
        placeholder="500"
      />
      {#if errors.sink?.batch_size}
        <p class="text-destructive text-sm">{errors.sink.batch_size}</p>
      {/if}
      <p class="text-sm text-muted-foreground">
        Number of documents to send in each batch (default: 100)
      </p>
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
      routedSinkType="elasticsearch"
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      bind:selectedDynamic
      {errors}
    />

    {#if !selectedDynamic}
      <div class="space-y-2">
        <Label for="index_name">Index name</Label>
        <Input
          id="index_name"
          name="sink[index_name]"
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
