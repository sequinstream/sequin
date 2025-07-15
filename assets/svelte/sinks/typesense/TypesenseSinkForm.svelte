<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import type { TypesenseConsumer } from "$lib/consumers/types";
  import { Label } from "$lib/components/ui/label";
  import { Eye, EyeOff, Info } from "lucide-svelte";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";

  export let form: TypesenseConsumer;
  export let errors: any = {};
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

  let isDynamicRouting = form.routingMode === "dynamic";
  let showPassword = false;

  // Import action options based on the typesense_sink.ex Elixir module

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Typesense configuration</CardTitle>
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
      <Label for="api_key">API key</Label>
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

    <Accordion>
      <AccordionItem value="advanced">
        <AccordionTrigger>Advanced Typsense Sink settings</AccordionTrigger>
        <AccordionContent>
          <div class="space-y-4 pt-4">
            <div class="space-y-2">
              <div class="flex items-center space-x-2">
                <Label for="timeout_seconds">Request timeout (seconds)</Label>
                <Tooltip.Root openDelay={200}>
                  <Tooltip.Trigger>
                    <Info class="h-4 w-4 text-gray-400 cursor-help" />
                  </Tooltip.Trigger>
                  <Tooltip.Content class="p-4 max-w-xs">
                    <div class="text-sm text-muted-foreground font-normal">
                      The timeout for requests to the Typesense server. You may
                      want to increase this if:
                      <ul class="list-disc pl-4 mt-2">
                        <li>You have a slow network connection</li>
                        <li>
                          You are indexing a large number of documents per batch
                        </li>
                        <li>You have auto-embedding enabled in Typesense</li>
                      </ul>
                    </div>
                  </Tooltip.Content>
                </Tooltip.Root>
              </div>
              <Input
                id="timeout_seconds"
                bind:value={form.sink.timeout_seconds}
                placeholder="10"
                type="number"
                min="1"
              />
              {#if errors.sink?.timeout_seconds}
                <p class="text-destructive text-sm">
                  {errors.sink.timeout_seconds}
                </p>
              {/if}
            </div>
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
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
      routedSinkType="typesense"
      {errors}
      bind:selectedDynamic={isDynamicRouting}
    />

    {#if !isDynamicRouting}
      <div class="space-y-2">
        <Label for="collection_name">Collection name</Label>
        <Input
          id="collection_name"
          bind:value={form.sink.collection_name}
          placeholder="my-collection"
        />
        {#if errors.sink?.collection_name}
          <p class="text-destructive text-sm">{errors.sink.collection_name}</p>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
