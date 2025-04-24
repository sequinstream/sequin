<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import TransformPicker from "$lib/consumers/TransformPicker.svelte";
  import { Input } from "$lib/components/ui/input";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { truncateMiddle } from "$lib/utils";
  import { toast } from "svelte-sonner";
  import {
    Loader2,
    ExternalLink,
    RotateCwIcon,
    CheckIcon,
    ChevronDown,
    Plus,
  } from "lucide-svelte";
  import { concatenateUrl } from "../databases/utils";
  import {
    DropdownMenu,
    DropdownMenuTrigger,
    DropdownMenuContent,
    DropdownMenuItem,
  } from "$lib/components/ui/dropdown-menu";
  import * as Tooltip from "$lib/components/ui/tooltip";

  export let live;
  export let form;
  export let parent;
  export let httpEndpoints;
  export let errors: any = {};

  export let transforms: Array<any> = [];
  export let refreshTransforms: () => void;
  export let transformRefreshState: "idle" | "refreshing" | "done" = "idle";

  const pushEvent = (event, payload = {}, cb = (result: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  let selectedHttpEndpoint = form.sink.httpEndpointId
    ? httpEndpoints.find((endpoint) => endpoint.id === form.sink.httpEndpointId)
    : null;

  $: {
    if (form.sink.httpEndpointId) {
      selectedHttpEndpoint = httpEndpoints.find(
        (endpoint) => endpoint.id === form.sink.httpEndpointId,
      );
    }
  }

  let isGeneratingWebhookSite = false;

  // Updated createWebhookSiteEndpoint function
  function createWebhookSiteEndpoint() {
    isGeneratingWebhookSite = true;
    pushEvent("generate_webhook_site_url", {}, (result: any) => {
      isGeneratingWebhookSite = false;
      if (result.http_endpoint_id) {
        pushEvent("refresh_http_endpoints", {}, () => {
          form.sink.httpEndpointId = result.http_endpoint_id;
        });
      } else if (result.error) {
        toast.error("Failed to generate Webhook.site URL:", result.error);
      } else {
        toast.error("Failed to generate Webhook.site URL");
      }
    });
  }

  // Updated refreshHttpEndpoints function
  function refreshHttpEndpoints() {
    httpEndpointsRefreshState = "refreshing";
    pushEvent("refresh_http_endpoints", {}, () => {
      httpEndpointsRefreshState = "done";
      setTimeout(() => {
        httpEndpointsRefreshState = "idle";
      }, 2000);
    });
  }

  // Declare the httpEndpointsRefreshState variable
  let httpEndpointsRefreshState: "idle" | "refreshing" | "done" = "idle";

  $: fullUrl = selectedHttpEndpoint?.baseUrl
    ? concatenateUrl(selectedHttpEndpoint?.baseUrl, form.sink.httpEndpointPath)
    : "";

  let selectedDynamic = form.routingMode == "dynamic";
  $: {
    if (selectedDynamic) {
      form.routingMode = "dynamic";
    } else {
      form.routingMode = "static";
      form.routingId = "none";
    }
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>Webhook Sink configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <div class="space-y-2">
      <Label for="request-timeout">Visibility timeout</Label>
      <div class="flex items-center space-x-2">
        <Input
          id="request-timeout"
          type="number"
          bind:value={form.ackWaitMs}
          class="w-24"
        />
        <span class="text-sm text-muted-foreground">ms</span>
      </div>
      <p class="text-sm text-muted-foreground">
        The maximum duration allowed for the HTTP request to complete. If the
        request doesn't finish within this time, it will be considered failed
        and may be retried.
      </p>
      {#if errors.ack_wait_ms}
        <p class="text-destructive text-sm">{errors.ack_wait_ms}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <Label for="batch-size">Batch size</Label>
      <div class="flex items-center space-x-2">
        <Input
          id="batch-size"
          type="number"
          bind:value={form.batchSize}
          class="w-24"
          min="1"
        />
        <span class="text-sm text-muted-foreground">records</span>
      </div>
      <p class="text-sm text-muted-foreground">
        The number of records to include in each webhook request. Default is 1
        record per request.
      </p>
      {#if errors.batch_size}
        <p class="text-destructive text-sm">{errors.batch_size}</p>
      {/if}
    </div>

    <Accordion class="w-full">
      <AccordionItem value="advanced">
        <AccordionTrigger>Advanced configuration</AccordionTrigger>
        <AccordionContent>
          <div class="space-y-4 pt-4">
            <div class="space-y-2">
              <Label for="max-ack-pending">Max ack pending</Label>
              <Input
                id="max-ack-pending"
                type="number"
                bind:value={form.maxAckPending}
              />
              <p class="text-sm text-muted-foreground">
                Sets the maximum number of messages that can be pending
                acknowledgment at any time. This helps control the flow of
                messages and prevents overwhelming the consumer.
              </p>
              {#if errors.max_ack_pending}
                <p class="text-destructive text-sm">
                  {errors.max_ack_pending}
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
    <CardTitle>HTTP Endpoint</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    {#if !form.sink.httpEndpointId}
      <p class="text-xs mb-2">
        Just kicking the tires?
        <button
          on:click={createWebhookSiteEndpoint}
          class="hover:underline bg-transparent border-none p-0 cursor-pointer inline-flex items-center"
          type="button"
          class:text-carbon-500={isGeneratingWebhookSite}
          class:text-link={!isGeneratingWebhookSite}
          disabled={isGeneratingWebhookSite}
        >
          {#if isGeneratingWebhookSite}
            <Loader2 class="h-3 w-3 mr-1 animate-spin" />
            Generating...
          {:else}
            Create and use a new Webhook.site endpoint
          {/if}
        </button>
      </p>
    {/if}
    <div class="flex items-center space-x-2">
      <Select
        selected={{
          value: form.sink.httpEndpointId,
          label: selectedHttpEndpoint?.name || "Select an endpoint",
        }}
        onSelectedChange={(event) => {
          form.sink.httpEndpointId = event.value;
        }}
      >
        <SelectTrigger class="w-full" disabled={httpEndpoints.length === 0}>
          <SelectValue placeholder="Select an endpoint" />
        </SelectTrigger>
        <SelectContent>
          {#each httpEndpoints as endpoint}
            <SelectItem value={endpoint.id}>{endpoint.name}</SelectItem>
          {/each}
        </SelectContent>
      </Select>
      <div class="flex items-center">
        <Tooltip.Root>
          <Tooltip.Trigger>
            <Button
              variant="outline"
              size="sm"
              on:click={refreshHttpEndpoints}
              disabled={httpEndpointsRefreshState === "refreshing"}
              class="p-2"
              aria-label="Refresh HTTP Endpoints"
            >
              {#if httpEndpointsRefreshState === "refreshing"}
                <RotateCwIcon class="h-5 w-5 animate-spin" />
              {:else if httpEndpointsRefreshState === "done"}
                <CheckIcon class="h-5 w-5 text-green-500" />
              {:else}
                <RotateCwIcon class="h-5 w-5" />
              {/if}
            </Button>
          </Tooltip.Trigger>
          <Tooltip.Content>
            <p class="text-xs">Refresh HTTP endpoints</p>
          </Tooltip.Content>
        </Tooltip.Root>
        <DropdownMenu>
          <DropdownMenuTrigger asChild let:builder>
            <Button
              variant="outline"
              size="sm"
              builders={[builder]}
              class="ml-2"
            >
              New HTTP Endpoint
              <ChevronDown class="h-4 w-4 ml-2" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent>
            <DropdownMenuItem class="cursor-pointer">
              <!-- Keep it a link to enable right click -->
              <ExternalLink class="h-4 w-4 mr-2" />
              <a href="/http-endpoints/new" target="_blank">
                Create new HTTP Endpoint
              </a>
            </DropdownMenuItem>
            <DropdownMenuItem
              on:click={createWebhookSiteEndpoint}
              class="cursor-pointer"
            >
              <Plus class="h-4 w-4 mr-2" />
              Create Webhook.site endpoint
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </div>
    </div>
    {#if errors.sink?.http_endpoint_id}
      <p class="text-destructive text-sm">Please select an HTTP endpoint</p>
    {/if}

    {#if selectedHttpEndpoint?.name}
      <div class="mt-4 space-y-4">
        <div class="flex items-center space-x-2">
          <Switch id="dynamic-routing" bind:checked={selectedDynamic} />
          <Label for="dynamic-routing">Dynamic routing</Label>
        </div>

        {#if selectedHttpEndpoint}
          {#if !selectedDynamic}
            <div class="space-y-2">
              <Label for="http-endpoint-path">HTTP Endpoint Path</Label>
              <div class="flex flex-row bg-white">
                <div
                  class="text-sm rounded-l px-4 h-10 flex items-center justify-center bg-muted border border-input whitespace-nowrap"
                >
                  {truncateMiddle(selectedHttpEndpoint.baseUrl, 50)}
                </div>
                <Input
                  id="http-endpoint-path"
                  bind:value={form.sink.httpEndpointPath}
                  placeholder="/some-path"
                  class="rounded-l-none focus-visible:ring-0 focus-visible:ring-offset-0"
                  style="border-left: none;"
                />
              </div>
              <p class="text-sm text-muted-foreground">
                The path to append to the base URL for this consumer's requests.
              </p>
              {#if errors.sink?.http_endpoint_path}
                {#each errors.sink.http_endpoint_path as error}
                  <p class="text-destructive text-sm">
                    {error}
                  </p>
                {/each}
              {/if}
            </div>
          {:else}
            <div class="space-y-2">
              <Label for="http-endpoint-path">HTTP Endpoint Path</Label>
              <div class="flex flex-row bg-white">
                <div
                  class="text-sm rounded-l px-4 h-10 flex items-center justify-center bg-muted border border-input whitespace-nowrap"
                >
                  {truncateMiddle(selectedHttpEndpoint.baseUrl, 50)}
                </div>
                <Input
                  id="http-endpoint-path"
                  value=""
                  class="rounded-l-none focus-visible:ring-0 focus-visible:ring-offset-0"
                  style="border-left: none;"
                  disabled={true}
                />
              </div>
              <p class="text-sm text-muted-foreground">
                When using dynamic routing, the path will be determined by your
                routing transform.
              </p>
            </div>

            <TransformPicker
              {transforms}
              selectedTransformId={form.routingId || "none"}
              title="Router"
              onTransformChange={(transformId) =>
                (form.routingId = transformId === "none" ? null : transformId)}
              {refreshTransforms}
              transformTypes={["routing"]}
              typeLabelKey="sink_type"
              createNewQueryParams="?type=routing&sink_type=http_push"
              bind:refreshState={transformRefreshState}
            >
              <p class="text-sm text-muted-foreground">
                Select a routing transform to dynamically set request
                parameters:
              </p>
            </TransformPicker>

            {#if errors.routing_id}
              <p class="text-destructive text-sm">{errors.routing_id}</p>
            {/if}

            <div class="p-4 bg-muted/50 rounded-md mt-4">
              <p class="text-sm text-muted-foreground">
                HTTP Endpoint routing transform allows you to dynamically set:
              </p>
              <ul
                class="list-disc pl-5 mt-2 space-y-1 text-sm text-muted-foreground"
              >
                <li>
                  <span class="font-mono text-xs bg-stone-100 px-1 rounded"
                    >method</span
                  >
                  - HTTP method (<code>GET</code>, <code>POST</code>,
                  <code>PUT</code>, etc.)
                </li>
                <li>
                  <span class="font-mono text-xs bg-stone-100 px-1 rounded"
                    >endpoint_path</span
                  >
                  - Path portion of the URL
                </li>
              </ul>
              <p class="text-sm text-muted-foreground mt-3">
                <strong>Note:</strong> The base URL cannot be changed dynamically.
              </p>
              <p class="text-sm text-muted-foreground mt-3">
                Your transform needs to return a map with the above keys to
                route the message.
              </p>
              <p class="text-sm text-muted-foreground mt-3">
                For missing or nil keys, the default values from the static
                configuration will be used.
              </p>
            </div>
          {/if}

          {#if !selectedDynamic && form.sink.httpEndpointId && fullUrl && fullUrl !== ""}
            <div class="mt-4 space-y-2">
              <Label>Fully qualified URL</Label>
              <div class="flex items-center space-x-2 overflow-x-auto">
                <p
                  class="text-xs w-fit font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
                >
                  {fullUrl}
                </p>
              </div>

              {#if errors.routing_id}
                <p class="text-destructive text-sm">{errors.routing_id}</p>
              {/if}

            </div>
          {/if}
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
