<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";
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

  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

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

  let isDynamicRouting = form.routingMode === "dynamic";
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

    <div class="flex items-center space-x-2">
      <Switch id="batch-messages" bind:checked={form.sink.batch} />
      <Label for="batch-messages">Batch messages</Label>
    </div>
    {#if form.sink.batch}
      <p class="text-sm text-muted-foreground">
        The endpoint will receive a batch of messages under a <code
          class="relative rounded bg-muted px-[0.3rem] py-[0.2rem] font-mono text-sm"
          >data</code
        >
        key:
        <a
          href="https://sequinstream.com/docs/reference/sinks/webhooks#change-messages"
          target="_blank"
          class="inline-flex text-link hover:underline items-center"
          >Learn more <ExternalLink class="h-3 w-3 ml-1" /></a
        >
      </p>
      <pre
        class="relative rounded bg-muted px-[0.3rem] py-[0.2rem] font-mono text-sm"><code
          >{`{
  "data": [
    message_1,
    message_2,
    // ...
  ]
}
`}</code
        ></pre>
    {:else}
      <p class="text-sm text-muted-foreground">
        To control the shape of messages the endpoint will receive, <a
          href="https://sequinstream.com/docs/reference/transforms"
          target="_blank"
          class="text-link hover:underline">use a transform</a
        >.
      </p>
    {/if}

    {#if form.sink.batch}
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
    {/if}

    <Accordion class="w-full">
      <AccordionItem value="advanced">
        <AccordionTrigger>Advanced configuration</AccordionTrigger>
        <AccordionContent>
          <div class="space-y-4 pt-4">
            {#if form.sink.batch}
              <div class="space-y-2">
                <Label for="batch-timeout">Batch timeout</Label>
                <div class="flex items-center space-x-2">
                  <Input
                    id="batch-timeout"
                    type="number"
                    bind:value={form.batchTimeoutMs}
                    class="w-24"
                    min="1"
                  />
                  <span class="text-sm text-muted-foreground">ms</span>
                </div>
                <p class="text-sm text-muted-foreground">
                  The maximum time to wait for a batch to reach its full size
                  before sending. Defaults to 50ms.
                </p>
                {#if errors.batch_timeout_ms}
                  <p class="text-destructive text-sm">
                    {errors.batch_timeout_ms}
                  </p>
                {/if}
              </div>
            {/if}

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

            <div class="space-y-2">
              <Label for="max-retry-count">Max retry count</Label>
              <Input
                id="max-retry-count"
                type="number"
                bind:value={form.maxRetryCount}
              />
              <p class="text-sm text-muted-foreground">
                Maximum number of retry attempts for failed message deliveries.
                Messages exceeding this limit will be discarded.
              </p>
              {#if errors.max_retry_count}
                <p class="text-destructive text-sm">
                  {errors.max_retry_count}
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
        <SelectContent class="max-h-[60vh] overflow-y-auto">
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
        <DynamicRoutingForm
          {form}
          {functions}
          {refreshFunctions}
          {functionRefreshState}
          routedSinkType="http_push"
          {errors}
          bind:selectedDynamic={isDynamicRouting}
        />

        {#if !isDynamicRouting}
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
        {/if}

        {#if !isDynamicRouting && form.sink.httpEndpointId && fullUrl && fullUrl !== ""}
          <div class="mt-4 space-y-2">
            <Label>Fully qualified URL</Label>
            <div class="flex items-center space-x-2 overflow-x-auto">
              <p
                class="text-xs w-fit font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
              >
                {fullUrl}
              </p>
            </div>
          </div>
        {/if}
      </div>
    {/if}
  </CardContent>
</Card>
