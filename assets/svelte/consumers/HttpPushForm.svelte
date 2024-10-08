<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
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
  import TableSelector from "../components/TableSelector.svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { cn, truncateMiddle } from "$lib/utils";
  import { toast } from "svelte-sonner";
  import {
    ExternalLinkIcon,
    Loader2,
    ExternalLink,
    RefreshCwIcon,
    CheckIcon,
  } from "lucide-svelte";
  import { concatenateUrl } from "../databases/utils";
  import SortAndFilterCard from "../components/SortAndFilterCard.svelte";

  export let live;
  export let parent;
  export let consumer;
  export let databases;
  export let httpEndpoints;
  export let errors: any = {};
  export let submitError;

  let form = {
    messageKind: consumer.message_kind || "event",
    postgresDatabaseId: consumer.postgres_database_id,
    tableOid: consumer.table_oid,
    sourceTableFilters: consumer.source_table_filters || [],
    sourceTableActions: consumer.source_table_actions || [],
    name: consumer.name || "",
    ackWaitMs: consumer.ack_wait_ms || 30000,
    maxAckPending: consumer.max_ack_pending || 10000,
    maxWaiting: consumer.max_waiting,
    httpEndpointId: consumer.http_endpoint_id,
    httpEndpointPath: consumer.http_endpoint_path || "",
    sortColumnAttnum: consumer.sort_column_attnum || null,
    recordConsumerState: consumer.record_consumer_state || {
      initialMinSortCol: null,
    },
  };
  let isSubmitting = false;

  const pushEvent = (event, payload = {}, cb = (result?: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  let selectedDatabase = form.postgresDatabaseId
    ? databases.find((db) => db.id === form.postgresDatabaseId)
    : null;
  let selectedTable =
    form.tableOid && selectedDatabase
      ? selectedDatabase.tables.find((table) => table.oid === form.tableOid)
      : null;
  let selectedHttpEndpoint = form.httpEndpointId
    ? httpEndpoints.find((endpoint) => endpoint.id === form.httpEndpointId)
    : null;

  const isEditMode = !!consumer.id;

  $: {
    if (isEditMode || (form.postgresDatabaseId && form.tableOid)) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId
      );
      if (selectedDatabase) {
        selectedTable = selectedDatabase.tables.find(
          (table) => table.oid === form.tableOid
        );
      }
    }
  }

  $: {
    if (form.httpEndpointId) {
      selectedHttpEndpoint = httpEndpoints.find(
        (endpoint) => endpoint.id === form.httpEndpointId
      );
    }
  }

  let dialogOpen = true;
  let showConfirmDialog = false;
  let isGeneratingWebhookSite = false;

  // Updated createWebhookSiteEndpoint function
  function createWebhookSiteEndpoint() {
    isGeneratingWebhookSite = true;
    pushEvent("generate_webhook_site_url", {}, (result: any) => {
      isGeneratingWebhookSite = false;
      if (result.http_endpoint_id) {
        pushEvent("refresh_http_endpoints", {}, () => {
          form.httpEndpointId = result.http_endpoint_id;
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

  function handleConsumerSubmit() {
    isSubmitting = true;
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        isSubmitting = false;
      }
    });
  }

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;

    // Set the form name based on the selected table
    if (form.tableOid) {
      const selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId
      );
      if (selectedDatabase) {
        const selectedTable = selectedDatabase.tables.find(
          (table) => table.oid === form.tableOid
        );
        if (selectedTable) {
          const tableName = selectedTable.name;
          const newName = `${tableName}_push_consumer`;
          form.name = newName;
        }
      }
    }
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
    // Trigger a form update to refresh error messages
    pushEvent("form_updated", { form });
  }

  function handleClose() {
    pushEvent("form_closed");
  }

  $: isCreateConsumerDisabled = !form.postgresDatabaseId || !form.tableOid;

  $: fullUrl =
    selectedHttpEndpoint?.baseUrl && form.httpEndpointPath
      ? concatenateUrl(selectedHttpEndpoint?.baseUrl, form.httpEndpointPath)
      : "";
</script>

<FullPageModal
  title={isEditMode
    ? "Edit an HTTP push consumer"
    : "Create an HTTP push consumer"}
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form
    on:submit|preventDefault={handleConsumerSubmit}
    class="space-y-6 max-w-3xl mx-auto mt-6"
  >
    <Card>
      <CardHeader>
        <CardTitle>Source</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label>Table</Label>
          {#if isEditMode}
            <Select
              disabled
              selected={{
                value: form.postgresDatabaseId,
                label: selectedDatabase?.name || "Selected database",
              }}
            >
              <SelectTrigger
                class={cn(
                  "w-full",
                  "bg-muted text-muted-foreground opacity-100"
                )}
              >
                <SelectValue placeholder="Selected database" />
              </SelectTrigger>
            </Select>
            <Select
              disabled
              selected={{
                value: form.tableOid,
                label: selectedTable?.name || "Selected table",
              }}
            >
              <SelectTrigger
                class={cn(
                  "w-full",
                  "bg-muted text-muted-foreground opacity-100"
                )}
              >
                <SelectValue placeholder="Selected table" />
              </SelectTrigger>
            </Select>
          {:else}
            <TableSelector
              {pushEvent}
              {databases}
              selectedDatabaseId={form.postgresDatabaseId}
              selectedTableOid={form.tableOid}
              onSelect={handleTableSelect}
            />
          {/if}
          {#if errors.postgres_database_id || errors.table_oid}
            <p class="text-destructive text-sm">
              {errors.postgres_database_id || errors.table_oid}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="message-kind">Source</Label>
          <Select
            selected={{
              value: form.messageKind,
              label:
                form.messageKind === "event" ? "WAL (Changes)" : "Table (Rows)",
            }}
            onSelectedChange={(event) => {
              form.messageKind = event.value;
            }}
            disabled={isEditMode}
          >
            <SelectTrigger
              class={cn(
                "w-full",
                isEditMode && "bg-muted text-muted-foreground opacity-100"
              )}
            >
              <SelectValue placeholder="Select source type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem class="cursor-pointer" value="event"
                >WAL (Changes)</SelectItem
              >
              <SelectItem class="cursor-pointer" value="record"
                >Table (Rows)</SelectItem
              >
            </SelectContent>
          </Select>
          <p class="text-sm text-muted-foreground">
            {#if form.messageKind === "event"}
              By consuming from the table's WAL, this consumer will receive
              every create, update, and delete that happens to rows and includes
              both new and old values. Replays and backfills are limited. <a
                href="https://sequinstream.com/docs/core-concepts#rows-and-changes"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center text-link hover:underline"
              >
                Learn more
                <ExternalLinkIcon class="w-3 h-3 ml-1" />
              </a>
            {:else if form.messageKind === "record"}
              By consuming directly from the table, this consumer will receive
              the latest version of rows whenever they change. Messages will not
              include old values and the consumer will not receive deletes. <a
                href="https://sequinstream.com/docs/core-concepts#rows-and-changes"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center text-link hover:underline"
              >
                Learn more
                <ExternalLinkIcon class="w-3 h-3 ml-1" />
              </a>
            {/if}
          </p>
          {#if errors.message_kind}
            <p class="text-destructive text-sm">
              {errors.message_kind}
            </p>
          {/if}
        </div>
      </CardContent>
    </Card>

    <SortAndFilterCard
      messageKind={form.messageKind}
      showStartPositionForm={!isEditMode}
      {selectedTable}
      bind:form
      {errors}
      {isEditMode}
      onFilterChange={handleFilterChange}
    />

    <Card>
      <CardHeader>
        <CardTitle>Push configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="request-timeout">Request timeout</Label>
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
            The maximum duration allowed for the HTTP request to complete. If
            the request doesn't finish within this time, it will be considered
            failed and may be retried.
          </p>
          {#if errors.ack_wait_ms}
            <p class="text-destructive text-sm">{errors.ack_wait_ms}</p>
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
        {#if !form.httpEndpointId}
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
              value: form.httpEndpointId,
              label: selectedHttpEndpoint?.name || "Select an endpoint",
            }}
            onSelectedChange={(event) => {
              form.httpEndpointId = event.value;
            }}
          >
            <SelectTrigger class="w-full">
              <SelectValue placeholder="Select an endpoint" />
            </SelectTrigger>
            <SelectContent>
              {#each httpEndpoints as endpoint}
                <SelectItem value={endpoint.id}>{endpoint.name}</SelectItem>
              {/each}
            </SelectContent>
          </Select>
          <div class="flex items-center">
            <Button
              variant="outline"
              size="sm"
              on:click={refreshHttpEndpoints}
              disabled={httpEndpointsRefreshState === "refreshing"}
              class="p-2"
              aria-label="Refresh HTTP Endpoints"
            >
              {#if httpEndpointsRefreshState === "refreshing"}
                <RefreshCwIcon class="h-5 w-5 animate-spin" />
              {:else if httpEndpointsRefreshState === "done"}
                <CheckIcon class="h-5 w-5 text-green-500" />
              {:else}
                <RefreshCwIcon class="h-5 w-5" />
              {/if}
            </Button>
            <a href="/http-endpoints/new" target="_blank" class="ml-2">
              <Button variant="outline" size="sm">
                <ExternalLink class="h-4 w-4 mr-2" />
                New HTTP Endpoint
              </Button>
            </a>
          </div>
        </div>

        {#if errors.http_endpoint_id}
          <p class="text-destructive text-sm">Please select an HTTP endpoint</p>
        {/if}

        {#if form.httpEndpointId}
          <div class="space-y-2">
            <Label for="http-endpoint-path">Consumer Endpoint Path</Label>
            <div class="flex flex-row bg-white">
              <div
                class="text-sm rounded-l px-4 h-10 flex items-center justify-center bg-muted border border-input whitespace-nowrap"
              >
                {truncateMiddle(selectedHttpEndpoint.baseUrl, 50)}
              </div>
              <Input
                id="http-endpoint-path"
                bind:value={form.httpEndpointPath}
                placeholder="/webhook"
                class="rounded-l-none focus-visible:ring-0 focus-visible:ring-offset-0"
                style="border-left: none;"
              />
            </div>
            <p class="text-sm text-muted-foreground">
              The path to append to the base URL for this consumer's requests.
            </p>
            {#if errors.http_endpoint_path}
              <p class="text-destructive text-sm">
                {errors.http_endpoint_path}
              </p>
            {/if}
          </div>
        {/if}

        {#if form.httpEndpointId && fullUrl && fullUrl !== ""}
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
      </CardContent>
    </Card>
    <Card>
      <CardHeader>
        <CardTitle>Consumer</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="consumer-name">Name</Label>
          <div class="flex flex-col gap-1">
            <Input
              id="consumer-name"
              bind:value={form.name}
              placeholder="Enter a unique name for your consumer"
              data-1p-ignore
              data-lpignore="true"
              data-form-type="other"
              disabled={isEditMode}
            />
            <p class="text-xs font-light">
              Must contain only alphanumeric characters, underscores, hyphens,
              or dots..
            </p>
          </div>
          {#if errors.name}
            <p class="text-destructive text-sm">{errors.name}</p>
          {/if}
        </div>
        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {:else if Object.keys(errors).length > 0}
          <p class="text-destructive text-sm">Validation errors, see above</p>
        {/if}
        <Button
          loading={isSubmitting}
          type="submit"
          disabled={isCreateConsumerDisabled}
        >
          {isEditMode ? "Update" : "Create"} consumer
          <span slot="loading"
            >{isEditMode ? "Updating..." : "Creating..."}</span
          >
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
