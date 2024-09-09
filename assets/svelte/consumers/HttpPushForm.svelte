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
  import TableFilters from "../components/TableFilters.svelte";
  import HttpEndpointForm from "../http_endpoints/FormBody.svelte";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { cn } from "$lib/utils";
  import { toast } from "svelte-sonner";
  import { ExternalLinkIcon, Loader2 } from "lucide-svelte";

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
    httpEndpoint: {
      name: "",
      baseUrl: "",
      headers: {},
      encryptedHeaders: {},
    },
  };

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
  let showNewHttpEndpointForm = false;
  let isGeneratingWebhookSite = false;

  function handleConsumerSubmit() {
    pushEvent("form_submitted", { form });
  }

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
    // Trigger a form update to refresh error messages
    pushEvent("form_updated", { form });
  }

  function handleClose() {
    pushEvent("form_closed");
  }

  function createWebhookSiteEndpoint() {
    isGeneratingWebhookSite = true;
    pushEvent("generate_webhook_site_url", {}, (result: any) => {
      isGeneratingWebhookSite = false;
      if (result.url && result.name) {
        form.httpEndpoint = {
          name: result.name,
          baseUrl: result.url,
          headers: {},
          encryptedHeaders: {},
        };
        showNewHttpEndpointForm = true;
      } else if (result.error) {
        toast.error("Failed to generate Webhook.site URL:", result.error);
      } else {
        toast.error("Failed to generate Webhook.site URL");
      }
    });
  }

  $: isCreateConsumerDisabled = !form.postgresDatabaseId || !form.tableOid;
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

    <Card>
      <CardHeader>
        <CardTitle>Filters</CardTitle>
      </CardHeader>
      <CardContent>
        {#if form.messageKind === "event"}
          {@const actions = form.sourceTableActions || []}
          {@const switches = [
            { id: "insert", label: "Insert" },
            { id: "update", label: "Update" },
            { id: "delete", label: "Delete" },
          ]}
          <div class="space-y-2">
            <Label>Operations to capture</Label>
            <div class="flex items-center space-x-4">
              {#each switches as { id, label }}
                <div class="flex items-center space-x-2">
                  <Label for={id} class="cursor-pointer">
                    {label}
                  </Label>
                  <Switch
                    {id}
                    disabled={!form.postgresDatabaseId && !form.tableOid}
                    checked={actions.includes(id)}
                    onCheckedChange={(checked) => {
                      const newActions = checked
                        ? [...actions, id]
                        : actions.filter((a) => a !== id);
                      form.sourceTableActions = newActions;
                    }}
                  />
                </div>
              {/each}
            </div>
            {#if errors.source_tables?.[1]?.actions}
              <p class="text-destructive text-sm">
                {errors.source_tables[1].actions}
              </p>
            {/if}
          </div>
        {/if}
        <div class="my-6">
          <TableFilters
            filters={form.sourceTableFilters}
            columns={selectedTable ? selectedTable.columns : []}
            onFilterChange={handleFilterChange}
            disabled={!form.postgresDatabaseId && !form.tableOid}
            errors={errors.source_tables?.[0]?.column_filters || []}
          />
        </div>
      </CardContent>
    </Card>

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
        {#if !form.httpEndpointId && !showNewHttpEndpointForm}
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
        {#if isEditMode}
          <Select
            disabled
            selected={{
              value: form.httpEndpointId,
              label:
                selectedHttpEndpoint?.name ||
                (showNewHttpEndpointForm ? "+ Add new" : "Select an endpoint"),
            }}
          >
            <SelectTrigger
              class={cn("w-full", "bg-muted text-muted-foreground opacity-100")}
            >
              <SelectValue placeholder="Selected HTTP endpoint" />
            </SelectTrigger>
          </Select>
        {:else}
          <Select
            selected={{
              value: form.httpEndpointId,
              label: selectedHttpEndpoint?.name || "Select an endpoint",
            }}
            onSelectedChange={(event) => {
              if (event.value === "new") {
                form.httpEndpointId = null;
                showNewHttpEndpointForm = true;
              } else {
                form.httpEndpointId = event.value;
              }
            }}
          >
            <SelectTrigger class="w-full">
              <SelectValue placeholder="Select an endpoint" />
            </SelectTrigger>
            <SelectContent>
              {#each httpEndpoints as endpoint}
                <SelectItem value={endpoint.id}>{endpoint.name}</SelectItem>
              {/each}
              <SelectItem value="new">+ Add new</SelectItem>
            </SelectContent>
          </Select>
        {/if}

        {#if showNewHttpEndpointForm}
          <HttpEndpointForm
            bind:form={form.httpEndpoint}
            errors={errors.http_endpoint || {}}
          />
        {:else if errors.http_endpoint_id || errors.http_endpoint}
          <p class="text-destructive text-sm">
            Please select or create an HTTP endpoint
          </p>
        {/if}

        <div class="space-y-2">
          <Label for="http-endpoint-path">Consumer Endpoint Path</Label>
          <Input
            id="http-endpoint-path"
            bind:value={form.httpEndpointPath}
            placeholder="/webhook"
          />
          <p class="text-sm text-muted-foreground">
            The path to append to the base URL for this consumer's requests.
          </p>
          {#if errors.http_endpoint_path}
            <p class="text-destructive text-sm">{errors.http_endpoint_path}</p>
          {/if}
        </div>

        {#if form.httpEndpointId || form.httpEndpoint.baseUrl}
          <div class="mt-4">
            <Label>Fully Qualified URL</Label>
            <p
              class="w-fit font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md"
            >
              {(form.httpEndpointId
                ? selectedHttpEndpoint?.baseUrl
                : form.httpEndpoint.baseUrl
              ).replace(/\/$/, "")}/{form.httpEndpointPath.replace(/^\//, "")}
            </p>
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
          <Input
            id="consumer-name"
            bind:value={form.name}
            placeholder="Enter a unique name for your consumer"
            data-1p-ignore
            data-lpignore="true"
            data-form-type="other"
            disabled={isEditMode}
          />
          {#if errors.name}
            <p class="text-destructive text-sm">{errors.name}</p>
          {/if}
        </div>

        <Button type="submit" disabled={isCreateConsumerDisabled}
          >{isEditMode ? "Update" : "Create"} consumer</Button
        >
        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {/if}
      </CardContent>
    </Card>
  </form>
</FullPageModal>
