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
  import * as Dialog from "$lib/components/ui/dialog";
  import TableSelector from "../components/TableSelector.svelte";
  import TableFilters from "../components/TableFilters.svelte";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import HttpEndpointForm from "../http_endpoints/HttpEndpointForm.svelte";

  export let live;
  export let parent;
  export let http_push_consumer;
  export let databases;
  export let httpEndpoints;
  export let errors: any = {};
  export let submitError;

  let form = {
    messageKind: http_push_consumer.message_kind || "event",
    postgresDatabaseId: http_push_consumer.postgres_database_id,
    tableOid: http_push_consumer.table_oid,
    sourceTableFilters: http_push_consumer.source_table_filters || [],
    name: http_push_consumer.name || "",
    ackWaitMs: http_push_consumer.ack_wait_ms || 30000,
    maxAckPending: http_push_consumer.max_ack_pending || 10000,
    maxWaiting: http_push_consumer.max_waiting || 20,
    httpEndpointId: http_push_consumer.http_endpoint_id,
    httpEndpoint: {
      name: "",
      base_url: "",
      headers: {},
    },
  };

  console.log("form", form);

  const pushEvent = (event, payload = {}, cb = () => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  let selectedDatabase: any;
  let selectedTable: any;
  let selectedHttpEndpoint: any;

  $: {
    if (form.postgresDatabaseId && form.tableOid) {
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

  let showConfirmDialog = false;
  let dialogOpen = true;
  let showNewHttpEndpointForm = false;

  function handleConsumerSubmit() {
    pushEvent("form_submitted", { form });
  }

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
  }

  function confirmClose() {
    showConfirmDialog = false;
    dialogOpen = false;
    pushEvent("form_closed");
  }

  function cancelClose() {
    showConfirmDialog = false;
  }

  function toggleHttpEndpointForm() {
    showNewHttpEndpointForm = !showNewHttpEndpointForm;
  }
</script>

<Dialog.Root bind:open={dialogOpen} preventScroll={false}>
  <Dialog.Portal>
    <Dialog.Content
      closeButton={false}
      class="w-full h-full max-w-full max-h-full"
    >
      <div class="flex flex-col h-full">
        <div class="flex justify-between items-center p-6 border-b">
          <Dialog.Title class="text-2xl font-semibold"
            >Create an HTTP push consumer</Dialog.Title
          >
          <Dialog.Close asChild>
            <Button
              variant="outline"
              on:click={() => (showConfirmDialog = true)}
            >
              Exit
            </Button>
          </Dialog.Close>
        </div>

        <div class="flex-grow p-6 overflow-y-auto">
          <form
            on:submit|preventDefault={handleConsumerSubmit}
            class="space-y-6"
          >
            <div class="space-y-6">
              <h2 class="text-xl font-semibold">Source</h2>

              <div class="space-y-2">
                <label
                  for="message-kind"
                  class="block font-medium text-sm text-gray-700"
                  >Stream type</label
                >
                <Select
                  selected={{
                    value: form.messageKind,
                    label:
                      form.messageKind === "event"
                        ? "Change stream"
                        : "Sync stream",
                  }}
                  onSelectedChange={(event) => (form.messageKind = event.value)}
                >
                  <SelectTrigger class="w-full">
                    <SelectValue placeholder="Select stream type" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem class="cursor-pointer" value="event"
                      >Change stream</SelectItem
                    >
                    <SelectItem class="cursor-pointer" value="record"
                      >Sync stream</SelectItem
                    >
                  </SelectContent>
                </Select>
                <p class="text-sm text-gray-500 mt-1">
                  {#if form.messageKind === "event"}
                    A change stream will send you every create, update, and
                    delete that happens to rows and includes both new and old
                    values.
                  {:else if form.messageKind === "record"}
                    A sync stream will send you the latest version of rows
                    whenever they change and only includes new values, no
                    deletes.
                  {/if}
                </p>
                {#if errors.message_kind}
                  <p class="text-red-500 text-sm">{errors.message_kind}</p>
                {/if}
              </div>

              <div class="space-y-2">
                <label class="block font-medium text-sm text-gray-700"
                  >Source table</label
                >
                <TableSelector
                  {pushEvent}
                  {databases}
                  selectedDatabaseId={form.postgresDatabaseId}
                  selectedTableOid={form.tableOid}
                  onSelect={handleTableSelect}
                />
                {#if errors.postgres_database_id || errors.table_oid}
                  <p class="text-red-500 text-sm">
                    {errors.postgres_database_id || errors.table_oid}
                  </p>
                {/if}
              </div>

              <TableFilters
                filters={form.sourceTableFilters}
                columns={selectedTable ? selectedTable.columns : []}
                onFilterChange={handleFilterChange}
                disabled={!form.postgresDatabaseId && !form.tableOid}
              />
            </div>

            <div class="space-y-6">
              <h2 class="text-xl font-semibold">Push configuration</h2>

              <div class="space-y-2">
                <label
                  for="request-timeout"
                  class="block font-medium text-sm text-gray-700"
                  >Request timeout</label
                >
                <div class="flex items-center gap-2">
                  <Input
                    id="request-timeout"
                    type="number"
                    bind:value={form.ackWaitMs}
                    class="w-24"
                  />
                  <Select>
                    <SelectTrigger class="w-[180px]">
                      <SelectValue placeholder="Select unit" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="seconds">Seconds</SelectItem>
                      <SelectItem value="minutes">Minutes</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <p class="text-sm text-gray-500 mt-1">
                  The maximum duration allowed for the HTTP request to complete.
                  If the request doesn't finish within this time, it will be
                  considered failed and may be retried.
                </p>
                {#if errors.ack_wait_ms}
                  <p class="text-red-500 text-sm">{errors.ack_wait_ms}</p>
                {/if}
              </div>

              <Accordion class="w-full">
                <AccordionItem value="advanced">
                  <AccordionTrigger>Advanced configuration</AccordionTrigger>
                  <AccordionContent>
                    <div class="space-y-4 pt-4">
                      <div class="space-y-2">
                        <label
                          for="max-ack-pending"
                          class="block font-medium text-sm text-gray-700"
                          >Max ack pending</label
                        >
                        <Input
                          id="max-ack-pending"
                          type="number"
                          bind:value={form.maxAckPending}
                        />
                        <p class="text-sm text-gray-500 mt-1">
                          Sets the maximum number of messages that can be
                          pending acknowledgment at any time. This helps control
                          the flow of messages and prevents overwhelming the
                          consumer.
                        </p>
                        {#if errors.max_ack_pending}
                          <p class="text-red-500 text-sm">
                            {errors.max_ack_pending}
                          </p>
                        {/if}
                      </div>
                      <div class="space-y-2">
                        <label
                          for="max-waiting"
                          class="block font-medium text-sm text-gray-700"
                          >Max waiting</label
                        >
                        <Input
                          id="max-waiting"
                          type="number"
                          bind:value={form.maxWaiting}
                        />
                        <p class="text-sm text-gray-500 mt-1">
                          Determines the maximum number of waiting pull
                          requests. This setting helps manage resource
                          utilization and prevents excessive polling.
                        </p>
                        {#if errors.max_waiting}
                          <p class="text-red-500 text-sm">
                            {errors.max_waiting}
                          </p>
                        {/if}
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            </div>

            <div class="space-y-6">
              <h2 class="text-xl font-semibold">HTTP Endpoint</h2>

              <div class="space-y-2">
                <label
                  for="http-endpoint"
                  class="block font-medium text-sm text-gray-700"
                  >HTTP Endpoint</label
                >
                <Select
                  selected={{
                    value: form.httpEndpointId,
                    label: selectedHttpEndpoint?.name || "Select an endpoint",
                  }}
                  onSelectedChange={(event) => {
                    if (event.value === "new") {
                      form.httpEndpointId = null;
                      toggleHttpEndpointForm();
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
                      <SelectItem value={endpoint.id}
                        >{endpoint.name}</SelectItem
                      >
                    {/each}
                    <SelectItem value="new">+ Add new</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {#if showNewHttpEndpointForm}
                <HttpEndpointForm
                  formData={form.httpEndpoint}
                  formErrors={errors.httpEndpoint || {}}
                />
              {/if}
            </div>
            <div class="space-y-6">
              <h2 class="text-xl font-semibold">Consumer</h2>

              <div class="space-y-2">
                <label
                  for="name"
                  class="block font-medium text-sm text-gray-700">Name</label
                >
                <Input
                  id="name"
                  bind:value={form.name}
                  placeholder="Enter a unique name for your consumer"
                  data-1p-ignore
                  data-lpignore="true"
                  data-form-type="other"
                />
                {#if errors.name}
                  <p class="text-red-500 text-sm">{errors.name}</p>
                {/if}
              </div>

              <Button type="submit">Create Consumer</Button>
              {#if submitError}
                <p class="text-red-500 text-sm mt-2">{submitError}</p>
              {/if}
            </div>
          </form>
        </div>
      </div>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>

<AlertDialog.Root open={showConfirmDialog}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>Are you sure you want to exit?</AlertDialog.Title>
      <AlertDialog.Description>
        Your changes will be lost if you exit without saving.
      </AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel on:click={cancelClose}>Cancel</AlertDialog.Cancel>
      <AlertDialog.Action on:click={confirmClose}>Exit</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
