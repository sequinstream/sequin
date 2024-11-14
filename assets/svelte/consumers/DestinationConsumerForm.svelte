<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import {
    Select,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import SequenceSelector from "../components/SequenceSelector.svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { cn } from "$lib/utils";
  import FilterForm from "../components/FilterForm.svelte";
  import GroupColumnsForm from "./GroupColumnsForm.svelte";
  import DestinationHttpPushForm from "./DestinationHttpPushForm.svelte";

  export let live;
  export let parent;
  export let consumer;
  export let databases;
  export let httpEndpoints;
  export let errors: any = {};
  export let submitError;

  let initialForm = {
    messageKind: consumer.message_kind || "record",
    postgresDatabaseId: consumer.postgres_database_id,
    tableOid: consumer.table_oid,
    sourceTableFilters: consumer.source_table_filters || [],
    sourceTableActions: consumer.source_table_actions || [],
    name: consumer.name || "",
    ackWaitMs: consumer.ack_wait_ms || 30000,
    maxAckPending: consumer.max_ack_pending || 10000,
    maxWaiting: consumer.max_waiting || 20,
    httpEndpointId: consumer.http_endpoint_id,
    httpEndpointPath: consumer.http_endpoint_path || "",
    sortColumnAttnum: consumer.sort_column_attnum || null,
    recordConsumerState: consumer.record_consumer_state || {
      initialMinSortCol: null,
    },
    sequenceId: consumer.sequence_id || null,
    groupColumnAttnums: consumer.group_column_attnums || [],
    batchSize: consumer.batch_size || 1,
  };

  let form = { ...initialForm };
  let isDirty = false;
  let isSubmitting = false;

  $: {
    isDirty = JSON.stringify(form) !== JSON.stringify(initialForm);
    pushEvent("form_updated", { form });
  }

  const pushEvent = (event, payload = {}, cb = (result: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  let selectedDatabase: any;
  let selectedSequence: any;
  let selectedTable: any;

  $: {
    if (form.postgresDatabaseId) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId,
      );
    }

    if (
      selectedDatabase &&
      !form.sequenceId &&
      selectedDatabase.sequences.length > 0
    ) {
      form.sequenceId = selectedDatabase.sequences[0].id;
    }

    if (form.sequenceId && selectedDatabase) {
      selectedSequence = selectedDatabase.sequences.find(
        (sequence) => sequence.id === form.sequenceId,
      );
    }

    selectedTable =
      selectedSequence && selectedDatabase
        ? selectedDatabase.tables.find(
            (table) => table.oid === selectedSequence.table_oid,
          )
        : null;
  }

  const isEditMode = !!consumer.id;
  let dialogOpen = true;
  let showConfirmDialog = false;

  function handleConsumerSubmit() {
    isSubmitting = true;
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        isSubmitting = false;
      }
    });
  }

  function handleSequenceSelect(event: {
    databaseId: string;
    sequenceId: string;
  }) {
    form.postgresDatabaseId = event.databaseId;
    form.sequenceId = event.sequenceId;

    // Set the form name based on the selected sequence
    if (form.sequenceId) {
      const selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId,
      );
      if (selectedDatabase) {
        const selectedSequence = selectedDatabase.sequences.find(
          (sequence) => sequence.id === form.sequenceId,
        );
        if (selectedSequence) {
          const tableName = selectedSequence.table_name;
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

  $: isCreateConsumerDisabled = !form.postgresDatabaseId || !form.sequenceId;
</script>

<FullPageModal
  title={isEditMode
    ? "Edit Webhook Subscription"
    : "Create Webhook Subscription"}
  bind:open={dialogOpen}
  bind:showConfirmDialog
  showConfirmOnExit={isDirty}
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
                  "bg-muted text-muted-foreground opacity-100",
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
                  "bg-muted text-muted-foreground opacity-100",
                )}
              >
                <SelectValue placeholder="Selected table" />
              </SelectTrigger>
            </Select>
          {:else}
            <SequenceSelector
              {pushEvent}
              {databases}
              selectedDatabaseId={form.postgresDatabaseId}
              selectedSequenceId={form.sequenceId}
              onSelect={handleSequenceSelect}
            />
          {/if}
          {#if errors.postgres_database_id || errors.table_oid}
            <p class="text-destructive text-sm">
              {errors.postgres_database_id || errors.table_oid}
            </p>
          {/if}
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>
          {#if form.messageKind === "record"}
            Records to process
          {:else}
            Changes to process
          {/if}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <FilterForm
          messageKind={form.messageKind}
          {selectedTable}
          sortColumnName={selectedSequence?.sort_column_name}
          sortColumnType={selectedSequence?.sort_column_type}
          bind:form
          {errors}
          {isEditMode}
          onFilterChange={handleFilterChange}
          showTitle={false}
        />
      </CardContent>
    </Card>

    <GroupColumnsForm
      {errors}
      {isEditMode}
      {selectedTable}
      bind:groupColumnAttnums={form.groupColumnAttnums}
    />

    <DestinationHttpPushForm
      {errors}
      {httpEndpoints}
      bind:form
      {live}
      {parent}
    />

    <Card>
      <CardHeader>
        <CardTitle>Webhook Subscription name</CardTitle>
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
          {isEditMode ? "Update" : "Create"} Webhook Subscription
          <span slot="loading"
            >{isEditMode ? "Updating..." : "Creating..."}</span
          >
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
