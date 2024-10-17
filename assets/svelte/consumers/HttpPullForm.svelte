<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
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
  import SequenceSelector from "../components/SequenceSelector.svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { cn } from "$lib/utils";
  import { ExternalLinkIcon } from "lucide-svelte";
  import FilterForm from "../components/FilterForm.svelte";

  export let live;
  export let parent;
  export let consumer;
  export let databases;
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
    sortColumnAttnum: consumer.sort_column_attnum || null,
    recordConsumerState: consumer.record_consumer_state || {
      producer: "table_and_wal",
      initialMinSortCol: null,
    },
    sequenceId: consumer.sequence_id || null,
  };

  let form = { ...initialForm };
  let isDirty = false;
  let isSubmitting = false;

  $: selectedTable =
    selectedSequence && selectedDatabase
      ? selectedDatabase.tables.find(
          (table) => table.oid === selectedSequence.table_oid
        )
      : null;

  $: {
    isDirty = JSON.stringify(form) !== JSON.stringify(initialForm);
    pushEvent("form_updated", { form });
  }

  const pushEvent = (event, payload = {}, cb = (event: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  let selectedDatabase: any;
  let selectedSequence: any;

  $: {
    if (form.postgresDatabaseId && form.sequenceId) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId
      );
      if (selectedDatabase) {
        selectedSequence = selectedDatabase.sequences.find(
          (sequence) => sequence.id === form.sequenceId
        );
      }
    }
  }

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

    // Set the form name based on the selected table
    if (form.sequenceId) {
      const selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId
      );
      if (selectedDatabase) {
        const selectedSequence = selectedDatabase.sequences.find(
          (sequence) => sequence.id === form.sequenceId
        );
        if (selectedSequence) {
          const tableName = selectedSequence.table_name;
          const newName = `${tableName}_pull_consumer`;
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

  const isEditMode = !!consumer.id;
</script>

<FullPageModal
  title={isEditMode
    ? "Edit an HTTP pull consumer"
    : "Create an HTTP pull consumer"}
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
          <Label>Sequence</Label>
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
                label: selectedSequence.table_name,
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
          bind:form
          {errors}
          {isEditMode}
          onFilterChange={handleFilterChange}
          showTitle={false}
        />
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Pull configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="visibility-timeout">Visibility timeout</Label>
          <div class="flex items-center space-x-2">
            <Input
              id="visibility-timeout"
              type="number"
              bind:value={form.ackWaitMs}
              class="w-24"
            />
            <span class="text-sm text-muted-foreground">ms</span>
          </div>
          <p class="text-sm text-muted-foreground">
            The duration that a message is hidden from other consumers after
            it's been received. This prevents multiple consumers from processing
            the same message simultaneously. If the message isn't processed
            within this time, it becomes visible to other consumers again.
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
                <div class="space-y-2">
                  <Label for="max-waiting">Max waiting</Label>
                  <Input
                    id="max-waiting"
                    type="number"
                    bind:value={form.maxWaiting}
                  />
                  <p class="text-sm text-muted-foreground">
                    Determines the maximum number of waiting pull requests. This
                    setting helps manage resource utilization and prevents
                    excessive polling.
                  </p>
                  {#if errors.max_waiting}
                    <p class="text-destructive text-sm">
                      {errors.max_waiting}
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
              or dots.
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
