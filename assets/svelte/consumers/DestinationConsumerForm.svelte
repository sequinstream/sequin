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
  import DestinationSqsForm from "./DestinationSqsForm.svelte";
  import DestinationRedisForm from "./DestinationRedisForm.svelte";
  import DestinationKafkaForm from "./DestinationKafkaForm.svelte";
  import { CircleAlert } from "lucide-svelte";
  import * as Alert from "$lib/components/ui/alert/index.js";

  export let live;
  export let parent;
  export let consumer;
  export let consumerTitle;
  export let databases;
  export let httpEndpoints;
  export let errors: any = {};
  export let submitError;

  let initialForm = {
    type: consumer.type,
    messageKind: consumer.message_kind || "record",
    postgresDatabaseId: consumer.postgres_database_id,
    tableOid: consumer.table_oid,
    sourceTableFilters: consumer.source_table_filters || [],
    sourceTableActions: consumer.source_table_actions || [],
    name: consumer.name || "",
    ackWaitMs: consumer.ack_wait_ms || 30000,
    maxAckPending: consumer.max_ack_pending || 10000,
    maxWaiting: consumer.max_waiting || 20,
    destination: consumer.destination,
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

  type TestConnectionState = {
    status: "initial" | "loading" | "success" | "error";
    displayStatus?: boolean;
    error?: string;
    lastTestStatus?: "success" | "error" | "none";
  };

  let testConnectionState: TestConnectionState = {
    status: "initial",
    lastTestStatus: "none",
  };

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

  function onTestConnection() {
    testConnectionState = {
      status: "loading",
      lastTestStatus: testConnectionState.lastTestStatus,
    };

    pushEvent("test_connection", {}, (reply) => {
      if (reply.ok) {
        testConnectionState = {
          displayStatus: true,
          status: "success",
          lastTestStatus: "success",
        };
        setTimeout(() => {
          testConnectionState = {
            status: "initial",
            lastTestStatus: "success",
          };
        }, 3000);
      } else {
        testConnectionState = {
          displayStatus: true,
          status: "error",
          error: reply.error,
          lastTestStatus: "error",
        };
        setTimeout(() => {
          testConnectionState.displayStatus = false;
          testConnectionState.status = "initial";
        }, 3000);
      }
    });
  }
</script>

<FullPageModal
  title={isEditMode ? `Edit ${consumerTitle}` : `Create ${consumerTitle}`}
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

    {#if consumer.type === "http_push"}
      <DestinationHttpPushForm
        {errors}
        {httpEndpoints}
        bind:form
        {live}
        {parent}
      />
    {:else if consumer.type === "sqs"}
      <DestinationSqsForm {errors} bind:form />
    {:else if consumer.type === "redis"}
      <DestinationRedisForm {errors} bind:form />
    {:else if consumer.type === "kafka"}
      <DestinationKafkaForm {errors} bind:form />
    {/if}

    <Card>
      <CardHeader>
        <CardTitle>Destination Consumer name</CardTitle>
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
        <div class="flex justify-end items-center gap-2">
          {#if consumer.type === "sqs" || consumer.type === "redis"}
            <Button
              loading={testConnectionState.status === "loading"}
              type="button"
              variant="outline"
              class="self-end"
              on:click={onTestConnection}
            >
              {#if testConnectionState.status === "success" && testConnectionState.displayStatus}
                <span
                  class="flex items-center p-1 gap-1 mr-2 bg-green-500 rounded-full"
                ></span>
                Connection succeeded
              {:else if testConnectionState.status === "error" && testConnectionState.displayStatus}
                <span
                  class="flex items-center p-1 gap-1 mr-2 bg-red-500 rounded-full"
                ></span>
                Connection failed
              {:else}
                <span
                  class="flex items-center w-2 h-2 mr-2 rounded-full"
                  class:bg-green-500={testConnectionState.lastTestStatus ===
                    "success"}
                  class:bg-red-500={testConnectionState.lastTestStatus ===
                    "error"}
                  class:bg-gray-300={testConnectionState.lastTestStatus ===
                    "none"}
                ></span>
                Test Connection
              {/if}
            </Button>
          {/if}
          <Button
            loading={isSubmitting}
            type="submit"
            disabled={isCreateConsumerDisabled}
          >
            {isEditMode ? "Update" : "Create"} Destination Consumer
            <span slot="loading"
              >{isEditMode ? "Updating..." : "Creating..."}</span
            >
          </Button>
        </div>
        {#if testConnectionState.lastTestStatus === "error"}
          <Alert.Root variant="destructive">
            <CircleAlert class="h-4 w-4" />
            <Alert.Title>Connection test failed</Alert.Title>
            <Alert.Description>
              {#if typeof testConnectionState.error === "object"}
                <ul
                  class="mt-4"
                  style="font-family: monospace; white-space: pre-wrap; list-style-type: disc; padding-left: 20px;"
                >
                  {#each Object.entries(testConnectionState.error) as [key, value]}
                    <li>{key}: {value}</li>
                  {/each}
                </ul>
              {:else}
                <pre
                  class="mt-4"
                  style="white-space: pre-wrap; word-wrap: break-word;"><code
                    >{testConnectionState.error}</code
                  >
              </pre>
              {/if}
            </Alert.Description>
          </Alert.Root>
        {/if}
      </CardContent>
    </Card>
  </form>
</FullPageModal>
