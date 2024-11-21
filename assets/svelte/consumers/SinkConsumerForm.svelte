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
  import SinkHttpPushForm from "$lib/consumers/SinkHttpPushForm.svelte";
  import SinkSqsForm from "$lib/consumers/SinkSqsForm.svelte";
  import SinkRedisForm from "$lib/consumers/SinkRedisForm.svelte";
  import SinkKafkaForm from "$lib/consumers/SinkKafkaForm.svelte";
  import SinkSequinStreamForm from "$lib/consumers/SinkSequinStreamForm.svelte";
  import { CircleAlert, ExternalLinkIcon, Info } from "lucide-svelte";
  import * as Alert from "$lib/components/ui/alert/index.js";
  import TableSelector from "../components/TableSelector.svelte";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import Datetime from "../components/Datetime.svelte";
  import { Switch } from "$lib/components/ui/switch";
  import * as Popover from "$lib/components/ui/popover";
  import * as Dialog from "$lib/components/ui/dialog";
  import * as Tabs from "$lib/components/ui/tabs";

  type Column = {
    attnum: number;
    isPk?: boolean;
    name: string;
    type: string;
    filterType: string;
  };

  type Table = {
    oid: number;
    schema: string;
    name: string;
    default_group_columns: number[];
    is_event_table: boolean;
    sort_column?: {
      name: string;
      type: string;
      attnum: number;
    };
    columns: Column[];
  };

  type Database = {
    id: string;
    name: string;
    tables: Table[];
  };

  export let live;
  export let parent;
  export let consumer;
  export let consumerTitle;
  export let httpEndpoints;
  export let databases: Database[];
  export let errors: {
    consumer: Record<string, string>;
    sequence: Record<string, string>;
  };
  export let submitError;

  let initialForm = {
    type: consumer.type,
    messageKind: consumer.message_kind || "event",
    postgresDatabaseId: consumer.postgres_database_id,
    tableOid: consumer.table_oid,
    sortColumnAttnum: null,
    sourceTableFilters: consumer.source_table_filters || [],
    sourceTableActions: consumer.source_table_actions || [],
    name: consumer.name || "",
    ackWaitMs: consumer.ack_wait_ms || 30000,
    maxAckPending: consumer.max_ack_pending || 10000,
    maxWaiting: consumer.max_waiting || 20,
    sink: consumer.sink,
    initialBackfill: {
      enabled: false,
      startPosition: "beginning",
      initialMinSortCol: null,
    },
    groupColumnAttnums: consumer.group_column_attnums || [],
    batchSize: consumer.batch_size || 1,
  };

  let form = { ...initialForm };
  let isDirty = false;
  let isSubmitting = false;
  let sortColumnName = null;

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

  let selectedDatabase: any;
  let selectedTable: any;

  $: {
    if (form.postgresDatabaseId) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId,
      );
    }

    if (form.tableOid && selectedDatabase) {
      selectedTable = selectedDatabase.tables.find(
        (table) => table.oid === form.tableOid,
      );
    }

    if (selectedTable) {
      form.sortColumnAttnum =
        selectedTable.sort_column?.attnum || form.sortColumnAttnum;

      sortColumnName = selectedTable.columns.find(
        (column) => column.attnum === form.sortColumnAttnum,
      )?.name;

      // Force message kind to "record" for event tables
      if (selectedTable.is_event_table) {
        form.messageKind = "record";
      }
    }
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

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    if (form.tableOid !== event.tableOid) {
      form.sortColumnAttnum = null;
      form.messageKind = "event";
    }

    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;

    // Set the form name based on the selected table
    if (form.tableOid) {
      const selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId,
      );
      if (selectedDatabase) {
        const selectedTable = selectedDatabase.tables.find(
          (table) => table.oid === form.tableOid,
        );
        if (selectedTable) {
          const tableName = selectedTable.name;
          const newName = `${tableName}_sink`;
          form.name = newName;
        }
      }
    }
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
  }

  function handleClose() {
    pushEvent("form_closed");
  }

  $: isCreateConsumerDisabled = !form.postgresDatabaseId || !form.tableOid;

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

  let startPosition = "beginning";
  let minSortColError: string = "";
  let enableBackfill = false;

  $: {
    if (!enableBackfill) {
      form.initialBackfill = {
        enabled: false,
        startPosition: "beginning",
        initialMinSortCol: null,
      };
    } else if (startPosition === "beginning") {
      form.initialBackfill = {
        enabled: true,
        startPosition: "beginning",
        initialMinSortCol: null,
      };
    } else if (startPosition === "specific") {
      form.initialBackfill = {
        enabled: true,
        startPosition: "specific",
        initialMinSortCol: form.initialBackfill.initialMinSortCol,
      };
    }
  }
  let showExampleModal = false;
  let selectedExampleType: "change" | "record" = "change";

  const recordExample = {
    record: {
      id: 1,
      name: "John Smith",
      title: "Senior Developer",
      salary: 120000,
      is_manager: false,
    },
    metadata: {
      table_schema: "public",
      table_name: "employees",
      consumer: {
        id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        name: "employee_updates_consumer",
      },
    },
  };

  const changeExamples = {
    insert: {
      record: {
        id: 2,
        name: "Sarah Johnson",
        title: "Product Manager",
        salary: 135000,
        is_manager: true,
      },
      changes: null,
      action: "insert",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        commit_timestamp: "2023-10-15T14:30:00Z",
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "hr_updates_consumer",
        },
      },
    },
    update: {
      record: {
        id: 1,
        name: "John Smith",
        title: "Engineering Manager",
        salary: 150000,
        is_manager: true,
      },
      changes: {
        title: "Senior Developer",
        salary: 120000,
        is_manager: false,
      },
      action: "update",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        commit_timestamp: "2023-10-16T09:45:00Z",
        consumer: {
          id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
          name: "employee_updates_consumer",
        },
      },
    },
    delete: {
      record: {
        id: 3,
        name: "Michael Brown",
        title: "Software Engineer",
        salary: 110000,
        is_manager: false,
      },
      changes: null,
      action: "delete",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        commit_timestamp: "2023-10-17T18:20:00Z",
        consumer: {
          id: "a1b2c3d4-e5f6-4a5b-8c7d-9e0f1a2b3c4d",
          name: "employee_departures_consumer",
        },
      },
    },
    read: {
      record: {
        id: 2,
        name: "Sarah Johnson",
        title: "Product Manager",
        salary: 135000,
        is_manager: true,
      },
      changes: null,
      action: "read",
      metadata: {
        table_schema: "public",
        table_name: "employees",
        commit_timestamp: "2023-10-15T14:30:00Z",
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "hr_updates_consumer",
        },
      },
    },
  };
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
            <TableSelector
              {databases}
              onSelect={handleTableSelect}
              {pushEvent}
              selectedDatabaseId={form.postgresDatabaseId}
              selectedTableOid={form.tableOid}
              showSortColumn={true}
            />

            {#if errors.consumer.postgres_database_id}
              <p class="text-destructive text-sm">
                {errors.consumer.postgres_database_id}
              </p>
            {/if}

            {#if errors.consumer.table_oid}
              <p class="text-destructive text-sm">
                {errors.consumer.table_oid}
              </p>
            {/if}

            {#if selectedTable && selectedTable.sort_column === null}
              <div class="space-y-2">
                <Label for="sort_column_attnum">Sort and start</Label>
                <p class="text-sm text-muted-foreground mt-1 mb-2">
                  Select the sort column for the table. Your system should
                  update the sort column whenever a row is updated. A good
                  example of a sort column is <code>updated_at</code>.
                  <a
                    href="https://sequinstream.com/docs/how-sequin-works#creating-a-table-stream"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="inline-flex items-center text-link hover:underline"
                  >
                    Learn more
                    <ExternalLinkIcon class="w-3 h-3 ml-1" />
                  </a>
                </p>
                <Select
                  selected={{
                    value: form.sortColumnAttnum,
                    label: sortColumnName || "Select a sort column",
                  }}
                  onSelectedChange={(event) => {
                    form.sortColumnAttnum = event.value;
                  }}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Select a sort column" />
                  </SelectTrigger>
                  <SelectContent>
                    {#each selectedTable.columns as column}
                      <SelectItem value={column.attnum}
                        >{column.name}</SelectItem
                      >
                    {/each}
                  </SelectContent>
                </Select>
                {#if errors.sequence.sort_column_attnum}
                  <p class="text-destructive text-sm">
                    {errors.sequence.sort_column_attnum}
                  </p>
                {/if}
              </div>
            {/if}
          {/if}
          {#if selectedTable}
            <div class="space-y-2">
              <Label for="message_kind">Message type</Label>
              <p class="text-sm text-muted-foreground mt-1 mb-2">
                Select the kind of messages you want to process.
                <!-- <a
                  href="https://sequinstream.com/docs/reference/messages"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex items-center text-link hover:underline"
                >
                  Learn more
                  <ExternalLinkIcon class="w-3 h-3 ml-1" />
                </a> -->
                <button
                  type="button"
                  class="text-muted-foreground underline decoration-dotted"
                  on:click={() => (showExampleModal = true)}
                >
                  See examples
                </button>
              </p>
              <Select
                selected={{
                  value: form.messageKind,
                  label: form.messageKind === "record" ? "Records" : "Changes",
                }}
                onSelectedChange={(event) => {
                  form.messageKind = event.value;
                }}
                disabled={selectedTable.is_event_table || isEditMode}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select a message type" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="event">Changes</SelectItem>
                  <SelectItem value="record">Records</SelectItem>
                </SelectContent>
              </Select>
              {#if selectedTable.is_event_table}
                <p class="text-muted-foreground text-xs">
                  Sequin automatically sets the message type to "Records" for
                  event tables.
                </p>
              {/if}
              {#if errors.consumer.message_kind}
                <p class="text-destructive text-sm">
                  {errors.consumer.message_kind}
                </p>
              {/if}
            </div>
          {/if}
          {#if errors.consumer.postgres_database_id || errors.consumer.table_oid}
            <p class="text-destructive text-sm">
              {errors.consumer.postgres_database_id ||
                errors.consumer.table_oid}
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
        <FilterForm
          messageKind={form.messageKind}
          {selectedTable}
          bind:form
          {errors}
          onFilterChange={handleFilterChange}
          showTitle={false}
        />
      </CardContent>
    </Card>

    {#if !isEditMode}
      <Card>
        <CardHeader class="flex flex-row items-center justify-between">
          <CardTitle class="flex items-center gap-2"
            >Initial backfill
            <Popover.Root>
              <Popover.Trigger asChild let:builder>
                <Button
                  builders={[builder]}
                  variant="link"
                  class="text-muted-foreground hover:text-foreground p-0"
                >
                  <Info class="h-4 w-4" />
                </Button>
              </Popover.Trigger>
              <Popover.Content class="w-80">
                <div class="grid gap-4">
                  <div class="space-y-2">
                    <p class="text-sm text-muted-foreground font-normal">
                      Sequin will run an initial <a
                        href="https://sequinstream.com/docs/reference/backfills"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="text-primary underline"
                      >
                        backfill
                      </a> of data from the selected table to the sink destination.
                    </p>
                  </div>
                </div>
              </Popover.Content>
            </Popover.Root>
          </CardTitle>
          <Switch
            bind:checked={enableBackfill}
            disabled={isEditMode || !selectedTable}
          />
        </CardHeader>
        <CardContent>
          <div class="space-y-2">
            <div class="text-sm text-muted-foreground">
              {#if !enableBackfill}
                No initial backfill. You can run backfills at any time in the
                future.
              {/if}
            </div>

            {#if enableBackfill}
              <RadioGroup
                bind:value={startPosition}
                disabled={isEditMode || !selectedTable}
              >
                <div class="flex items-center space-x-2">
                  <RadioGroupItem value="beginning" id="beginning" />
                  <Label for="beginning">From the beginning</Label>
                </div>
                <div class="flex items-center space-x-2">
                  <RadioGroupItem value="specific" id="specific" />
                  <Label for="specific">From a specific point</Label>
                </div>
              </RadioGroup>

              {#if startPosition === "specific"}
                <div
                  class="grid grid-cols-[auto_1fr] gap-4 content-center mt-4"
                >
                  <div class="flex items-center space-x-2 text-sm font-mono">
                    <span class="bg-secondary-2xSubtle px-2 py-1 rounded"
                      >{selectedTable?.sort_column?.name}</span
                    >
                    <span class="bg-secondary-2xSubtle px-2 py-1 rounded"
                      >&gt;=</span
                    >
                  </div>

                  {#if selectedTable?.sort_column?.type.startsWith("timestamp")}
                    <Datetime
                      bind:value={form.initialBackfill.initialMinSortCol}
                      bind:error={minSortColError}
                    />
                    {#if minSortColError}
                      <p class="text-sm text-red-500 mt-2">{minSortColError}</p>
                    {/if}
                  {:else if ["integer", "bigint", "smallint", "serial"].includes(selectedTable?.sort_column?.type)}
                    <Input
                      type="number"
                      bind:value={form.initialBackfill.initialMinSortCol}
                    />
                  {:else}
                    <Input
                      type="text"
                      bind:value={form.initialBackfill.initialMinSortCol}
                    />
                  {/if}
                </div>
              {/if}
            {/if}
          </div>
        </CardContent>
      </Card>
    {/if}

    <GroupColumnsForm
      errors={errors.consumer}
      {isEditMode}
      {selectedTable}
      bind:groupColumnAttnums={form.groupColumnAttnums}
    />

    {#if consumer.type === "http_push"}
      <SinkHttpPushForm
        errors={errors.consumer}
        {httpEndpoints}
        bind:form
        {live}
        {parent}
      />
    {:else if consumer.type === "sqs"}
      <SinkSqsForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "redis"}
      <SinkRedisForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "kafka"}
      <SinkKafkaForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "sequin_stream"}
      <SinkSequinStreamForm errors={errors.consumer} bind:form />
    {/if}

    <Card>
      <CardHeader>
        <CardTitle>Sink name</CardTitle>
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
          {#if errors.consumer.name}
            <p class="text-destructive text-sm">{errors.consumer.name}</p>
          {/if}
        </div>
        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {:else if Object.keys(errors.consumer).length > 0 || Object.keys(errors.sequence).length > 0}
          <p class="text-destructive text-sm">Validation errors, see above</p>
        {/if}
        <div class="flex justify-end items-center gap-2">
          {#if consumer.type === "sqs" || consumer.type === "redis" || consumer.type === "kafka"}
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
            {isEditMode ? "Update" : "Create"} Sink
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

<Dialog.Root bind:open={showExampleModal}>
  <Dialog.Portal>
    <Dialog.Overlay />
    <Dialog.Content class="max-w-3xl">
      <Dialog.Header class="mb-4">
        <Dialog.Title>Message examples</Dialog.Title>
      </Dialog.Header>

      <div class="space-y-2">
        <div class="mb-4">
          <Select
            selected={{
              value: selectedExampleType,
              label:
                selectedExampleType === "record"
                  ? "Row Messages"
                  : "Change Messages",
            }}
            onSelectedChange={(event) => {
              selectedExampleType = event.value;
            }}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select example type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="record">Row Messages</SelectItem>
              <SelectItem value="change">Change Messages</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {#if selectedExampleType === "record"}
          <div class="space-y-2">
            <p>
              A row message contains the latest version of a row. Sequin sends a
              row message whenever a row is inserted, updated, or backfilled.
            </p>
            <p>
              In contrast to change messages, row messages do not contain
              <code>changes</code> or <code>action</code> fields.
            </p>
            <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
                >{JSON.stringify(recordExample, null, 2).trim()}</code
              ></pre>
          </div>
        {:else}
          <Tabs.Root value="insert" class="w-full mb-4">
            <Tabs.List class="grid grid-cols-4 mb-4">
              <Tabs.Trigger value="insert" class="w-full">Insert</Tabs.Trigger>
              <Tabs.Trigger value="update" class="w-full">Update</Tabs.Trigger>
              <Tabs.Trigger value="delete" class="w-full">Delete</Tabs.Trigger>
              <Tabs.Trigger value="read" class="w-full">Read</Tabs.Trigger>
            </Tabs.List>
            <Tabs.Content value="insert" class="space-y-2">
              <p>
                Sequin emits an <code>insert</code> message when a row is inserted.
              </p>
              <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
                  >{JSON.stringify(changeExamples.insert, null, 2).trim()}</code
                ></pre>
            </Tabs.Content>
            <Tabs.Content value="update" class="space-y-2">
              <p>
                Sequin emits an <code>update</code> message when a row is updated.
              </p>
              <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
                  >{JSON.stringify(changeExamples.update, null, 2).trim()}</code
                ></pre>
            </Tabs.Content>
            <Tabs.Content value="delete" class="space-y-2">
              <p>
                Sequin emits a <code>delete</code> message when a row is deleted.
              </p>
              <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
                  >{JSON.stringify(changeExamples.delete, null, 2).trim()}</code
                ></pre>
            </Tabs.Content>
            <Tabs.Content value="read" class="space-y-2">
              <p>
                Sequin emits a <code>read</code> message during backfills.
                <code>read</code>
                messages look just like <code>insert</code> messages.
              </p>
              <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
                  >{JSON.stringify(changeExamples.read, null, 2).trim()}</code
                ></pre>
            </Tabs.Content>
          </Tabs.Root>
        {/if}
      </div>

      <Dialog.Footer class="mt-4">
        <Button on:click={() => (showExampleModal = false)}>Close</Button>
      </Dialog.Footer>
      <Dialog.Close />
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
