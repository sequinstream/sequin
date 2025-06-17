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
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    ExpandableCard,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import FullPageForm from "../components/FullPageForm.svelte";
  import FilterForm from "../components/FilterForm.svelte";
  import FunctionPicker from "$lib/consumers/FunctionPicker.svelte";
  import SinkHttpPushForm from "$lib/consumers/SinkHttpPushForm.svelte";
  import SqsSinkForm from "$lib/sinks/sqs/SqsSinkForm.svelte";
  import SnsSinkForm from "$lib/sinks/sns/SnsSinkForm.svelte";
  import KinesisSinkForm from "$lib/sinks/kinesis/KinesisSinkForm.svelte";
  import S2SinkForm from "$lib/sinks/s2/S2SinkForm.svelte";
  import RedisStreamSinkForm from "$lib/sinks/redis-stream/RedisStreamSinkForm.svelte";
  import RedisStringSinkForm from "$lib/sinks/redis-string/RedisStringSinkForm.svelte";
  import KafkaSinkForm from "$lib/sinks/kafka/KafkaSinkForm.svelte";
  import GcpPubsubSinkForm from "$lib/sinks/gcp_pubsub/GcpPubsubSinkForm.svelte";
  import SequinStreamSinkForm from "$lib/sinks/sequin_stream/SequinStreamSinkForm.svelte";
  import NatsSinkForm from "$lib/sinks/nats/NatsSinkForm.svelte";
  import RabbitMqSinkForm from "$lib/sinks/rabbitmq/RabbitMqSinkForm.svelte";
  import AzureEventHubSinkForm from "$lib/sinks/azure_event_hub/AzureEventHubSinkForm.svelte";
  import { CircleAlert, Info, Plus } from "lucide-svelte";
  import TypesenseSinkForm from "$lib/sinks/typesense/TypesenseSinkForm.svelte";
  import ElasticsearchSinkForm from "$lib/sinks/elasticsearch/ElasticsearchSinkForm.svelte";
  import * as Alert from "$lib/components/ui/alert/index.js";
  import SchemaTableSelector from "../components/SchemaTableSelector.svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import * as Popover from "$lib/components/ui/popover";
  import * as Dialog from "$lib/components/ui/dialog";
  import MessageExamples from "$lib/components/MessageExamples.svelte";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import type { Table as DatabaseTable, Table } from "$lib/databases/types";
  import BackfillForm from "$lib/components/BackfillForm.svelte";
  import type { Source } from "./types";
  import { externalType } from "./utils";

  type Database = {
    id: string;
    name: string;
    tables: DatabaseTable[];
    schemas: string[];
  };

  export let live;
  export let parent;
  export let consumer;
  export let consumerTitle;
  export let httpEndpoints;
  export let databases: Database[];
  export let functions: Array<{
    id: string;
    name: string;
    type: string;
    description: string;
  }>;
  export let errors: {
    consumer: Record<string, string | Record<string, string>>;
    backfill: Record<string, string>;
  };
  export let isSelfHosted: boolean;

  type MessageKind = "event" | "record";

  interface FormState {
    type: string;
    messageKind: MessageKind;
    maxMemoryMb: number;
    postgresDatabaseId: string | null;
    source: Source;
    actions: string[];
    name: string;
    ackWaitMs: number;
    maxAckPending: number;
    maxWaiting: number;
    maxRetryCount: number | null;
    sink: any;
    runInitialBackfill: boolean;
    backfill: {
      selectedTableOids: number[];
    };
    groupColumnAttnums: number[];
    batchSize: number;
    transform: string;
    filterId: string;
    routingId: string;
    routingMode: string;
    timestampFormat: string;
  }

  let initialForm: FormState = {
    type: consumer.type,
    messageKind: (consumer.message_kind || "event") as MessageKind,
    maxMemoryMb: Number(consumer.max_memory_mb),
    postgresDatabaseId: consumer.postgres_database_id,
    source: consumer.source,
    actions: consumer.actions || ["insert", "update", "delete"],
    name: consumer.name || "",
    ackWaitMs: Number(consumer.ack_wait_ms) || 30000,
    maxAckPending: Number(consumer.max_ack_pending) || 10000,
    maxWaiting: Number(consumer.max_waiting) || 20,
    maxRetryCount: Number(consumer.max_retry_count) || null,
    sink: consumer.sink,
    runInitialBackfill: false,
    backfill: {
      selectedTableOids: [],
    },
    groupColumnAttnums: consumer.group_column_attnums || [],
    batchSize: Number(consumer.batch_size) || 1,
    transform: consumer.transform_id || "none",
    timestampFormat: consumer.timestamp_format || "iso8601",
    routingId: consumer.routing_id || "none",
    routingMode: consumer.routing_mode,
    filterId: consumer.filter_id || "none",
  };
  console.log("initialForm", initialForm);

  let form: FormState = { ...initialForm };
  let lastPushedFormJSON = null;
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
    // Only push the form if it has changed since the last push
    // Prevents infinite loop of pushing the same form over and over
    if (JSON.stringify(form) !== lastPushedFormJSON) {
      pushEvent("form_updated", { form });
      lastPushedFormJSON = JSON.stringify(form);
    }
  }

  let selectedDatabase: Database | null = null;
  let isCreateConsumerDisabled: boolean = true;

  const pushEvent = (event, payload = {}, cb = (reply: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  function handleDatabaseSelect(database: Database | null) {
    selectedDatabase = database;
    if (database) {
      form.name = `${database.name}-${externalType(form.type)}-sink`;
    }
  }

  $: {
    if (form.postgresDatabaseId) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId,
      );
    }

    isCreateConsumerDisabled = !form.postgresDatabaseId;
  }

  let tables_included_in_source: Table[] = [];
  $: tables_included_in_source = selectedDatabase?.tables.filter((t) => {
    return tableIncludedInSource(t, form.source);
  });

  function tableIncludedInSource(table: DatabaseTable, source: Source) {
    if (source.exclude_table_oids?.includes(table.oid)) {
      return false;
    }

    if (source.exclude_schemas?.includes(table.schema)) {
      return false;
    }

    if (
      !!source.include_schemas &&
      !source.include_schemas.includes(table.schema)
    ) {
      return false;
    }

    if (
      !!source.include_table_oids &&
      !source.include_table_oids.includes(table.oid)
    ) {
      return false;
    }

    return true;
  }

  const isEditMode = !!consumer.id;

  function handleConsumerSubmit() {
    isSubmitting = true;
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        isSubmitting = false;
      }
    });
  }

  function handleClose() {
    pushEvent("form_closed");
  }

  let showLocalhostWarningDialog = false;
  let dismissedLocalhostWarning = false;

  function onTestConnection() {
    // Add check for localhost in sink configuration
    const sinkConfig = form.sink;
    let hasLocalhost = false;

    if (sinkConfig) {
      const configStr = JSON.stringify(sinkConfig).toLowerCase();
      hasLocalhost =
        configStr.includes("localhost") || configStr.includes("127.0.0.1");
    }

    if (hasLocalhost && !dismissedLocalhostWarning) {
      showLocalhostWarningDialog = true;
    } else {
      runConnectionTest();
    }
  }

  function runConnectionTest() {
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

  let showMessageTypeExampleModal = false;
  let selectedExampleType: "change" | "record" = "change";

  let transformSectionEnabled = false;
  let transformSectionExpanded = false;
  let backfillSectionEnabled = false;
  let backfillSectionExpanded = false;
  $: {
    transformSectionEnabled = !!selectedDatabase;
    transformSectionExpanded = transformSectionEnabled && !isEditMode;

    backfillSectionEnabled = !!selectedDatabase && !isEditMode;
    backfillSectionExpanded = backfillSectionEnabled && !isEditMode;
  }

  let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

  function refreshFunctions() {
    functionRefreshState = "refreshing";
    pushEvent("refresh_functions", {}, () => {
      functionRefreshState = "done";
      setTimeout(() => {
        functionRefreshState = "idle";
      }, 2000);
    });
  }

  const exampleIsoTimestamp = "2025-01-01T00:00:00.000000Z";
  const exampleUnixMicrosecondTimestamp = 1704086400000000;
</script>

<FullPageForm
  title={isEditMode
    ? `Edit ${consumerTitle} ${form.name}`
    : `Create ${consumerTitle}`}
  showConfirmOnExit={isDirty}
  on:close={handleClose}
>
  <form
    on:submit|preventDefault={handleConsumerSubmit}
    class="space-y-6 max-w-3xl mx-auto"
  >
    <Card>
      <CardHeader>
        <CardTitle>Source</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <SchemaTableSelector
            {isEditMode}
            {databases}
            bind:source={form.source}
            bind:selectedDatabaseId={form.postgresDatabaseId}
            {pushEvent}
            errors={errors.consumer?.source || {}}
            onDatabaseSelect={handleDatabaseSelect}
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
          {functions}
          messageKind={form.messageKind}
          {selectedDatabase}
          bind:form
          {errors}
          {refreshFunctions}
          {functionRefreshState}
          showTitle={false}
        />
      </CardContent>
    </Card>

    <ExpandableCard
      disabled={!transformSectionEnabled}
      expanded={transformSectionExpanded}
    >
      <svelte:fragment slot="title">
        Transforms
        <Tooltip.Root openDelay={200}>
          <Tooltip.Trigger>
            <Info class="h-4 w-4 text-gray-400 cursor-help" />
          </Tooltip.Trigger>
          <Tooltip.Content class="p-4 max-w-xs">
            <p class="text-sm text-muted-foreground font-normal">
              Transform your messages before they are sent to the sink
              destination.
            </p>
          </Tooltip.Content>
        </Tooltip.Root>
      </svelte:fragment>

      <svelte:fragment slot="summary">
        {#if !selectedDatabase}
          <p class="text-sm text-muted-foreground">Please select a database.</p>
        {:else if form.transform === "none"}
          <p class="text-sm text-muted-foreground">
            No transform in use. Messages will be sent to the sink destination
            in Sequin's <a
              href="https://sequinstream.com/docs/reference/messages"
              target="_blank"
              rel="noopener noreferrer"
              class="text-primary underline">message format</a
            >.
          </p>
        {:else}
          <p class="font-medium">
            {functions.find((f) => f.id === form.transform)?.name ||
              form.transform}
          </p>
          <p class="text-sm text-muted-foreground">
            {functions.find((f) => f.id === form.transform)?.description || ""}
          </p>
        {/if}
      </svelte:fragment>

      <svelte:fragment slot="content">
        <FunctionPicker
          {functions}
          selectedFunctionId={form.transform}
          title="Transform"
          onFunctionChange={(functionId) => (form.transform = functionId)}
          {refreshFunctions}
          functionTypes={["transform", "path"]}
          createNewQueryParams="?type=transform"
          bind:refreshState={functionRefreshState}
        >
          <svelte:fragment slot="none-option">
            No transform in use. Messages will be sent to the sink destination
            in Sequin's <a
              href="https://sequinstream.com/docs/reference/messages"
              target="_blank"
              rel="noopener noreferrer"
              class="text-primary underline">message format</a
            >.
          </svelte:fragment>
        </FunctionPicker>
      </svelte:fragment>
    </ExpandableCard>

    {#if !isEditMode}
      <ExpandableCard
        disabled={!backfillSectionEnabled}
        expanded={!isEditMode && backfillSectionEnabled}
      >
        <svelte:fragment slot="title">
          Initial backfill

          <Tooltip.Root openDelay={200}>
            <Tooltip.Trigger>
              <Info class="h-4 w-4 text-gray-400 cursor-help" />
            </Tooltip.Trigger>
            <Tooltip.Content class="p-4 max-w-xs">
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
            </Tooltip.Content>
          </Tooltip.Root>
        </svelte:fragment>

        <svelte:fragment slot="summary">
          <p class="text-sm text-muted-foreground">
            {#if form.backfill.selectedTableOids.length === 0}
              No initial backfill. You can run backfills at any time in the
              future.
            {:else if form.backfill.selectedTableOids.length === 1}
              Backfilling the <b
                >{tables_included_in_source.find(
                  (t) => t.oid === form.backfill.selectedTableOids[0],
                )?.name}</b
              > table.
            {:else if form.backfill.selectedTableOids.length === tables_included_in_source.length}
              Backfilling all tables in the database.
            {:else}
              Backfilling {form.backfill.selectedTableOids.length} tables in the
              database.
            {/if}
          </p>
        </svelte:fragment>

        <svelte:fragment slot="content">
          <BackfillForm {tables_included_in_source} form={form.backfill} />
        </svelte:fragment>
      </ExpandableCard>
    {/if}

    {#if consumer.type === "http_push"}
      <SinkHttpPushForm
        errors={errors.consumer}
        {httpEndpoints}
        bind:form
        {live}
        {parent}
        {functions}
        {refreshFunctions}
        bind:functionRefreshState
      />
    {:else if consumer.type === "sqs"}
      <SqsSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "sns"}
      <SnsSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "kinesis"}
      <KinesisSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "s2"}
      <S2SinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "redis_stream"}
      <RedisStreamSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "redis_string"}
      <RedisStringSinkForm
        errors={errors.consumer}
        bind:form
        {refreshFunctions}
        {functions}
        {functionRefreshState}
      />
    {:else if consumer.type === "kafka"}
      <KafkaSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "gcp_pubsub"}
      <GcpPubsubSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "sequin_stream"}
      <SequinStreamSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "nats"}
      <NatsSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "rabbitmq"}
      <RabbitMqSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "azure_event_hub"}
      <AzureEventHubSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "typesense"}
      <TypesenseSinkForm errors={errors.consumer} bind:form />
    {:else if consumer.type === "elasticsearch"}
      <ElasticsearchSinkForm errors={errors.consumer} bind:form />
    {/if}

    <Card>
      <CardHeader>
        <CardTitle>Sink settings</CardTitle>
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
            />
            <p class="text-xs font-light">
              Must contain only alphanumeric characters, underscores, hyphens,
              or dots.
            </p>
            {#if isEditMode && form.name !== initialForm.name}
              <p class="text-amber-500 text-sm mt-1">
                If you're using YAML to configure Sequin, you may need to change
                the name for this sink in your YAML config file.
              </p>
            {/if}
          </div>
          {#if errors.consumer.name}
            <p class="text-destructive text-sm">{errors.consumer.name}</p>
          {/if}
        </div>

        {#if isSelfHosted}
          <Accordion class="w-full">
            <AccordionItem value="advanced">
              <AccordionTrigger>Advanced configuration</AccordionTrigger>
              <AccordionContent>
                <div class="space-y-4 pt-4">
                  <div class="space-y-2">
                    <Label for="timestamp-format">Timestamp format</Label>
                    <Select
                      selected={{
                        value: form.timestampFormat,
                        label:
                          form.timestampFormat === "iso8601"
                            ? `ISO8601 string (e.g. ${exampleIsoTimestamp})`
                            : `Unix timestamp (numeric, microseconds, e.g. ${exampleUnixMicrosecondTimestamp})`,
                      }}
                      onSelectedChange={(event) => {
                        form.timestampFormat = event.value;
                      }}
                    >
                      <SelectTrigger id="timestamp-format" class="w-full">
                        <SelectValue placeholder="Select timestamp format" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="iso8601"
                          >ISO8601 string (e.g. {exampleIsoTimestamp})</SelectItem
                        >
                        <SelectItem value="unix_microsecond"
                          >Unix timestamp (numeric, microseconds, e.g. {exampleUnixMicrosecondTimestamp})</SelectItem
                        >
                      </SelectContent>
                    </Select>
                    <p class="text-xs font-light">
                      Choose how timestamps should be formatted in messages sent
                      to your sink.
                    </p>
                    {#if errors.consumer.timestamp_format}
                      <p class="text-destructive text-sm">
                        {errors.consumer.timestamp_format}
                      </p>
                    {/if}
                  </div>
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>
        {/if}

        <div class="flex justify-end items-center gap-2">
          {#if consumer.type !== "http_push" && consumer.type !== "sequin_stream"}
            <Popover.Root bind:open={showLocalhostWarningDialog}>
              <Popover.Trigger />
              <Button
                loading={testConnectionState.status === "loading"}
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
              <Popover.Content class="w-80">
                <div class="grid gap-4">
                  <div class="space-y-2">
                    <h4 class="font-medium leading-none">
                      Warning: localhost detected
                    </h4>
                    <p class="text-sm text-muted-foreground">
                      Using '<code>localhost</code>' in your configuration may
                      not work as expected. Consider using '<code
                        >host.docker.internal</code
                      >' instead to connect to services running on your machine.
                    </p>
                  </div>
                  <div class="flex justify-end gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      on:click={() => (showLocalhostWarningDialog = false)}
                    >
                      Cancel
                    </Button>
                    <Button
                      size="sm"
                      on:click={() => {
                        dismissedLocalhostWarning = true;
                        showLocalhostWarningDialog = false;
                        runConnectionTest();
                      }}
                    >
                      Test anyway
                    </Button>
                  </div>
                </div>
              </Popover.Content>
            </Popover.Root>
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
</FullPageForm>

<Dialog.Root bind:open={showMessageTypeExampleModal}>
  <Dialog.Portal>
    <Dialog.Overlay />
    <Dialog.Content class="lg:max-w-5xl md:max-w-3xl p-4">
      <Dialog.Header class="mb-4">
        <Dialog.Title>Message examples</Dialog.Title>
      </Dialog.Header>

      <!-- Mobile view -->
      <div class="lg:hidden space-y-2">
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
              <SelectItem value="change">Change Messages</SelectItem>
              <SelectItem value="record">Row Messages</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <MessageExamples type={selectedExampleType} />
      </div>

      <!-- Desktop view -->
      <div class="hidden lg:grid grid-cols-2 gap-8">
        <div>
          <h3 class="text-lg font-semibold mb-4">Change Messages</h3>
          <MessageExamples type="change" />
        </div>
        <div>
          <h3 class="text-lg font-semibold mb-4">Row Messages</h3>
          <MessageExamples type="record" />
        </div>
      </div>

      <Dialog.Footer class="mt-4">
        <Button on:click={() => (showMessageTypeExampleModal = false)}
          >Close</Button
        >
      </Dialog.Footer>
      <Dialog.Close />
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>
