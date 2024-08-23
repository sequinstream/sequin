<script lang="ts">
  import NewTopnav from "./NewTopnav.svelte";
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
    PlusCircle,
    InfoIcon,
    RefreshCwIcon,
    CheckIcon,
  } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";

  import HttpPushVisual from "./HttpPushVisual.svelte";
  import HttpPullVisual from "./HttpPullVisual.svelte";
  import TableSelector from "../components/TableSelector.svelte";
  import TableFilters from "../components/TableFilters.svelte";
  import { getColorFromName } from "../utils";

  export let currentStep: string;
  export let live;
  export let parent;
  export let form: {
    messageKind: string;
    consumerKind: string;
    postgresDatabaseId: string;
    tableOid: number | null;
    sourceTableFilters: {
      column: number | null;
      operator: string | null;
      value: string;
    }[];
    name: string;
    ackWaitMs: number;
    maxAckPending: number;
    maxDeliver: number;
    maxWaiting: number;
    status: "active" | "disabled";
    httpEndpoint?: {
      url: string;
      headers: { [key: string]: string };
    };
    replicationSlotId: string;
  };

  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<{
      oid: number;
      schema: string;
      name: string;
      columns: Array<{
        attnum: number;
        name: string;
      }>;
    }>;
  }>;

  let continueDisabled = true;
  $: {
    switch (currentStep) {
      case "select_table":
        continueDisabled = !form.postgresDatabaseId || !form.tableOid;
        break;
      case "configure_filters":
        continueDisabled = form.sourceTableFilters.some(
          (filter) => !filter.column || !filter.operator || !filter.value
        );
        break;
    }
  }

  const pushEvent = (event, payload = {}, cb = () => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  const streamTypes = [
    {
      id: "change",
      title: "Change stream",
      description:
        "Receive every create, update, and delete that happens to rows.",
      comingSoon: false,
      replacements: ["Postgres triggers", "SQS", "ORM hooks", "WAL events"],
    },
    {
      id: "change_retention",
      title: "Change stream with retention",
      description:
        "Receive every create, update, and delete that happens to rows.",
      comingSoon: true,
      replacements: ["Kafka", "Debezium", { text: "pg_audit", style: "code" }],
    },
    {
      id: "sync",
      title: "Sync stream",
      description: "Receive the latest version of rows whenever they change.",
      comingSoon: false,
      replacements: ["Kafka", { text: "COPY", style: "code" }, "Manual syncs"],
    },
  ];

  const consumerTypes = [
    {
      id: "http_push",
      title: "HTTP Push",
      description: "(Webhooks)",
      comingSoon: false,
    },
    {
      id: "http_pull",
      title: "HTTP Pull",
      description: "(Like SQS)",
      comingSoon: false,
    },
  ];

  const streamFeatures = [
    {
      features: [
        { text: "Exactly-once processing", attribute: "positive" },
        {
          text: "Creates, updates, deletes with both new and old values",
          attribute: "positive",
        },
        { text: "No replays or rewinds", attribute: "negative" },
        { text: "Events deleted after processing", attribute: "neutral" },
      ],
    },
    {
      features: [
        { text: "Exactly-once processing", attribute: "positive" },
        {
          text: "Creates, updates, deletes with both new and old values",
          attribute: "positive",
        },
        { text: "Replays and rewinds", attribute: "positive" },
        { text: "Audit table of changes", attribute: "positive" },
      ],
    },
    {
      features: [
        { text: "Exactly-once processing", attribute: "positive" },
        {
          text: "Receive new on every record create or update",
          attribute: "positive",
        },
        { text: "Backfill historical data", attribute: "positive" },
        { text: "Rewind and replay data", attribute: "positive" },
        { text: "No deletes or old values", attribute: "negative" },
      ],
    },
  ];

  function handleStreamClick(stream) {
    if (!stream.comingSoon) {
      form.messageKind = stream.id;
      pushEvent("form_updated", { form, step_forward: true });
    }
  }

  function handleConsumerClick(consumer) {
    if (!consumer.comingSoon) {
      form.consumerKind = consumer.id;
      pushEvent("form_updated", { form, step_forward: true });
    }
  }

  function isComingSoon(index) {
    return streamTypes[index].comingSoon;
  }

  const tables = [
    { name: "mydb → public.users" },
    { name: "mydb → public.orders" },
  ];

  const columns = ["id", "name", "email", "created_at"];
  const operators = [
    "=",
    "!=",
    ">",
    "<",
    ">=",
    "<=",
    "IN",
    "NOT IN",
    "IS NULL",
    "IS NOT NULL",
  ];

  let selectedDatabase: any;
  let selectedTable: any;

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

  let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  function refreshTables(databaseId: string) {
    if (databaseId) {
      tableRefreshState = "refreshing";
      pushEvent("refresh_tables", { database_id: databaseId }, () => {
        tableRefreshState = "done";
        setTimeout(() => {
          tableRefreshState = "idle";
        }, 2000);
      });
    }
  }

  function handleConsumerConfigSubmit() {
    pushEvent("form_updated", { form, step_forward: true });
  }

  let activeInfo = "";

  const infoContent = {
    name: [
      "Choose a unique name for your consumer.",
      "You'll use this name in HTTP requests to Sequin.",
    ],
    ackWaitMs: [
      "The visibility timeout is the duration that a message is hidden from other consumers after it's been received.",
      "This prevents multiple consumers from processing the same message simultaneously.",
      "If the message isn't processed within this time, it becomes visible to other consumers again.",
    ],
    maxAckPending: [
      "Max ack pending sets the maximum number of messages that can be pending acknowledgment at any time.",
      "This helps control the flow of messages and prevents overwhelming the consumer.",
    ],
    maxWaiting: [
      "Max waiting determines the maximum number of waiting pull requests.",
      "This setting helps manage resource utilization and prevents excessive polling.",
    ],
  };

  // Add this hard-coded data
  const sourceTable = {
    name: "mydb → public.users",
    color: "bg-red-100",
    filters: [
      { column: "org_id", operator: "=", value: "acme_1234" },
      { column: "status", operator: "=", value: "active" },
    ],
  };

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;
  }

  function onConsumerCreate() {
    pushEvent("form_updated", { form, step_forward: true });
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
    pushEvent("form_updated", { form });
  }
</script>

<div class="flex flex-col min-h-screen">
  <NewTopnav {currentStep} />
  <div class="flex-grow" style="min-width: 750px;">
    {#if currentStep === "select_stream"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Select a stream type</h2>
        </div>
      </div>

      <div class="p-8 max-w-5xl mx-auto">
        <div class="grid grid-cols-3 gap-6">
          {#each streamTypes as stream}
            <div class="stream-container">
              <div
                class="stream-box relative {stream.comingSoon
                  ? 'disabled'
                  : ''}"
                on:click={() => handleStreamClick(stream)}
                on:keydown={(e) =>
                  e.key === "Enter" && handleStreamClick(stream)}
                tabindex={stream.comingSoon ? -1 : 0}
                role="button"
                aria-disabled={stream.comingSoon}
              >
                {#if stream.comingSoon}
                  <div class="coming-soon-badge">Coming Soon</div>
                {/if}
                <h3 class="title">{stream.title}</h3>
                <p class="description">{stream.description}</p>
              </div>
            </div>
          {/each}
        </div>

        <div class="grid grid-cols-3 gap-6 mt-6">
          {#each streamTypes as stream, i}
            <div class="replacement-box" class:coming-soon={isComingSoon(i)}>
              <p class="replacement-title">Replacement for:</p>
              <ul class="replacement-list">
                {#each stream.replacements as replacement}
                  <li>
                    {#if typeof replacement === "string"}
                      {replacement}
                    {:else if replacement.style === "code"}
                      <code>{replacement.text}</code>
                    {:else}
                      {replacement.text}
                    {/if}
                  </li>
                {/each}
              </ul>
            </div>
          {/each}
        </div>

        <div class="grid grid-cols-3 gap-6 mt-6">
          {#each streamFeatures as { features }, i}
            <div class="feature-box" class:coming-soon={isComingSoon(i)}>
              <ul class="feature-list">
                {#each features as feature}
                  <li class="feature-item">
                    {#if feature.attribute === "positive"}
                      <span class="text-green-500">✓</span>
                    {:else if feature.attribute === "negative"}
                      <span class="text-red-500">✗</span>
                    {:else}
                      <span class="text-gray-400">•</span>
                    {/if}
                    {feature.text}
                  </li>
                {/each}
              </ul>
            </div>
          {/each}
        </div>
      </div>
    {/if}

    {#if currentStep === "select_consumer"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Select a consumer type</h2>
        </div>
      </div>
      <div class="p-8 max-w-4xl mx-auto">
        <div class="grid grid-cols-1 md:grid-cols-2 gap-6 w-full">
          {#each consumerTypes as consumer}
            <div
              class="bg-white border border-gray-200 overflow-hidden rounded-lg shadow {consumer.comingSoon
                ? 'opacity-50 cursor-not-allowed'
                : 'cursor-pointer hover:shadow-md transition-shadow'}"
              on:click={() =>
                !consumer.comingSoon && handleConsumerClick(consumer)}
              on:keydown={(e) =>
                !consumer.comingSoon &&
                e.key === "Enter" &&
                handleConsumerClick(consumer)}
              tabindex={consumer.comingSoon ? -1 : 0}
              role="button"
              aria-disabled={consumer.comingSoon}
            >
              <div class="p-4">
                <h2 class="text-lg font-medium text-gray-700 mb-2">
                  {consumer.title}
                </h2>
                <div class="text-sm text-gray-500 mb-4">
                  {consumer.description}
                </div>
                {#if consumer.id === "http_push"}
                  <HttpPushVisual />
                {:else if consumer.id === "http_pull"}
                  <HttpPullVisual />
                {/if}
                <p class="text-sm text-gray-600 mt-4">
                  {#if consumer.id === "http_push"}
                    Sequin will send updates from Postgres tables to an endpoint
                    you specify.
                  {:else if consumer.id === "http_pull"}
                    Your application will read from Sequin's queue via HTTP.
                  {/if}
                </p>
              </div>
              {#if consumer.comingSoon}
                <div
                  class="absolute top-2 right-2 bg-secondary-3xSubtle text-xs font-semibold px-2 py-1 rounded"
                >
                  Coming Soon
                </div>
              {/if}
            </div>
          {/each}
        </div>
      </div>
    {/if}

    {#if currentStep === "select_table"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Select a table</h2>
        </div>
      </div>
      <div class="p-8 max-w-5xl mx-auto">
        <div class="flex justify-end mb-4"></div>
        <TableSelector
          {databases}
          onSelect={handleTableSelect}
          {pushEvent}
          selectedDatabaseId={form.postgresDatabaseId}
          selectedTableOid={form.tableOid}
        />
      </div>
    {/if}

    {#if currentStep === "configure_filters"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Define filters</h2>
        </div>
      </div>
      <div class="p-8 max-w-5xl mx-auto">
        <div class="mb-6">
          <p class="text-muted-foreground">
            Optionally filter rows from the table based on the SQL <code
              >where</code
            > conditions below.
          </p>
        </div>
        {#if form.postgresDatabaseId && form.tableOid}
          {#if selectedDatabase && selectedTable}
            <div class="mb-6">
              <div
                class="grid grid-cols-[auto_1fr_1fr_15px] gap-4 mb-2 items-center"
              >
                <icon
                  class="hero-table-cells w-6 h-6 rounded {getColorFromName(
                    `${selectedTable.schema}.${selectedTable.name}`
                  )}"
                ></icon>
                <span class="font-medium"
                  >{selectedTable.schema}.{selectedTable.name}</span
                >
                <Button
                  variant="outline"
                  size="sm"
                  on:click={() => refreshTables(form.postgresDatabaseId)}
                  disabled={tableRefreshState === "refreshing"}
                  class="justify-self-end"
                >
                  {#if tableRefreshState === "refreshing"}
                    <RefreshCwIcon class="h-4 w-4 mr-2 animate-spin" />
                  {:else if tableRefreshState === "done"}
                    <CheckIcon class="h-4 w-4 mr-2 text-green-500" />
                  {:else}
                    <RefreshCwIcon class="h-4 w-4 mr-2" />
                  {/if}
                  Refresh
                </Button>
              </div>
              <TableFilters
                filters={form.sourceTableFilters}
                columns={selectedTable.columns}
                onFilterChange={handleFilterChange}
              />
            </div>
          {:else}
            <p>Loading table information...</p>
          {/if}
        {:else}
          <p>Please select a database and table first.</p>
        {/if}
      </div>
    {/if}

    {#if currentStep === "configure_consumer"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Configure consumer</h2>
        </div>
      </div>
      <div class="p-8 max-w-5xl mx-auto">
        <div class="flex gap-6">
          <div class="w-full max-w-3xl">
            <h2 class="text-2xl font-bold mb-2">
              Create an HTTP pull consumer
            </h2>
            <p class="text-muted-foreground mb-6">
              Configure your consumer settings
            </p>
            <form
              on:submit|preventDefault={handleConsumerConfigSubmit}
              class="space-y-6"
            >
              <div class="space-y-2">
                <Label
                  for="name"
                  class="flex items-center gap-2"
                  data-1p-ignore
                  data-lpignore="true"
                  data-form-type="other"
                >
                  Name
                  <button
                    type="button"
                    on:click={() => (activeInfo = "name")}
                    class="text-muted-foreground"
                  >
                    <InfoIcon class="h-4 w-4" />
                  </button>
                </Label>
                <Input
                  id="name"
                  bind:value={form.name}
                  placeholder="Enter consumer name"
                  on:focus={() => (activeInfo = "name")}
                />
              </div>

              <div class="space-y-2">
                <Label for="visibility-timeout" class="flex items-center gap-2">
                  Visibility timeout
                  <button
                    type="button"
                    on:click={() => (activeInfo = "ackWaitMs")}
                    class="text-muted-foreground"
                  >
                    <InfoIcon class="h-4 w-4" />
                  </button>
                </Label>
                <div class="flex items-center gap-2">
                  <Input
                    id="visibility-timeout"
                    type="number"
                    bind:value={form.ackWaitMs}
                    class="w-24"
                    on:focus={() => (activeInfo = "ackWaitMs")}
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
              </div>

              <Accordion class="w-full">
                <AccordionItem value="advanced">
                  <AccordionTrigger>Advanced configuration</AccordionTrigger>
                  <AccordionContent>
                    <div class="space-y-4 pt-4">
                      <div class="space-y-2">
                        <Label
                          for="max-ack-pending"
                          class="flex items-center gap-2"
                        >
                          Max ack pending
                          <button
                            type="button"
                            on:click={() => (activeInfo = "maxAckPending")}
                            class="text-muted-foreground"
                          >
                            <InfoIcon class="h-4 w-4" />
                          </button>
                        </Label>
                        <Input
                          id="max-ack-pending"
                          type="number"
                          bind:value={form.maxAckPending}
                          on:focus={() => (activeInfo = "maxAckPending")}
                        />
                      </div>
                      <div class="space-y-2">
                        <Label
                          for="max-waiting"
                          class="flex items-center gap-2"
                        >
                          Max waiting
                          <button
                            type="button"
                            on:click={() => (activeInfo = "maxWaiting")}
                            class="text-muted-foreground"
                          >
                            <InfoIcon class="h-4 w-4" />
                          </button>
                        </Label>
                        <Input
                          id="max-waiting"
                          type="number"
                          bind:value={form.maxWaiting}
                          on:focus={() => (activeInfo = "maxWaiting")}
                        />
                      </div>
                    </div>
                  </AccordionContent>
                </AccordionItem>
              </Accordion>
            </form>
          </div>

          <Card class="w-80 h-fit sticky top-4">
            <CardHeader>
              <CardTitle>Information</CardTitle>
            </CardHeader>
            <CardContent>
              {#if activeInfo}
                {#each infoContent[activeInfo] as infoParagraph}
                  <p class="text-sm text-muted-foreground mb-2">
                    {infoParagraph}
                  </p>
                {/each}
              {:else}
                <p class="text-sm text-muted-foreground">
                  Select a field or click an info icon to see more details.
                </p>
              {/if}
            </CardContent>
          </Card>
        </div>
      </div>
    {/if}
  </div>

  <!-- Bottom Nav -->
  <div
    class="flex flex-shrink-0 h-16 z-10 justify-center w-full sticky bottom-0 bg-surface-base border-t border-carbon-100"
  >
    <div class="flex items-center justify-between w-full px-8 max-w-[1288px]">
      <div>
        {#if currentStep !== "select_stream"}
          <Button variant="outline" on:click={() => pushEvent("step_back")}>
            <svg
              xmlns="http://www.w3.org/2000/svg"
              viewBox="0 0 24 24"
              class="w-4 h-4 mr-2"
            >
              <path
                fill-rule="evenodd"
                d="M12.768 6.36a1 1 0 0 1-.128 1.408L7.562 12l5.078 4.232a1 1 0 1 1-1.28 1.536l-6-5a1 1 0 0 1 0-1.536l6-5a1 1 0 0 1 1.408.128Z"
                clip-rule="evenodd"
              />
              <path
                fill-rule="evenodd"
                d="M5 12a1 1 0 0 1 1-1h14a1 1 0 1 1 0 2H6a1 1 0 0 1-1-1Z"
                clip-rule="evenodd"
              />
            </svg>
            Go back
          </Button>
        {/if}
      </div>
      <div>
        {#if currentStep === "select_stream"}
          <p class="text-sm leading-4 text-carbon-200 font-semibold">
            Select a stream type to continue
          </p>
        {:else if currentStep === "select_consumer"}
          <p class="text-sm leading-4 text-carbon-200 font-semibold">
            Select a consumer type to continue
          </p>
        {:else if currentStep === "configure_consumer"}
          <Button on:click={onConsumerCreate}>Create Consumer</Button>
        {:else}
          <Button
            on:click={() => pushEvent("step_forward")}
            disabled={continueDisabled}
          >
            Continue
            <icon class="w-4 h-4 ml-2 hero-arrow-right" aria-hidden="true" />
          </Button>
        {/if}
      </div>
    </div>
  </div>
</div>

<style lang="postcss">
  .container {
    max-width: 1288px;
  }

  .stream-box {
    @apply bg-white border border-border rounded-lg p-6 hover:shadow-md transition-shadow cursor-pointer;
    height: 180px;
    display: flex;
    flex-direction: column;
  }

  .stream-box.disabled {
    @apply opacity-50 cursor-not-allowed hover:shadow-none;
  }

  .title {
    @apply text-lg font-semibold mb-3;
  }

  .description {
    @apply mt-auto;
  }

  .coming-soon-badge {
    @apply absolute top-0 right-0 bg-secondary-3xSubtle text-xs font-semibold px-2 py-1 rounded-tr-lg rounded-bl-lg;
  }

  .replacement-box {
    @apply bg-secondary-3xSubtle p-4 rounded-lg;
  }

  .replacement-box.coming-soon,
  .feature-box.coming-soon {
    @apply opacity-50;
  }

  .replacement-title {
    @apply font-semibold mb-2;
  }

  .replacement-list {
    @apply list-disc pl-5 text-sm;
  }

  .replacement-list code {
    @apply bg-gray-100 px-1 py-0.5 rounded text-sm font-mono;
  }

  .feature-box {
    @apply bg-white border border-border rounded-lg p-4;
  }

  .feature-list {
    @apply list-none p-0;
  }

  .feature-item {
    @apply flex items-start mb-2 text-sm;
  }

  .feature-item span {
    @apply mr-2 font-bold;
  }

  .consumer-box {
    @apply bg-white border border-border rounded-lg p-6 hover:shadow-md transition-shadow cursor-pointer;
    height: 180px;
    display: flex;
    flex-direction: column;
  }

  .consumer-box.disabled {
    @apply opacity-50 cursor-not-allowed hover:shadow-none;
  }
</style>
