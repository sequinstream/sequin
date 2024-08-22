<script lang="ts">
  import NewTopnav from "./NewTopnav.svelte";
  import NewBottomnav from "./NewBottomnav.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { PlusCircle, RefreshCw, InfoIcon, PencilIcon } from "lucide-svelte";
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
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import HttpPushVisual from "./HttpPushVisual.svelte";
  import HttpPullVisual from "./HttpPullVisual.svelte";

  export let currentStep: string;
  export let live;
  export let parent;
  export let form: {
    messageKind: string;
    consumerKind: string;
    sourceTable: {
      name: string;
      filters: {
        column: string;
        operator: string;
        value: string;
      }[];
    };
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

  const pushEvent = (event, payload = {}, cb = () => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  console.log(currentStep);

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
      pushEvent("consumer_updated", { consumer: form, step_forward: true });
    }
  }

  function handleConsumerClick(consumer) {
    if (!consumer.comingSoon) {
      form.consumerKind = consumer.id;
      pushEvent("consumer_updated", { consumer: form, step_forward: true });
    }
  }

  function isComingSoon(index) {
    return streamTypes[index].comingSoon;
  }

  const tables = [
    { name: "mydb → public.users" },
    { name: "mydb → public.orders" },
  ];

  function getColorFromTableName(tableName: string): string {
    const colors = [
      "bg-red-100",
      "bg-blue-100",
      "bg-green-100",
      "bg-yellow-100",
      "bg-indigo-100",
      "bg-purple-100",
      "bg-pink-100",
      "bg-teal-100",
    ];
    const hash = tableName.split("").reduce((acc, char) => {
      return char.charCodeAt(0) + ((acc << 5) - acc);
    }, 0);
    const index = Math.abs(hash) % colors.length;
    return colors[index];
  }

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

  function addFilter() {
    form.sourceTable.filters = [
      ...form.sourceTable.filters,
      { column: "", operator: "", value: "" },
    ];
  }

  function removeFilter(index: number) {
    form.sourceTable.filters = form.sourceTable.filters.filter(
      (_, i) => i !== index
    );
  }

  function updateFilter(
    index: number,
    key: keyof { column: string; operator: string; value: string },
    event: Event
  ) {
    const target = event.target as HTMLInputElement | HTMLSelectElement;
    form.sourceTable.filters = form.sourceTable.filters.map((filter, i) =>
      i === index ? { ...filter, [key]: target.value } : filter
    );
  }

  function handleConsumerConfigSubmit() {
    pushEvent("consumer_updated", { consumer: form, step_forward: true });
  }

  let activeInfo = "";

  const infoContent = {
    name: "Choose a unique name for your consumer. This name will be used to identify the consumer in your system.",
    visibilityTimeout:
      "The visibility timeout is the duration that a message is hidden from other consumers after it's been received. This prevents multiple consumers from processing the same message simultaneously. If the message isn't processed within this time, it becomes visible to other consumers again.",
    maxAckPending:
      "Max ack pending sets the maximum number of messages that can be pending acknowledgment at any time. This helps control the flow of messages and prevents overwhelming the consumer.",
    maxWaiting:
      "Max waiting determines the maximum number of waiting pull requests. This setting helps manage resource utilization and prevents excessive polling.",
  };

  function setActiveInfo(key: keyof typeof infoContent) {
    activeInfo = infoContent[key];
  }

  // Add this hard-coded data
  const sourceTable = {
    name: "mydb → public.users",
    color: "bg-red-100",
    filters: [
      { column: "org_id", operator: "=", value: "acme_1234" },
      { column: "status", operator: "=", value: "active" },
    ],
  };
</script>

<div class="flex flex-col min-h-screen">
  <NewTopnav {currentStep} />
  <div class="flex-grow">
    {#if currentStep === "select_stream"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Select a stream type</h2>
        </div>
      </div>

      <div class="p-8 max-w-5xl mx-auto">
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
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

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mt-6">
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

        <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mt-6">
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
        <p>Placeholder for table selection step</p>
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
        <div class="p-6">
          {#each tables as table}
            <div class="mb-6">
              <div class="flex items-center space-x-2 mb-2">
                <div
                  class="w-4 h-4 rounded {getColorFromTableName(table.name)}"
                ></div>
                <span class="font-medium">{table.name}</span>
                <Button variant="outline" size="sm" class="ml-auto">
                  <RefreshCw class="w-4 h-4 mr-2" />
                  Refresh
                </Button>
              </div>
              {#each form.sourceTable.filters as filter, index}
                <div class="grid grid-cols-[1fr_1fr_1fr_auto] gap-4 mb-2">
                  <Select on:change={(e) => updateFilter(index, "column", e)}>
                    <SelectTrigger class="border-carbon-100">
                      <SelectValue placeholder="Select column" />
                    </SelectTrigger>
                    <SelectContent>
                      {#each columns as column}
                        <SelectItem value={column}>{column}</SelectItem>
                      {/each}
                    </SelectContent>
                  </Select>
                  <Select on:change={(e) => updateFilter(index, "operator", e)}>
                    <SelectTrigger class="border-carbon-100">
                      <SelectValue placeholder="Operator" />
                    </SelectTrigger>
                    <SelectContent>
                      {#each operators as operator}
                        <SelectItem value={operator}>{operator}</SelectItem>
                      {/each}
                    </SelectContent>
                  </Select>
                  <Input
                    type="text"
                    placeholder="Value"
                    value={filter.value}
                    on:input={(e) => updateFilter(index, "value", e)}
                  />
                  <div class="flex items-center justify-end">
                    <button
                      on:click={() => removeFilter(index)}
                      class="text-carbon-400 hover:text-carbon-600"
                    >
                      <icon class="hero-x-mark w-4 h-4" />
                    </button>
                  </div>
                </div>
              {/each}
              <Button
                variant="outline"
                size="sm"
                on:click={addFilter}
                class="mt-2"
              >
                <PlusCircle class="w-4 h-4 mr-2" />
                Add filter
              </Button>
            </div>
          {/each}
        </div>
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
                <Label for="name" class="flex items-center gap-2">
                  Name
                  <button
                    type="button"
                    on:click={() => setActiveInfo("name")}
                    class="text-muted-foreground"
                  >
                    <InfoIcon class="h-4 w-4" />
                  </button>
                </Label>
                <Input
                  id="name"
                  bind:value={form.name}
                  placeholder="Enter consumer name"
                  on:focus={() => setActiveInfo("name")}
                />
              </div>

              <div class="space-y-2">
                <Label for="visibility-timeout" class="flex items-center gap-2">
                  Visibility timeout
                  <button
                    type="button"
                    on:click={() => setActiveInfo("visibilityTimeout")}
                    class="text-muted-foreground"
                  >
                    <InfoIcon class="h-4 w-4" />
                  </button>
                </Label>
                <div class="flex items-center gap-2">
                  <Input
                    id="visibility-timeout"
                    type="number"
                    bind:value={form.visibilityTimeout}
                    class="w-24"
                    on:focus={() => setActiveInfo("visibilityTimeout")}
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

              <Accordion type="single" collapsible class="w-full">
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
                            on:click={() => setActiveInfo("maxAckPending")}
                            class="text-muted-foreground"
                          >
                            <InfoIcon class="h-4 w-4" />
                          </button>
                        </Label>
                        <Input
                          id="max-ack-pending"
                          type="number"
                          bind:value={form.maxAckPending}
                          on:focus={() => setActiveInfo("maxAckPending")}
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
                            on:click={() => setActiveInfo("maxWaiting")}
                            class="text-muted-foreground"
                          >
                            <InfoIcon class="h-4 w-4" />
                          </button>
                        </Label>
                        <Input
                          id="max-waiting"
                          type="number"
                          bind:value={form.maxWaiting}
                          on:focus={() => setActiveInfo("maxWaiting")}
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
              <p class="text-sm text-muted-foreground">
                {activeInfo ||
                  "Select a field or click an info icon to see more details."}
              </p>
            </CardContent>
          </Card>
        </div>
      </div>
    {/if}

    {#if currentStep === "confirmation"}
      <div
        class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
      >
        <div class="flex items-center container">
          <h2 class="text-xl font-semibold">Confirm consumer details</h2>
        </div>
      </div>
      <div class="p-8 max-w-5xl mx-auto">
        <h1 class="text-3xl font-bold mb-2">Create an HTTP pull consumer</h1>
        <h2 class="text-xl text-muted-foreground mb-6">
          Confirm details for {form.name || "New Consumer"}
        </h2>

        <div class="space-y-8">
          <div>
            <div class="grid grid-cols-2 items-center mb-4">
              <h3 class="text-lg font-semibold">Filters</h3>
              <div class="text-right">
                <Button
                  variant="outline"
                  size="sm"
                  on:click={() =>
                    pushEvent("step_back", { step: "configure_filters" })}
                >
                  <PencilIcon class="w-4 h-4 mr-2" />
                  Edit Filters
                </Button>
              </div>
            </div>
            <div class="flex items-center space-x-2 mb-2">
              <div class={`w-4 h-4 rounded ${sourceTable.color}`}></div>
              <span class="font-medium">{sourceTable.name}</span>
            </div>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Column</TableHead>
                  <TableHead>Operator</TableHead>
                  <TableHead>Value</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {#each sourceTable.filters as filter}
                  <TableRow>
                    <TableCell>{filter.column}</TableCell>
                    <TableCell>{filter.operator}</TableCell>
                    <TableCell>{filter.value}</TableCell>
                  </TableRow>
                {/each}
              </TableBody>
            </Table>
          </div>

          <div>
            <div class="grid grid-cols-2 items-center mb-4">
              <h3 class="text-lg font-semibold">Configuration</h3>
              <div class="text-right">
                <Button
                  variant="outline"
                  size="sm"
                  on:click={() =>
                    pushEvent("step_back", { step: "configure_consumer" })}
                >
                  <PencilIcon class="w-4 h-4 mr-2" />
                  Edit Configuration
                </Button>
              </div>
            </div>
            <Table>
              <TableBody>
                {#each ["name", "visibilityTimeout", "maxAckPending", "maxWaiting"] as key}
                  {#if form[key] !== null && form[key] !== undefined}
                    <TableRow>
                      <TableCell class="font-medium">{key}</TableCell>
                      <TableCell>{form[key]}</TableCell>
                    </TableRow>
                  {/if}
                {/each}
              </TableBody>
            </Table>
          </div>
        </div>
      </div>
    {/if}
  </div>

  <NewBottomnav
    {currentStep}
    onBack={() => pushEvent("step_back")}
    onContinue={() => pushEvent("step_forward")}
  />
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
