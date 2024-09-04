<script lang="ts">
  import { onMount } from "svelte";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import {
    InfoIcon,
    RefreshCwIcon,
    CheckIcon,
    Loader2,
    XIcon,
    CircleIcon,
    ExternalLinkIcon,
  } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
    CardDescription,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Badge } from "$lib/components/ui/badge";

  import HttpPushVisual from "./HttpPushVisual.svelte";
  import HttpPullVisual from "./HttpPullVisual.svelte";
  import TableSelector from "../components/TableSelector.svelte";
  import TableFilters from "../components/TableFilters.svelte";
  import { getColorFromName } from "../utils";
  import HttpEndpointForm from "../http_endpoints/FormBody.svelte";
  import { Switch } from "$lib/components/ui/switch";
  import { toast } from "svelte-sonner";
  import * as Dialog from "$lib/components/ui/dialog";
  import * as Tabs from "$lib/components/ui/tabs";

  let step = "select_table";
  export let live;
  export let parent;
  let form: {
    postgresDatabaseId: string;
    tableOid: number | null;
    sourceTableFilters: {
      columnAttnum: number | null;
      operator: string | null;
      value: string;
      valueType: string;
    }[];
    sourceTableActions: string[];
    messageKind: string;
    consumerKind: string;
    name: string;
    ackWaitMs: number;
    maxAckPending: number;
    maxWaiting: number;
    httpEndpointId: string | null;
    httpEndpoint: {
      name: string;
      baseUrl: string;
      headers: Record<string, string>;
    };
  } = {
    postgresDatabaseId: null,
    tableOid: null,
    sourceTableFilters: [],
    sourceTableActions: ["insert", "update", "delete"],
    messageKind: null,
    consumerKind: null,
    name: "",
    ackWaitMs: 30000,
    maxAckPending: 100,
    maxWaiting: 100,
    httpEndpointId: null,
    httpEndpoint: {
      name: "",
      baseUrl: "",
      headers: {},
    },
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

  export let httpEndpoints: Array<{
    id: string;
    name: string;
    baseUrl: string;
  }>;

  export let errors: any = {};

  let showConfirmOnExit = false;
  let formLoadedFromLocalStorage = false;

  $: {
    // Set to true when form is touched
    if (form) {
      showConfirmOnExit = true;
    }
  }

  let continueDisabled = true;
  $: {
    switch (step) {
      case "select_table":
        continueDisabled = !form.postgresDatabaseId || !form.tableOid;
        break;
      case "configure_filters":
        continueDisabled = form.sourceTableFilters.some(
          (filter) => !filter.columnAttnum || !filter.operator || !filter.value
        );
        break;
      case "configure_consumer":
        if (form.consumerKind === "http_push" && form.httpEndpointId) {
          continueDisabled = !form.name;
        } else if (form.consumerKind === "http_push") {
          continueDisabled =
            !form.name || !form.httpEndpoint.baseUrl || !form.httpEndpoint.name;
        } else if (form.consumerKind === "http_pull") {
          continueDisabled = !form.name;
        }
        break;
    }
  }

  const pushEvent = (event, payload = {}, cb = (event?: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  const messageTypes = [
    {
      id: "event",
      title: "Changes",
      description:
        "Receive every create, update, and delete that happens to rows.",
      comingSoon: false,
      source: "Source: WAL",
      replacements: ["Postgres triggers", "SQS", "ORM hooks", "WAL events"],
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
      id: "record",
      title: "Rows",
      description: "Receive the latest version of rows whenever they change.",
      comingSoon: false,
      source: "Source: Table",
      replacements: ["Kafka", { text: "COPY", style: "code" }, "Manual syncs"],
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

  function handleStreamClick(messageKind) {
    if (!messageKind.comingSoon) {
      handleFormUpdate({ messageKind: messageKind.id });
      pushEvent("form_updated", { form });
      step = "select_consumer";
      saveFormToStorage();
    }
  }

  function handleConsumerClick(consumer) {
    if (!consumer.comingSoon) {
      handleFormUpdate({ consumerKind: consumer.id });
      pushEvent("form_updated", { form });
      step = "configure_consumer";
      saveFormToStorage();
    }
  }

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
    pushEvent("form_updated", { form });
    step = "configure_filters";
  }

  let activeInfo = "";

  const ackWaitMsInfo =
    form.consumerKind === "http_push"
      ? [
          "The maximum duration allowed for the HTTP request to complete.",
          "If the request doesn't finish within this time, it will be considered failed and may be retried.",
        ]
      : [
          "The visibility timeout is the duration that a message is hidden from other consumers after it's been received.",
          "This prevents multiple consumers from processing the same message simultaneously.",
          "If the message isn't processed within this time, it becomes visible to other consumers again.",
        ];

  const infoContent = {
    name: [
      "Choose a unique name for your consumer.",
      "You'll use this name in HTTP requests to Sequin.",
    ],
    ackWaitMs: ackWaitMsInfo,
    maxAckPending: [
      "Max ack pending sets the maximum number of messages that can be pending acknowledgment at any time.",
      "This helps control the flow of messages and prevents overwhelming the consumer.",
    ],
    maxWaiting: [
      "Max waiting determines the maximum number of waiting pull requests.",
      "This setting helps manage resource utilization and prevents excessive polling.",
    ],
  };

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    handleFormUpdate({
      postgresDatabaseId: event.databaseId,
      tableOid: event.tableOid,
    });
  }

  function onConsumerCreate() {
    // Optimistically clear the form storage. If we hear back that the form is invalid, we'll restore it.
    // This is a workaround. If the form *is* valid, the reply from the server will be invalid, as we'll
    // be in the middle of a redirect.
    clearFormStorage();
    pushEvent("form_submitted", { form }, (event) => {
      if (event?.ok === false) {
        saveFormToStorage();
      }
    });
  }

  function handleFilterChange(newFilters) {
    handleFormUpdate({
      sourceTableFilters: newFilters.map((filter) => ({
        ...filter,
        column: filter.column ? parseInt(filter.column, 10) : null,
      })),
    });
  }

  function handleFormUpdate(updatedForm: Partial<typeof form>) {
    form = { ...form, ...updatedForm };
    pushEvent("form_updated", { form });
    saveFormToStorage();
  }

  function goBack() {
    switch (step) {
      case "configure_filters":
        step = "select_table";
        break;
      case "select_message_kind":
        step = "configure_filters";
        break;
      case "select_consumer":
        step = "select_message_kind";
        break;
      case "configure_consumer":
        step = "select_consumer";
        break;
    }
    saveFormToStorage();
  }

  function goForward() {
    switch (step) {
      case "select_table":
        step = "configure_filters";
        break;
      case "configure_filters":
        step = "select_message_kind";
        break;
    }
    saveFormToStorage();
  }

  function handleClose() {
    clearFormStorage();
    pushEvent("form_closed");
  }

  $: navSteps = [
    {
      id: "selectTable",
      label: "Select table",
      active: step === "select_table" || step === "configure_filters",
    },
    {
      id: "selectConsumer",
      label: "Select consumer",
      active: step === "select_message_kind" || step === "select_consumer",
    },
    {
      id: "configure",
      label: "Configure consumer",
      active: step === "configure_consumer",
    },
  ];

  let showNewHttpEndpointForm = false;
  let selectedHttpEndpoint: {
    id: string;
    name: string;
    baseUrl: string;
  } | null = null;

  $: {
    if (form.httpEndpointId) {
      selectedHttpEndpoint = httpEndpoints.find(
        (endpoint) => endpoint.id === form.httpEndpointId
      );
    }
  }

  const STORAGE_KEY = "sequin_consumer_wizard_form";
  const STORAGE_EXPIRY = 2 * 60 * 60 * 1000; // 2 hours in milliseconds

  const initialFormKeys = Object.keys(form);

  onMount(() => {
    const storedForm = localStorage.getItem(STORAGE_KEY);
    if (storedForm) {
      try {
        const parsedForm = JSON.parse(storedForm);
        const storedTimestamp = parsedForm._timestamp;
        const storedStep = parsedForm._step;
        console.log("parsedForm", parsedForm);

        delete parsedForm._timestamp;
        delete parsedForm._step;

        const currentTime = new Date().getTime();
        const isExpired = currentTime - storedTimestamp > STORAGE_EXPIRY;
        const hasSameKeys =
          JSON.stringify(Object.keys(parsedForm).sort()) ===
          JSON.stringify(initialFormKeys.sort());

        if (!isExpired && hasSameKeys) {
          form = parsedForm;
          step = storedStep || "select_table"; // Use stored step or default to "select_table"
        } else {
          localStorage.removeItem(STORAGE_KEY);
        }
      } catch (error) {
        console.error("Error parsing stored form:", error);
        localStorage.removeItem(STORAGE_KEY);
      }
    }

    formLoadedFromLocalStorage = true;
  });

  function saveFormToStorage() {
    // There's a race on mount. Don't start saving the form until we've loaded it from local storage.
    if (!formLoadedFromLocalStorage) {
      return;
    }

    const formToSave = {
      ...form,
      _timestamp: new Date().getTime(),
      _step: step,
    };
    console.log("formToSave", formToSave);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(formToSave));
  }

  function clearFormStorage() {
    localStorage.removeItem(STORAGE_KEY);
  }

  let isGeneratingWebhookSite = false;

  function createWebhookSiteEndpoint() {
    isGeneratingWebhookSite = true;
    pushEvent("generate_webhook_site_url", {}, (result: any) => {
      isGeneratingWebhookSite = false;
      if (result.url && result.name) {
        form.httpEndpoint = {
          name: result.name,
          baseUrl: result.url,
          headers: {},
        };
        showNewHttpEndpointForm = true;
      } else if (result.error) {
        toast.error("Failed to generate Webhook.site URL:", result.error);
      } else {
        toast.error("Failed to generate Webhook.site URL");
      }
    });
  }

  let showRowExample = false;
  let showChangeExample = false;

  const rowExample = {
    record: {
      id: 1,
      name: "Paul Atreides",
      title: "Duke of Arrakis",
      spice_allocation: 1000,
      is_kwisatz_haderach: true,
    },
    metadata: {
      table_schema: "public",
      table_name: "house_atreides_members",
      consumer: {
        id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        name: "dune_characters_consumer",
      },
    },
  };

  const changeExamples = {
    insert: {
      record: {
        id: 2,
        name: "Chani",
        title: "Fremen Warrior",
        spice_allocation: 500,
        is_sayyadina: true,
      },
      changes: null,
      action: "insert",
      metadata: {
        table_schema: "public",
        table_name: "fremen_members",
        commit_timestamp: "2023-10-15T14:30:00Z",
        consumer: {
          id: "e2f9a3b1-7c6d-4b5a-9f8e-1d2c3b4a5e6f",
          name: "arrakis_population_consumer",
        },
      },
    },
    update: {
      record: {
        id: 1,
        name: "Paul Atreides",
        title: "Emperor of the Known Universe",
        spice_allocation: 10000,
        is_kwisatz_haderach: true,
      },
      changes: {
        title: "Duke of Arrakis",
        spice_allocation: 1000,
      },
      action: "update",
      metadata: {
        table_schema: "public",
        table_name: "house_atreides_members",
        commit_timestamp: "2023-10-16T09:45:00Z",
        consumer: {
          id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
          name: "dune_characters_consumer",
        },
      },
    },
    delete: {
      record: {
        id: 3,
        name: "Baron Vladimir Harkonnen",
        title: "Baron of House Harkonnen",
        spice_allocation: 5000,
        is_mentat: false,
      },
      changes: null,
      action: "delete",
      metadata: {
        table_schema: "public",
        table_name: "house_harkonnen_members",
        commit_timestamp: "2023-10-17T18:20:00Z",
        consumer: {
          id: "a1b2c3d4-e5f6-4a5b-8c7d-9e0f1a2b3c4d",
          name: "landsraad_members_consumer",
        },
      },
    },
  };

  function showExample(type: string) {
    if (type === "event") {
      showChangeExample = true;
    } else if (type === "record") {
      showRowExample = true;
    }
  }
</script>

<FullPageModal {showConfirmOnExit} on:close={handleClose} bodyPadding={0}>
  <svelte:fragment slot="header">
    <div class="flex items-center justify-between w-full px-8">
      <div class="flex items-center justify-center flex-grow mx-8">
        {#each navSteps as step, index}
          {#if index > 0}
            <div class="flex h-px bg-carbon-100 flex-grow mx-4 max-w-[48px]" />
          {/if}
          <div class="flex gap-2 items-center">
            <div
              class="flex-shrink-0 flex items-center justify-center w-4 h-4 rounded-full transition-all duration-300 ease-in-out
                {step.active
                ? 'bg-matcha-600 border-2 border-matcha-200 bg-slate-500'
                : 'border border-carbon-300'}"
            />
            <div class="hidden sm:block">
              <span
                class="text-md leading-5 font-semibold
                  {step.active ? 'text-matcha-600' : 'text-carbon-600'}"
              >
                {step.label}
              </span>
            </div>
          </div>
        {/each}
      </div>
    </div>
  </svelte:fragment>

  <div class="flex flex-col">
    <div class="flex-grow" style="min-width: 750px;">
      {#if step === "select_table"}
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

      {#if step === "configure_filters"}
        <div
          class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
        >
          <div class="flex items-center container">
            <h2 class="text-xl font-semibold">Define filters</h2>
          </div>
        </div>
        <div class="p-8 max-w-5xl mx-auto">
          <Card>
            <CardHeader>
              <CardTitle>Filters</CardTitle>
            </CardHeader>
            <CardContent>
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
                      {#if form.sourceTableFilters.length > 0}
                        <Button
                          variant="outline"
                          size="sm"
                          on:click={() =>
                            refreshTables(form.postgresDatabaseId)}
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
                      {/if}
                    </div>
                    <TableFilters
                      filters={form.sourceTableFilters}
                      columns={selectedTable.columns}
                      onFilterChange={handleFilterChange}
                    />
                    {#if form.messageKind === "event"}
                      <div class="mt-6 space-y-2">
                        <Label>Operations to capture</Label>
                        <div class="flex items-center space-x-4">
                          {#each ["insert", "update", "delete"] as action}
                            <div class="flex items-center space-x-2">
                              <Label for={action} class="cursor-pointer">
                                {action.charAt(0).toUpperCase() +
                                  action.slice(1)}
                              </Label>
                              <Switch
                                id={action}
                                checked={form.sourceTableActions.includes(
                                  action
                                )}
                                onCheckedChange={(checked) => {
                                  const newActions = checked
                                    ? [...form.sourceTableActions, action]
                                    : form.sourceTableActions.filter(
                                        (a) => a !== action
                                      );
                                  handleFormUpdate({
                                    sourceTableActions: newActions,
                                  });
                                }}
                              />
                            </div>
                          {/each}
                        </div>
                        {#if errors.source_tables?.[0]?.actions}
                          <p class="text-destructive text-sm">
                            {errors.source_tables[0].actions}
                          </p>
                        {/if}
                      </div>
                    {/if}
                  </div>
                {:else}
                  <p>Loading table information...</p>
                {/if}
              {:else}
                <p>Please select a database and table first.</p>
              {/if}
            </CardContent>
          </Card>
        </div>
      {/if}
      {#if step === "select_message_kind"}
        <div
          class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
        >
          <div class="flex items-center justify-between container">
            <h2 class="text-xl font-semibold">
              What should the consumer process?
            </h2>
            <Button
              variant="outline"
              href="https://sequinstream.com/docs/core-concepts#rows-and-changes"
              target="_blank"
              class="flex items-center"
            >
              <span class="mr-1">Read docs</span>
              <ExternalLinkIcon class="w-4 h-4" />
            </Button>
          </div>
        </div>
        <div class="p-8 max-w-5xl mx-auto">
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            {#each messageTypes as type}
              <Card class="w-full grid grid-rows-[auto_1fr_auto] h-full">
                <CardHeader class="h-[8.125em]">
                  <div class="flex justify-between items-center mb-2">
                    <CardTitle>{type.title}</CardTitle>
                    <Badge variant="secondary">{type.source}</Badge>
                  </div>
                  <p class="text-muted-foreground">{type.description}</p>
                </CardHeader>
                <CardContent class="grid grid-rows-[8.75em_1fr] gap-4">
                  <div>
                    <h4 class="font-semibold mb-2">Replacement for:</h4>
                    <ul class="list-disc pl-5 space-y-1">
                      {#each type.replacements as replacement}
                        <li>
                          {#if typeof replacement === "string"}
                            {replacement}
                          {:else if replacement.style === "code"}
                            <code class="bg-muted px-1 py-0.5 rounded"
                              >{replacement.text}</code
                            >
                          {:else}
                            {replacement.text}
                          {/if}
                        </li>
                      {/each}
                    </ul>
                  </div>
                  <div>
                    <h4 class="font-semibold mb-2">Features:</h4>
                    <ul class="space-y-2">
                      {#each type.features as feature}
                        <li class="flex items-center">
                          {#if feature.attribute === "positive"}
                            <CheckIcon class="w-4 h-4 mr-2 text-green-500" />
                          {:else if feature.attribute === "negative"}
                            <XIcon class="w-4 h-4 mr-2 text-red-500" />
                          {:else}
                            <CircleIcon class="w-4 h-4 mr-2 text-gray-400" />
                          {/if}
                          <span>{feature.text}</span>
                        </li>
                      {/each}
                    </ul>
                  </div>
                </CardContent>
                <CardContent>
                  <div class="flex flex-col gap-2 justify-between items-center">
                    <Button
                      class="w-full"
                      on:click={() => handleStreamClick(type)}
                      disabled={type.comingSoon}
                    >
                      {type.comingSoon ? "Coming Soon" : `Select`}
                    </Button>
                    <Button
                      class="w-full"
                      variant="outline"
                      on:click={() => showExample(type.id)}
                    >
                      See example
                    </Button>
                  </div>
                </CardContent>
              </Card>
            {/each}
          </div>
        </div>
      {/if}

      {#if step === "select_consumer"}
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
                      Sequin will send updates from Postgres tables to an
                      endpoint you specify.
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

      {#if step === "configure_consumer"}
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
                Create an HTTP {form.consumerKind === "http_pull"
                  ? "pull"
                  : "push"} consumer
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
                    on:input={() => handleFormUpdate({ name: form.name })}
                    on:focus={() => (activeInfo = "name")}
                  />
                  {#if errors.name}
                    <p class="text-destructive text-sm">{errors.name[0]}</p>
                  {/if}
                </div>

                <div class="space-y-2">
                  <Label for="ack-wait-ms" class="flex items-center gap-2">
                    {form.consumerKind === "http_push"
                      ? "Request timeout"
                      : "Visibility timeout"}
                    <span> (ms)</span>
                    <button
                      type="button"
                      on:click={() => (activeInfo = "ackWaitMs")}
                      class="text-muted-foreground"
                    >
                      <InfoIcon class="h-4 w-4" />
                    </button>
                  </Label>
                  <Input
                    id="ack-wait-ms"
                    type="number"
                    bind:value={form.ackWaitMs}
                    on:input={() =>
                      handleFormUpdate({ ackWaitMs: form.ackWaitMs })}
                    on:focus={() => (activeInfo = "ackWaitMs")}
                  />
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
                            on:input={() =>
                              handleFormUpdate({
                                maxAckPending: form.maxAckPending,
                              })}
                            on:focus={() => (activeInfo = "maxAckPending")}
                          />
                        </div>
                        {#if form.consumerKind === "http_pull"}
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
                              on:input={() =>
                                handleFormUpdate({
                                  maxWaiting: form.maxWaiting,
                                })}
                              on:focus={() => (activeInfo = "maxWaiting")}
                            />
                          </div>
                        {/if}
                      </div>
                    </AccordionContent>
                  </AccordionItem>
                </Accordion>

                {#if form.consumerKind === "http_push"}
                  <div class="space-y-2">
                    <Label for="http-endpoint">HTTP Endpoint</Label>
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
                    <Select
                      selected={{
                        value: form.httpEndpointId,
                        label:
                          selectedHttpEndpoint?.name ||
                          (showNewHttpEndpointForm
                            ? "+ Add new"
                            : "Select an endpoint"),
                      }}
                      onSelectedChange={(event) => {
                        if (event.value === "new") {
                          form.httpEndpointId = null;
                          showNewHttpEndpointForm = true;
                        } else {
                          form.httpEndpointId = event.value;
                          showNewHttpEndpointForm = false;
                        }
                        handleFormUpdate({
                          httpEndpointId: form.httpEndpointId,
                        });
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
                      bind:form={form.httpEndpoint}
                      errors={errors.http_endpoint || {}}
                    />
                  {:else if errors.http_endpoint_id || errors.http_endpoint}
                    <p class="text-destructive text-sm">
                      Please select or create an HTTP endpoint
                    </p>
                  {/if}
                {/if}
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
  </div>

  <!-- Bottom Nav -->
  <svelte:fragment slot="footer">
    <div class="flex flex-shrink-0 h-16 justify-center w-full">
      <div class="flex items-center justify-between w-full px-8 max-w-[1288px]">
        <div>
          {#if step !== "select_message_kind"}
            <Button variant="outline" on:click={goBack}>
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
          {#if step === "select_message_kind"}
            <p class="text-sm leading-4 text-carbon-200 font-semibold">
              Select changes or rows to continue
            </p>
          {:else if step === "select_consumer"}
            <p class="text-sm leading-4 text-carbon-200 font-semibold">
              Select a consumer type to continue
            </p>
          {:else if step === "configure_consumer"}
            <Button on:click={onConsumerCreate} disabled={continueDisabled}
              >Create Consumer</Button
            >
          {:else}
            <Button on:click={goForward} disabled={continueDisabled}>
              Continue
              <icon class="w-4 h-4 ml-2 hero-arrow-right" aria-hidden="true" />
            </Button>
          {/if}
        </div>
      </div>
    </div>
  </svelte:fragment>
</FullPageModal>

<Dialog.Root bind:open={showRowExample}>
  <Dialog.Portal>
    <Dialog.Overlay />
    <Dialog.Content class="max-w-3xl">
      <Dialog.Header class="mb-4">
        <Dialog.Title>Row Example</Dialog.Title>
      </Dialog.Header>
      <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
          >{JSON.stringify(rowExample, null, 2).trim()}</code
        ></pre>
      <Dialog.Footer>
        <Button on:click={() => (showRowExample = false)}>Close</Button>
      </Dialog.Footer>
      <Dialog.Close />
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>

<Dialog.Root bind:open={showChangeExample}>
  <Dialog.Portal>
    <Dialog.Overlay />
    <Dialog.Content class="max-w-3xl">
      <Dialog.Header class="mb-4">
        <Dialog.Title>Change Example</Dialog.Title>
      </Dialog.Header>
      <Tabs.Root value="insert" class="w-full mb-4">
        <Tabs.List class="grid grid-cols-3 mb-4">
          <Tabs.Trigger value="insert" class="w-full">Insert</Tabs.Trigger>
          <Tabs.Trigger value="update" class="w-full">Update</Tabs.Trigger>
          <Tabs.Trigger value="delete" class="w-full">Delete</Tabs.Trigger>
        </Tabs.List>
        <Tabs.Content value="insert">
          <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
              >{JSON.stringify(changeExamples.insert, null, 2).trim()}</code
            ></pre>
        </Tabs.Content>
        <Tabs.Content value="update">
          <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
              >{JSON.stringify(changeExamples.update, null, 2).trim()}</code
            ></pre>
        </Tabs.Content>
        <Tabs.Content value="delete">
          <pre class="bg-gray-100 p-4 rounded-md overflow-x-auto"><code
              >{JSON.stringify(changeExamples.delete, null, 2).trim()}</code
            ></pre>
        </Tabs.Content>
      </Tabs.Root>
      <Dialog.Footer>
        <Button on:click={() => (showChangeExample = false)}>Close</Button>
      </Dialog.Footer>
      <Dialog.Close />
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>

<style lang="postcss">
  .container {
    max-width: 1288px;
  }

  .message-kind-box {
    @apply bg-white border border-border rounded-lg p-6 hover:shadow-md transition-shadow cursor-pointer;
    height: 180px;
    display: flex;
    flex-direction: column;
  }

  .message-kind-box.disabled {
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
