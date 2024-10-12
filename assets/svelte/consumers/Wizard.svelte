<script lang="ts">
  import { onMount } from "svelte";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import {
    InfoIcon,
    CheckIcon,
    Loader2,
    XIcon,
    CircleIcon,
    RefreshCwIcon,
    ExternalLink,
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
  import { toast } from "svelte-sonner";
  import * as Dialog from "$lib/components/ui/dialog";
  import * as Tabs from "$lib/components/ui/tabs";
  import { truncateMiddle } from "$lib/utils";
  import SortAndFilterCard from "../components/SortAndFilterCard.svelte";

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
      encryptedHeaders: Record<string, string>;
    };
    httpEndpointPath: string;
    sortColumnAttnum: number | null;
    recordConsumerState: {
      producer: "table_and_wal";
      initialMinSortCol: number | null | Date;
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
      encryptedHeaders: {},
    },
    httpEndpointPath: "",
    sortColumnAttnum: null,
    recordConsumerState: {
      producer: "table_and_wal",
      initialMinSortCol: null,
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
  let isCreatingConsumer = false;
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
      case "select_message_kind":
        continueDisabled = !form.messageKind;
        break;
      case "configure_filters":
        continueDisabled = form.sourceTableFilters.some(
          (filter) =>
            !filter.columnAttnum ||
            !filter.operator ||
            (!filter.value &&
              !["IS NULL", "IS NOT NULL"].includes(filter.operator))
        );
        if (form.messageKind === "record") {
          continueDisabled = continueDisabled || !form.sortColumnAttnum;
        }
        break;

      case "select_consumer":
        continueDisabled = !form.consumerKind;
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

  const pushEvent = (event, payload = {}, cb = (event: any) => {}) => {
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

  function handleMessageKindSelect(messageKind) {
    if (!messageKind.comingSoon) {
      handleFormUpdate({
        messageKind:
          form.messageKind === messageKind.id ? null : messageKind.id,
      });
    }
  }

  function handleConsumerSelect(consumer) {
    if (!consumer.comingSoon) {
      const newConsumerKind =
        form.consumerKind === consumer.id ? null : consumer.id;
      handleFormUpdate({
        consumerKind: newConsumerKind,
      });

      // Set the form name based on the selected table and consumer kind
      if (newConsumerKind && selectedTable) {
        const tableName = selectedTable.name;
        const consumerType = newConsumerKind === "http_push" ? "push" : "pull";
        const newName = `${tableName}_${consumerType}_consumer`;
        handleFormUpdate({ name: newName });
      }
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
    isCreatingConsumer = true;
    // Optimistically clear the form storage. If we hear back that the form is invalid, we'll restore it.
    // This is a workaround. If the form *is* valid, the reply from the server will be invalid, as we'll
    // be in the middle of a redirect.
    clearFormStorage();
    pushEvent("form_submitted", { form }, (event) => {
      // if the event was ok, we want to keep isCreatingConsumer=true while they are being redirected
      if (event?.ok === false) {
        isCreatingConsumer = false;
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
      case "select_message_kind":
        step = "select_table";
        break;
      case "configure_filters":
        step = "select_message_kind";
        break;
      case "select_consumer":
        step = "configure_filters";
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
        step = "select_message_kind";
        break;
      case "select_message_kind":
        step = "configure_filters";
        break;
      case "configure_filters":
        step = "select_consumer";
        break;
      case "select_consumer":
        step = "configure_consumer";
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
      if (result.http_endpoint_id) {
        pushEvent("refresh_http_endpoints", {}, () => {
          form.httpEndpointId = result.http_endpoint_id;
        });
      } else if (result.error) {
        toast.error("Failed to generate Webhook.site URL:", result.error);
      } else {
        toast.error("Failed to generate Webhook.site URL");
      }
    });
  }

  let showExample = false;

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

  // Add a new function to handle refreshing HTTP Endpoints
  function refreshHttpEndpoints() {
    httpEndpointsRefreshState = "refreshing";
    pushEvent("refresh_http_endpoints", {}, () => {
      httpEndpointsRefreshState = "done";
      setTimeout(() => {
        httpEndpointsRefreshState = "idle";
      }, 2000);
    });
  }

  // Add a reactive variable to track the refresh state
  let httpEndpointsRefreshState: "idle" | "refreshing" | "done" = "idle";
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

      {#if step === "select_message_kind"}
        <div
          class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
        >
          <div class="flex items-center container">
            <h2 class="text-xl font-semibold">
              What should the consumer process?
            </h2>
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
                      variant={form.messageKind === type.id
                        ? "outline"
                        : "default"}
                      on:click={() => handleMessageKindSelect(type)}
                      disabled={type.comingSoon}
                    >
                      {#if form.messageKind === type.id}
                        <CheckIcon class="w-4 h-4 mr-2" />
                        Selected
                      {:else}
                        {type.comingSoon ? "Coming Soon" : `Select`}
                      {/if}
                    </Button>
                    <Button
                      class="w-full"
                      variant="outline"
                      on:click={() => (showExample = true)}
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

      {#if step === "configure_filters"}
        <div
          class="flex w-full h-20 bg-canvas-subtle justify-center sticky top-0"
        >
          <div class="flex items-center container">
            <h2 class="text-xl font-semibold">Configure filters and sorting</h2>
          </div>
        </div>
        <div class="p-8 max-w-5xl mx-auto">
          <SortAndFilterCard
            showCardTitle={false}
            showTableInfo
            messageKind={form.messageKind}
            {selectedTable}
            bind:form
            {errors}
            isEditMode={false}
            onFilterChange={handleFilterChange}
          />
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
                class="bg-white border border-gray-200 overflow-hidden rounded-lg shadow flex flex-col justify-between"
              >
                <div class="p-4 flex flex-col gap-2">
                  <div class="h-80">
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
                  <Button
                    class="w-full mt-4"
                    variant={form.consumerKind === consumer.id
                      ? "outline"
                      : "default"}
                    on:click={() =>
                      !consumer.comingSoon && handleConsumerSelect(consumer)}
                  >
                    {#if form.consumerKind === consumer.id}
                      <CheckIcon class="w-4 h-4 mr-2" />
                      Selected
                    {:else}
                      {consumer.comingSoon ? "Coming Soon" : `Select`}
                    {/if}
                  </Button>
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
                  <div class="flex flex-col gap-1">
                    <Input
                      id="name"
                      bind:value={form.name}
                      placeholder="Enter consumer name"
                      on:input={() => handleFormUpdate({ name: form.name })}
                      on:focus={() => (activeInfo = "name")}
                    />
                    <p class="text-xs font-light">
                      Alphanumeric separated by underscores. No special
                      characters.
                    </p>
                  </div>
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
                    <h3 class="text-lg font-medium">HTTP Endpoint</h3>
                    {#if !form.httpEndpointId}
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
                    <div class="flex items-center space-x-2">
                      <Select
                        selected={{
                          value: form.httpEndpointId,
                          label:
                            selectedHttpEndpoint?.name || "Select an endpoint",
                        }}
                        onSelectedChange={(event) => {
                          form.httpEndpointId = event.value;
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
                        </SelectContent>
                      </Select>
                      <div class="flex items-center">
                        <Button
                          variant="outline"
                          size="sm"
                          on:click={refreshHttpEndpoints}
                          disabled={httpEndpointsRefreshState === "refreshing"}
                          class="p-2"
                          aria-label="Refresh HTTP Endpoints"
                        >
                          {#if httpEndpointsRefreshState === "refreshing"}
                            <RefreshCwIcon class="h-5 w-5 animate-spin" />
                          {:else if httpEndpointsRefreshState === "done"}
                            <CheckIcon class="h-5 w-5 text-green-500" />
                          {:else}
                            <RefreshCwIcon class="h-5 w-5" />
                          {/if}
                        </Button>
                        <a
                          href="/http-endpoints/new"
                          target="_blank"
                          class="ml-2"
                        >
                          <Button variant="outline" size="sm">
                            <ExternalLink class="h-4 w-4 mr-2" />
                            New HTTP Endpoint
                          </Button>
                        </a>
                      </div>
                    </div>

                    {#if errors.http_endpoint_id}
                      <p class="text-destructive text-sm">
                        Please select an HTTP endpoint
                      </p>
                    {/if}

                    {#if form.httpEndpointId}
                      <div class="space-y-2">
                        <Label for="http-endpoint-path"
                          >Consumer Endpoint Path</Label
                        >
                        <div class="flex flex-row bg-white">
                          <div
                            class="text-sm rounded-l px-4 h-10 flex items-center justify-center bg-muted border border-input whitespace-nowrap"
                          >
                            {truncateMiddle(selectedHttpEndpoint.baseUrl, 50)}
                          </div>
                          <Input
                            id="http-endpoint-path"
                            bind:value={form.httpEndpointPath}
                            placeholder="/webhook"
                            class="rounded-l-none focus-visible:ring-0 focus-visible:ring-offset-0"
                            style="border-left: none;"
                            on:input={() =>
                              handleFormUpdate({
                                httpEndpointPath: form.httpEndpointPath,
                              })}
                          />
                        </div>
                        <p class="text-sm text-muted-foreground">
                          The path to append to the base URL for this consumer's
                          requests.
                        </p>
                        {#if errors.http_endpoint_path}
                          <p class="text-destructive text-sm">
                            {errors.http_endpoint_path}
                          </p>
                        {/if}
                      </div>
                    {/if}

                    {#if form.httpEndpointId}
                      <div class="mt-4 space-y-2">
                        <Label>Fully Qualified URL</Label>
                        <div
                          class="flex items-center space-x-2 overflow-x-auto"
                        >
                          <p
                            class="text-xs w-fit font-mono bg-slate-50 pl-1 pr-4 py-1 border border-slate-100 rounded-md whitespace-nowrap"
                          >
                            {selectedHttpEndpoint.baseUrl.replace(
                              /\/$/,
                              ""
                            )}/{form.httpEndpointPath.replace(/^\//, "")}
                          </p>
                        </div>
                      </div>
                    {/if}
                  </div>
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
          {#if step !== "select_table"}
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
          {#if step === "configure_consumer"}
            <Button
              loading={isCreatingConsumer}
              on:click={onConsumerCreate}
              disabled={continueDisabled}
              >Create Consumer
              <span slot="loading">Creating...</span>
            </Button>
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

<Dialog.Root bind:open={showExample}>
  <Dialog.Portal>
    <Dialog.Overlay />
    <Dialog.Content class="max-w-5xl max-h-[calc(100vh-2rem)] flex flex-col">
      <div class="grid grid-cols-2 gap-4">
        <div>
          <Dialog.Header class="mb-4">
            <Dialog.Title>Change Example</Dialog.Title>
          </Dialog.Header>
          <Tabs.Root value="insert" class="w-full mb-4">
            <Tabs.List class="grid grid-cols-3 mb-4">
              <Tabs.Trigger value="insert" class="w-full">Insert</Tabs.Trigger>
              <Tabs.Trigger value="update" class="w-full">Update</Tabs.Trigger>
              <Tabs.Trigger value="delete" class="w-full">Delete</Tabs.Trigger>
            </Tabs.List>
            <div class="">
              <Tabs.Content value="insert">
                <pre class="bg-slate-100 p-4 rounded-md overflow-x-auto"><code
                    >{JSON.stringify(
                      changeExamples.insert,
                      null,
                      2
                    ).trim()}</code
                  ></pre>
              </Tabs.Content>
              <Tabs.Content value="update">
                <pre class="bg-slate-100 p-4 rounded-md overflow-x-auto"><code
                    >{JSON.stringify(
                      changeExamples.update,
                      null,
                      2
                    ).trim()}</code
                  ></pre>
              </Tabs.Content>
              <Tabs.Content value="delete">
                <pre class="bg-slate-100 p-4 rounded-md overflow-x-auto"><code
                    >{JSON.stringify(
                      changeExamples.delete,
                      null,
                      2
                    ).trim()}</code
                  ></pre>
              </Tabs.Content>
            </div>
          </Tabs.Root>
        </div>
        <div>
          <Dialog.Header class="mb-4">
            <Dialog.Title>Row Example</Dialog.Title>
          </Dialog.Header>
          <div
            class="h-[40px] mb-4 bg-slate-100 p-4 flex items-center justify-start text-center rounded-md text-sm font-medium"
          >
            The latest state of the row is always sent. No deletes.
          </div>
          <pre class="bg-slate-100 p-4 rounded-md overflow-x-auto"><code
              >{JSON.stringify(rowExample, null, 2).trim()}</code
            ></pre>
        </div>
      </div>
      <Dialog.Footer class="mt-auto">
        <Button on:click={() => (showExample = false)}>Close</Button>
      </Dialog.Footer>
      <Dialog.Close />
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>

<style lang="postcss">
  .container {
    max-width: 1288px;
  }
</style>
