<script lang="ts">
  import FullPageForm from "../components/FullPageForm.svelte";
  import EditableArgument from "./EditableArgument.svelte";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import * as Command from "$lib/components/ui/command";
  import {
    Check,
    ChevronsUpDown,
    Loader2,
    Trash2,
    Info,
    BookText,
    RotateCcw,
  } from "lucide-svelte";
  import { cn } from "$lib/utils";
  import { tick } from "svelte";
  import { Label } from "$lib/components/ui/label";
  import { onMount, onDestroy } from "svelte";
  import FunctionTransformSnippet from "$lib/mdx/function-transform-snippet.mdx";
  import {
    Popover,
    PopoverContent,
    PopoverTrigger,
  } from "$lib/components/ui/popover";
  import {
    AlertDialog,
    AlertDialogAction,
    AlertDialogCancel,
    AlertDialogContent,
    AlertDialogDescription,
    AlertDialogFooter,
    AlertDialogHeader,
    AlertDialogTitle,
  } from "$lib/components/ui/alert-dialog";
  import CopyToClipboard from "$lib/components/ui/CopyToClipboard.svelte";
  import DeleteFunctionDialog from "$lib/components/DeleteFunctionDialog.svelte";
  import { clearStorage } from "./messageStorage";
  import {
    saveFunctionCodeToStorage,
    loadFunctionCodeFromStorage,
    clearFunctionCodeStorage,
  } from "./functionCodeStorage";
  import {
    saveFunctionTypeToStorage,
    saveSinkTypeToStorage,
    loadFunctionTypeFromStorage,
    loadSinkTypeFromStorage,
    clearFunctionTypeStorage,
  } from "./functionTypeStorage";
  import type {
    FormData,
    FormErrors,
    TestMessage,
    Consumer,
    FieldType,
    ActionType,
  } from "./types";
  import { FieldValues } from "./types";

  // CodeMirror imports
  import { EditorView, basicSetup } from "codemirror";
  import { EditorState } from "@codemirror/state";
  import { elixir } from "codemirror-lang-elixir";
  import { keymap } from "@codemirror/view";
  import { indentWithTab } from "@codemirror/commands";
  import { autocompletion } from "@codemirror/autocomplete";

  import {
    Tooltip,
    TooltipContent,
    TooltipTrigger,
  } from "$lib/components/ui/tooltip";

  export let formData: FormData;
  export let formErrors: FormErrors = {};
  export let showErrors: boolean = false;
  export let testMessages: TestMessage[] = [];
  export let syntheticTestMessages: TestMessage[] = [];
  export let parent: string;
  export let live;
  export let usedByConsumers: Consumer[] = [];
  export let initialCodeMap: Record<string, string>;
  export let functionTransformsEnabled: boolean;
  export let functionCompletions: Array<{
    label: string;
    type: string;
    info: string;
  }>;

  let saving: boolean = false;
  let validating: boolean = false;
  let functionInternalToExternal = {
    path: "Path transform",
    transform: "Transform function",
    routing: "Routing function",
    filter: "Filter function",
  };

  let sinkTypeInternalToExternal = {
    http_push: "Webhook sink",
    redis_string: "Redis string sink",
  };

  let errorKeyOrder = ["description", "snippet", "line", "column"];

  let form: FormData = {
    ...formData,
    function: { ...formData.function },
    modified_test_messages: { ...formData.modified_test_messages },
  };
  let persistedCode = form.function.code;
  let isCodeModified = false;

  let isEditing = form.id !== null;
  let initialFormState = JSON.stringify(form);
  let isDirty = false;

  $: {
    isDirty = JSON.stringify(form) !== initialFormState;
  }
  let functionEditorElement: HTMLElement;
  let functionEditorView: EditorView;

  let copyTimeout: ReturnType<typeof setTimeout>;

  // CodeMirror custom completions for Elixir code
  const elixirCompletions = (context) => {
    const word = context.matchBefore(/[\w\.]+/);
    if (!word) return null;

    // Static completions that are always available
    const variableCompletions = [
      {
        label: "record",
        type: "variable",
        info: "The input record to transform",
      },
      {
        label: "changes",
        type: "variable",
        info: "The old values of the record",
      },
      {
        label: "metadata",
        type: "variable",
        info: "Additional metadata about the change",
      },
      {
        label: "action",
        type: "variable",
        info: "The type of change (insert/update/delete/read)",
      },
      {
        label: "metadata.commit_lsn",
        type: "property",
        info: "The LSN of the commit",
      },
      {
        label: "metadata.commit_timestamp",
        type: "property",
        info: "The timestamp of the commit",
      },
      {
        label: "metadata.table_name",
        type: "property",
        info: "The name of the table",
      },
      {
        label: "metadata.table_schema",
        type: "property",
        info: "The schema of the table",
      },
      {
        label: "metadata.database_name",
        type: "property",
        info: "The name of the database",
      },
      {
        label: "metadata.consumer.name",
        type: "property",
        info: "The name of the consumer",
      },
      {
        label: "metadata.consumer.id",
        type: "property",
        info: "The id of the consumer",
      },
      {
        label: "metadata.transaction_annotations",
        type: "property",
        info: "A user-specified map of key-value pairs that are used to store additional information about the transaction.",
      },
    ];

    return {
      from: word.from,
      options: [...variableCompletions, ...functionCompletions],
    };
  };

  function pushEvent(
    event: string,
    payload = {},
    callback = (event: any) => {},
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    if (usedByConsumers.length > 0) {
      showUpdateDialog = true;
    } else {
      clearMessageFieldsLocalStorage();
      clearFunctionCodeStorage(form.id);
      clearFunctionTypeStorage();
      saving = true;
      pushEvent("save", { function: form }, () => {
        saving = false;
      });
    }
  }

  function handleTypeSelect(event: any) {
    form.function.type = event.value;
    if (!isEditing) {
      saveFunctionTypeToStorage(event.value);
    }
    loadStoredOrInitialCode();
  }

  function handleRoutingSinkTypeSelect(event: any) {
    form.function.sink_type = event.value;
    if (!isEditing) {
      saveSinkTypeToStorage(event.value);
    }
    loadStoredOrInitialCode();
  }

  function checkIfCodeModified(code: string) {
    if (isEditing) {
      isCodeModified = persistedCode !== code;
    } else {
      const initialCode = initialCodeFor(
        form.function.type,
        form.function.sink_type,
      );
      isCodeModified = initialCode !== code;
    }
  }

  function loadStoredOrInitialCode() {
    const storedCode = loadFunctionCodeFromStorage(
      form.function.type,
      form.function.sink_type,
      form.id,
    );
    if (storedCode) {
      // Load stored code
      form.function.code = storedCode;
      checkIfCodeModified(storedCode);
    } else if (form.id == null && form.function.type) {
      // Load initial code
      const newInitialCode = initialCodeFor(
        form.function.type,
        form.function.sink_type,
      );
      form.function.code = newInitialCode;
    }

    functionEditorView.dispatch({
      changes: {
        from: 0,
        to: functionEditorView.state.doc.length,
        insert: form.function.code,
      },
    });
  }

  function deleteMessage(message: TestMessage, index: number) {
    if (
      deletedReplicationMessageTraceIdKeys.has(
        message.replication_message_trace_id,
      )
    ) {
      return;
    }

    deletedReplicationMessageTraceIdKeys.add(
      message.replication_message_trace_id,
    );

    testMessages = testMessages.filter(
      (m) =>
        !deletedReplicationMessageTraceIdKeys.has(
          m.replication_message_trace_id,
        ),
    );

    if (selectedMessageIndex >= index) {
      const updatedIndex = Math.max(selectedMessageIndex - 1, 0);
      selectMessage(updatedIndex);
    }

    validating = true;
    pushEvent(
      "delete_test_message",
      {
        replication_message_trace_id: message.replication_message_trace_id,
      },
      () => {
        validating = false;
      },
    );
  }

  let showUpdateDialog = false;
  let showDeleteDialog = false;

  ////////////////////////////////////////////////////////////////

  type Table = {
    oid: number;
    schema: string;
    name: string;
  };

  type Database = {
    id: string;
    name: string;
    tables: Array<Table>;
  };

  export let databases: Array<Database>;

  let selectedDatabaseId: string | undefined;
  let selectedTableOid: number | null;
  let selectedDatabase: Database | null = null;
  let selectedTable: Table | null = null;

  $: {
    if (selectedDatabaseId) {
      selectedDatabase = databases.find((db) => db.id === selectedDatabaseId);
    }
  }

  $: {
    if (selectedDatabase) {
      selectedTable =
        selectedDatabase.tables.find(
          (table) => table.oid === selectedTableOid,
        ) || null;
    }
  }

  let tableComboboxOpen = false;

  function closeTableComboboxAndFocusTrigger(triggerId: string) {
    tableComboboxOpen = false;
    tick().then(() => {
      document.getElementById(triggerId)?.focus();
    });
  }

  function handleTableSelectCombobox(tableOid: number) {
    if (selectedTableOid === tableOid) {
      return;
    }

    selectedTableOid = tableOid;
    selectedMessageIndex = 0;

    pushEvent("table_selected", {
      database_id: selectedDatabaseId,
      table_oid: selectedTableOid,
    });
  }

  // let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";
  // let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  function initialCodeFor(type: string, sinkType: string | null) {
    const key = type + (type === "routing" ? "_" + sinkType : "");
    return initialCodeMap[key];
  }

  function handleDatabaseSelect(event: any) {
    selectedDatabaseId = event.value;
    selectedTableOid = null; // Reset table selection when database changes
    testMessages = [];
  }

  // function refreshDatabases() {
  //   databaseRefreshState = "refreshing";
  //   pushEvent("refresh_databases", {}, () => {
  //     databaseRefreshState = "done";
  //     setTimeout(() => {
  //       databaseRefreshState = "idle";
  //     }, 2000);
  //   });
  // }

  // function refreshTables() {
  //   if (selectedDatabaseId) {
  //     tableRefreshState = "refreshing";
  //     pushEvent("refresh_tables", { database_id: selectedDatabaseId }, () => {
  //       tableRefreshState = "done";
  //       setTimeout(() => {
  //         tableRefreshState = "idle";
  //       }, 2000);
  //     });
  //   }
  // }

  let deletedReplicationMessageTraceIdKeys: Set<string> = new Set();
  let messagesToShow: TestMessage[] = [];
  let showSyntheticMessages = false;
  let selectedMessageIndex: number = 0;
  let selectedMessage: TestMessage | undefined;

  $: {
    testMessages = testMessages.filter(
      (m) =>
        !deletedReplicationMessageTraceIdKeys.has(
          m.replication_message_trace_id,
        ),
    );

    if (testMessages.length > 0) {
      messagesToShow = testMessages;
      showSyntheticMessages = false;
    } else {
      messagesToShow = syntheticTestMessages;
      showSyntheticMessages = true;
    }

    selectMessage(selectedMessageIndex);
  }

  function selectMessage(index: number) {
    selectedMessageIndex = index;

    const oldSelectedMessage = selectedMessage;
    selectedMessage = messagesToShow[selectedMessageIndex];

    // If there is an error for the submitted selectedMessage editable fields, keep the previous edited ones instead of relying on the ones returned from the server
    if (
      oldSelectedMessage &&
      formErrors.modified_test_messages &&
      oldSelectedMessage.replication_message_trace_id ===
        selectedMessage.replication_message_trace_id &&
      formErrors.modified_test_messages[
        selectedMessage.replication_message_trace_id
      ]
    ) {
      if (
        formErrors.modified_test_messages[
          selectedMessage.replication_message_trace_id
        ].record
      ) {
        selectedMessage.record = oldSelectedMessage.record;
      }
      if (
        formErrors.modified_test_messages[
          selectedMessage.replication_message_trace_id
        ].metadata
      ) {
        selectedMessage.metadata = oldSelectedMessage.metadata;
      }
      if (
        formErrors.modified_test_messages[
          selectedMessage.replication_message_trace_id
        ].action
      ) {
        selectedMessage.action = oldSelectedMessage.action;
      }
      if (
        formErrors.modified_test_messages[
          selectedMessage.replication_message_trace_id
        ].changes
      ) {
        selectedMessage.changes = oldSelectedMessage.changes;
      }
    }
  }

  onMount(() => {
    // Handle URL parameters for new functions
    if (!isEditing) {
      const urlParams = new URLSearchParams(window.location.search);

      const typeParam = urlParams.get("type");
      if (typeParam && functionInternalToExternal[typeParam]) {
        form.function.type = typeParam;
      } else {
        // Try to load from storage if not in URL params
        const storedType = loadFunctionTypeFromStorage();
        if (storedType && functionInternalToExternal[storedType]) {
          form.function.type = storedType;
        }
      }

      const sinkTypeParam = urlParams.get("sink_type");
      if (
        sinkTypeParam &&
        form.function.type === "routing" &&
        sinkTypeInternalToExternal[sinkTypeParam]
      ) {
        form.function.sink_type = sinkTypeParam;
      } else if (form.function.type === "routing") {
        // Try to load from storage if not in URL params
        const storedSinkType = loadSinkTypeFromStorage();
        if (storedSinkType && sinkTypeInternalToExternal[storedSinkType]) {
          form.function.sink_type = storedSinkType;
        }
      }
    }

    if (databases.length === 1 && !selectedDatabaseId) {
      handleDatabaseSelect({ value: databases[0].id });
    }

    if (selectedDatabase && selectedDatabase.tables.length === 1) {
      handleTableSelectCombobox(selectedDatabase.tables[0].oid);
    }

    // Initialize CodeMirror for function if it exists
    const startState = EditorState.create({
      doc: form.function.code || "",
      extensions: [
        basicSetup,
        elixir(),
        autocompletion({ override: [elixirCompletions] }),
        keymap.of([indentWithTab]),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            const code = update.state.doc.toString();
            form.function.code = code;
            saveFunctionCodeToStorage(form);
            checkIfCodeModified(code);
          }
        }),
      ],
    });

    functionEditorView = new EditorView({
      state: startState,
      parent: functionEditorElement,
    });

    loadStoredOrInitialCode();

    return () => {
      functionEditorView.destroy();
    };
  });

  $: {
    validating = true;
    pushEvent("validate", { function: form }, () => {
      validating = false;
    });
  }

  function handleCopyForChatGPT() {
    const codeErrors = formErrors.function?.code || [];

    const prompt = `I need help creating or modifying an Elixir function transform for Sequin. Here are the details:

Function Details:
- Name: ${form.name}
- Description: ${form.description}
- Current Function Code:
\`\`\`elixir
${form.function.code}
\`\`\`
${
  codeErrors.length > 0
    ? `
Validation Errors:
${codeErrors.map((error) => `- ${error}`).join("\n")}
`
    : ""
}

Test Message:
\`\`\`json
{
  "record": ${selectedMessage.record},
  "changes": ${selectedMessage.changes},
  "action": "${selectedMessage.action}",
  "metadata": ${selectedMessage.metadata}
}
\`\`\`

${showSyntheticMessages ? "Warning ⚠️: This is a synthetic test message. The actual data in your database may differ. If the specific data in the test message is important for the behavior of the function, please capture test messages from your database." : ""}

Documentation:
${FunctionTransformSnippet}

Please help me create or modify the Elixir function transform to achieve the desired transformation. The function should take the record, changes, action, and metadata as arguments and return the transformed data.`;

    return prompt;
  }

  function clearMessageFieldsLocalStorage() {
    syntheticTestMessages.concat(testMessages).forEach((message) => {
      FieldValues.forEach((field) => {
        clearStorage(message, field);
      });
    });
  }

  function handleClose() {
    clearMessageFieldsLocalStorage();
    clearFunctionCodeStorage(form.id);
    clearFunctionTypeStorage();
    pushEvent("form_closed");
  }

  function resetOriginalCode() {
    if (isEditing) {
      form.function.code = persistedCode;
    } else {
      form.function.code = initialCodeFor(
        form.function.type,
        form.function.sink_type,
      );
    }
    functionEditorView.dispatch({
      changes: {
        from: 0,
        to: functionEditorView.state.doc.length,
        insert: form.function.code,
      },
    });
    isCodeModified = false;
  }

  // Clean up timeout on component destroy
  onDestroy(() => {
    if (copyTimeout) {
      clearTimeout(copyTimeout);
    }
  });
</script>

<FullPageForm
  title={isEditing ? "Edit Function" : "New Function"}
  showConfirmOnExit={isDirty}
  enableEscapeClose={false}
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-4">
    <div
      class="w-full border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900 max-w-7xl mx-auto"
    >
      <div class="p-4 border-b border-slate-200 dark:border-slate-800">
        <div class="flex justify-between items-center">
          <h2 class="text-lg font-semibold tracking-tight">
            Function Configuration
          </h2>
          <a
            href="https://sequinstream.com/docs/reference/transforms"
            target="_blank"
          >
            <Button variant="outline" size="sm">
              <BookText class="h-3 w-3 mr-1" />
              Docs
            </Button>
          </a>
        </div>
      </div>
      <div class="p-4">
        <div class="space-y-4">
          <div class="space-y-2">
            <div class="flex items-center gap-2">
              <Label for="name">Function name</Label>
              <Popover>
                <PopoverTrigger>
                  <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
                </PopoverTrigger>
                <PopoverContent
                  class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                >
                  <div class="text-sm space-y-2">
                    <p class="text-slate-500 dark:text-slate-400">
                      Give your function a descriptive name to help identify its
                      purpose.
                    </p>
                  </div>
                </PopoverContent>
              </Popover>
            </div>
            <Input
              id="name"
              bind:value={form.name}
              placeholder="e.g. id-only-transform"
              class="max-w-xl bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
            />
            {#if showErrors && formErrors.name}
              <p class="text-sm text-red-500 dark:text-red-400">
                {formErrors.name[0]}
              </p>
            {/if}
          </div>

          <div class="space-y-2 max-w-xl">
            <div class="flex items-center gap-2">
              <Label for="type">Function type</Label>
              <Popover>
                <PopoverTrigger>
                  <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
                </PopoverTrigger>
                <PopoverContent
                  class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                >
                  <div class="text-sm space-y-2">
                    <p class="text-slate-500 dark:text-slate-400">
                      Select the type of function you want to create.
                    </p>

                    <ul
                      class="list-disc pl-4 space-y-1 text-slate-500 dark:text-slate-400"
                    >
                      <li>
                        <code class="font-mono">path</code> - Extract data from a
                        specific path in the message.
                      </li>
                      <li>
                        <code class="font-mono">function</code> - Transform the message
                        and other properties of the request with Elixir code.
                      </li>
                    </ul>

                    <p class="text-slate-500 dark:text-slate-400">
                      Read more in our <a
                        href="https://sequinstream.com/docs/reference/transforms"
                        class="text-blue-500 dark:text-blue-400"
                        target="_blank">documentation</a
                      >.
                    </p>
                  </div>
                </PopoverContent>
              </Popover>
            </div>

            <Select
              onSelectedChange={handleTypeSelect}
              selected={{
                value: form.function.type,
                label: functionInternalToExternal[form.function.type],
              }}
              disabled={isEditing}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a function type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem
                  value="filter"
                  label={functionInternalToExternal.filter}
                />
                <SelectItem
                  value="transform"
                  label={functionInternalToExternal.transform}
                />
                <SelectItem
                  value="routing"
                  label={functionInternalToExternal.routing}
                />
                <SelectItem
                  value="path"
                  label={functionInternalToExternal.path}
                />
              </SelectContent>
            </Select>
            {#if showErrors && formErrors.function && Array.isArray(formErrors.function)}
              <p class="text-sm text-red-500 dark:text-red-400">
                {formErrors.function[0]}
              </p>
            {/if}

            {#if form.function.type === "routing"}
              <Select
                onSelectedChange={handleRoutingSinkTypeSelect}
                selected={{
                  value: form.function.sink_type,
                  label: sinkTypeInternalToExternal[form.function.sink_type],
                }}
                disabled={isEditing}
              >
                <SelectTrigger>
                  <SelectValue
                    placeholder="Select a sink type for routing..."
                  />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem
                    value="http_push"
                    label={sinkTypeInternalToExternal.http_push}
                  />
                  <SelectItem
                    value="redis_string"
                    label={sinkTypeInternalToExternal.redis_string}
                  />
                </SelectContent>
              </Select>
              {#if showErrors && formErrors.function?.sink_type}
                <p class="text-sm text-red-500 dark:text-red-400">
                  {formErrors.function.sink_type[0]}
                </p>
              {/if}
            {/if}
          </div>

          <div class="space-y-2">
            <div class="flex items-center gap-2">
              <Label for="description">Description</Label>
              <Popover>
                <PopoverTrigger>
                  <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
                </PopoverTrigger>
                <PopoverContent
                  class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                >
                  <div class="text-sm space-y-2">
                    <p class="text-slate-500 dark:text-slate-400">
                      Add a description to explain what this transform does and
                      how it should be used.
                    </p>
                  </div>
                </PopoverContent>
              </Popover>
            </div>
            <textarea
              id="description"
              bind:value={form.description}
              placeholder="e.g. Extracts only the ID field from the record"
              class="w-full min-h-[100px] max-w-xl bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800 rounded-md p-2 resize-y"
            ></textarea>
            {#if showErrors && formErrors.description}
              <p class="text-sm text-red-500 dark:text-red-400">
                {formErrors.description[0]}
              </p>
            {/if}
          </div>

          {#if form.function.type === "path"}
            <div class="space-y-2">
              <div class="flex items-center gap-2">
                <Label for="path">Transform Path</Label>
                <Popover>
                  <PopoverTrigger>
                    <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
                  </PopoverTrigger>
                  <PopoverContent
                    class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                  >
                    <div class="text-sm space-y-2">
                      <p class="text-slate-500 dark:text-slate-400">
                        Enter a path to extract data from the message. Valid
                        paths must start with one of the following:
                      </p>
                      <ul
                        class="list-disc pl-4 space-y-1 text-slate-500 dark:text-slate-400"
                      >
                        <li>record - The main record data</li>
                        <li>changes - The changes made to the record</li>
                        <li>
                          action - The type of change (insert/update/delete)
                        </li>
                        <li>metadata - Additional metadata about the change</li>
                      </ul>
                      <p class="text-slate-500 dark:text-slate-400">
                        For example, <code class="font-mono">record.id</code> to
                        extract the id from the main record.
                      </p>
                    </div>
                  </PopoverContent>
                </Popover>
              </div>
              <Input
                id="path"
                bind:value={form.function.path}
                placeholder="e.g. record.id or changes.name"
                class="font-mono max-w-xl bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
              />
              {#if showErrors && formErrors.function?.path}
                <p class="text-sm text-red-500 dark:text-red-400">
                  {formErrors.function.path[0]}
                </p>
              {/if}
            </div>
          {/if}

          <div
            hidden={!(
              form.function.type === "transform" ||
              form.function.type === "routing" ||
              form.function.type === "filter"
            )}
          >
            <div class="space-y-2">
              {#if !functionTransformsEnabled}
                <div
                  class="flex w-fit items-start gap-2 p-3 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md"
                >
                  <Info
                    class="w-4 h-4 text-blue-500 dark:text-blue-400 mt-0.5"
                  />
                  <div class="flex flex-col gap-1">
                    <p class="text-sm text-blue-700 dark:text-blue-300">
                      Transform functions are not enabled in Cloud.
                    </p>
                    <p class="text-sm text-blue-700 dark:text-blue-300">
                      Talk to the Sequin team if you are interested in trying
                      them out.
                    </p>
                  </div>
                </div>
              {:else}
                <div
                  class="max-w-3xl"
                  hidden={form.function.type === "path" ||
                    form.function.type === undefined}
                >
                  <div class="flex items-center gap-2">
                    <Label for="function">Transform function</Label>
                    <Popover>
                      <PopoverTrigger>
                        <Info
                          class="w-4 h-4 text-slate-500 dark:text-slate-400"
                        />
                      </PopoverTrigger>
                      <PopoverContent
                        class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                      >
                        <div class="text-sm space-y-2">
                          <p class="text-slate-500 dark:text-slate-400">
                            Enter Elixir code that defines a function to
                            transform the message.
                          </p>
                          <p class="text-slate-500 dark:text-slate-400">
                            Read more about the <a
                              href="https://sequinstream.com/docs/reference/transforms#function-transform"
                              class="text-blue-500 dark:text-blue-400"
                              target="_blank">function transform</a
                            > in our documentation.
                          </p>
                        </div>
                      </PopoverContent>
                    </Popover>
                    <a
                      href="https://sequinstream.com/docs/reference/transforms#function-transform"
                      target="_blank"
                      class="ml-auto text-sm text-blue-500 dark:text-blue-400 hover:underline"
                    >
                      Read the docs
                    </a>
                  </div>
                  <div
                    bind:this={functionEditorElement}
                    class="w-full max-w-3xl max-h-full bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-800 rounded-md overflow-hidden relative"
                  >
                    <div
                      class="absolute bottom-2 right-2 flex items-center gap-2 z-10"
                    >
                      {#if isCodeModified}
                        <span
                          class="text-xs bg-yellow-100 dark:bg-yellow-900 text-yellow-700 dark:text-yellow-300 px-2 py-1 rounded-full font-medium select-none"
                          >modified</span
                        >
                      {/if}
                      <Tooltip>
                        <TooltipTrigger>
                          <button
                            type="button"
                            class="text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 disabled:opacity-50 disabled:cursor-not-allowed"
                            on:click={resetOriginalCode}
                            disabled={!isCodeModified}
                          >
                            <RotateCcw class="w-4 h-4" />
                          </button>
                        </TooltipTrigger>
                        <TooltipContent>Reset to original code</TooltipContent>
                      </Tooltip>
                    </div>
                  </div>

                  <p
                    class="-mb-2 min-h-10 text-sm text-red-500 dark:text-red-400"
                  >
                    {#if formErrors.function?.code}
                      {formErrors.function.code[0]}
                    {/if}
                  </p>
                </div>
              {/if}
            </div>
          </div>

          <div class="flex gap-2 pt-2">
            <AlertDialog bind:open={showUpdateDialog}>
              <Button type="submit" loading={saving} disabled={saving}>
                <span slot="loading">
                  {#if isEditing}
                    Updating...
                  {:else}
                    Creating...
                  {/if}
                </span>
                {#if isEditing}
                  Update Function
                {:else}
                  Create Function
                {/if}
              </Button>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Update Function</AlertDialogTitle>
                  <AlertDialogDescription>
                    This function is currently being used by the following
                    consumers:
                    <div
                      class="max-h-[40vh] overflow-y-auto mt-2 border border-slate-200 dark:border-slate-700 rounded-md p-2"
                    >
                      <ul class="list-disc pl-4 space-y-1">
                        {#each usedByConsumers as consumer}
                          <li class="font-mono">{consumer.name}</li>
                        {/each}
                      </ul>
                    </div>
                    <p class="mt-2">
                      Are you sure you want to update this function? This may
                      affect the behavior of these consumers.
                    </p>
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction
                    on:click={() => {
                      saving = true;
                      pushEvent("save", { function: form }, () => {
                        saving = false;
                      });
                    }}
                  >
                    Update Function
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>

            {#if form.function.type === "transform"}
              <CopyToClipboard
                textFn={handleCopyForChatGPT}
                buttonText="Copy for ChatGPT"
                successText="Copied to clipboard"
                buttonVariant="magic"
                buttonSize="default"
                className="w-48"
              />
            {/if}

            {#if isEditing}
              <Button
                type="button"
                variant="destructive"
                on:click={() => (showDeleteDialog = true)}
              >
                Delete Function
              </Button>
              <DeleteFunctionDialog
                bind:open={showDeleteDialog}
                consumers={usedByConsumers}
                onDelete={() => pushEvent("delete")}
              />
            {/if}
          </div>
        </div>
      </div>
    </div>

    <div
      class="w-full border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900 mt-6 max-w-7xl mx-auto px-4 sm:px-6"
    >
      <div class="p-4 border-b border-slate-200 dark:border-slate-800">
        <h2 class="text-lg font-semibold tracking-tight">Test Messages</h2>
        <div class="pt-2 text-sm text-slate-500 dark:text-slate-400">
          To capture test messages that match the <span
            class="font-medium text-slate-700 dark:text-slate-300"
            >real data</span
          >
          in your database:
          <ol class="pl-8 mt-2 space-y-1 list-decimal">
            <li>
              <div class="flex items-center gap-2">
                Select a database and table
                {#if selectedDatabaseId && selectedTableOid}
                  <svg
                    class="w-4 h-4 text-green-500"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                  >
                    <path
                      d="M20 6L9 17l-5-5"
                      stroke-linecap="round"
                      stroke-linejoin="round"
                    />
                  </svg>
                {:else}
                  <svg
                    class="w-4 h-4 text-gray-300 animate-spin"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="currentColor"
                    stroke-width="2"
                  >
                    <path
                      d="M12 2C6.47715 2 2 6.47715 2 12C2 17.5228 6.47715 22 12 22C17.5228 22 22 17.5228 22 12C22 6.47715 17.5228 2 12 2Z"
                      stroke-dasharray="45"
                      stroke-dashoffset="45"
                      stroke-linecap="round"
                    />
                  </svg>
                {/if}
              </div>
            </li>
            <li>
              <div class="flex items-center gap-2">
                <div class="flex items-center gap-2">
                  Make up to 10 inserts, updates, and deletes on the table
                </div>
                {#if selectedDatabaseId && selectedTableOid}
                  <div class="flex items-center gap-3">
                    <div class="flex gap-0.5">
                      {#each Array(10) as _, i}
                        <div
                          class="w-1 h-3 rounded-sm transition-colors duration-200 {i <
                          testMessages.length
                            ? 'bg-green-500'
                            : 'bg-slate-300 animate-pulse'}"
                        ></div>
                      {/each}
                    </div>
                  </div>
                {/if}
              </div>
            </li>
          </ol>
        </div>

        <div class="flex flex-col lg:flex-row gap-4 mt-4">
          <div class="flex-1 space-y-2">
            <Label for="database-select">Database</Label>
            <Select
              onSelectedChange={handleDatabaseSelect}
              selected={{
                value: selectedDatabaseId,
                label: selectedDatabase?.name,
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a database" />
              </SelectTrigger>
              <SelectContent>
                {#each databases as db}
                  <SelectItem value={db.id}>{db.name}</SelectItem>
                {/each}
              </SelectContent>
            </Select>
          </div>

          <div class="flex-1 space-y-2">
            <Label for="table-select">Table</Label>
            <Popover bind:open={tableComboboxOpen} let:ids>
              <PopoverTrigger asChild let:builder>
                <Button
                  builders={[builder]}
                  variant="outline"
                  role="combobox"
                  aria-expanded={tableComboboxOpen}
                  class="w-full justify-between"
                  disabled={!selectedDatabaseId}
                  id="table-combobox-trigger"
                >
                  {selectedTable
                    ? `${selectedTable.schema}.${selectedTable.name}`
                    : "Select a table"}
                  <ChevronsUpDown class="ml-2 h-4 w-4 shrink-0 opacity-50" />
                </Button>
              </PopoverTrigger>
              <PopoverContent class="p-0" align="start">
                <Command.Root>
                  <Command.Input
                    placeholder="Search tables..."
                    class="focus:ring-0 focus:ring-offset-0"
                  />
                  <Command.Empty>No table found.</Command.Empty>
                  <Command.Group>
                    <div class="max-h-[300px] overflow-y-auto">
                      {#each selectedDatabase?.tables || [] as table}
                        <Command.Item
                          value={`${table.schema}.${table.name}`}
                          onSelect={() => {
                            handleTableSelectCombobox(table.oid);
                            closeTableComboboxAndFocusTrigger(ids.trigger);
                          }}
                        >
                          <Check
                            class={cn(
                              "mr-2 h-4 w-4",
                              selectedTableOid !== table.oid &&
                                "text-transparent",
                            )}
                          />
                          {table.schema}.{table.name}
                        </Command.Item>
                      {/each}
                    </div>
                  </Command.Group>
                </Command.Root>
              </PopoverContent>
            </Popover>
          </div>
        </div>
      </div>

      <div
        class="grid grid-cols-1 xl:grid-cols-2 h-full gap-4 bg-white dark:bg-slate-950 text-slate-900 dark:text-slate-50 max-w-6xl mx-auto xl:max-w-none"
      >
        <!-- Left Rail: Test Messages and Original Message -->
        <div>
          <div class="p-4">
            <div class="flex items-center justify-between">
              <h3 class="text-lg font-semibold tracking-tight">Input</h3>
              {#if showSyntheticMessages}
                <span
                  class="text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded-full font-medium"
                  >Synthetic</span
                >
              {/if}
            </div>
          </div>
          <div class="p-4 space-y-2">
            {#each messagesToShow as message, i}
              <div
                class="w-full text-left rounded-lg border border-slate-200 dark:border-slate-800 transition-all duration-200 {selectedMessageIndex ===
                i
                  ? 'bg-blue-50 dark:bg-blue-950/50 border-blue-200 dark:border-blue-800 shadow-sm'
                  : 'hover:bg-slate-50 dark:hover:bg-slate-800/50'}"
              >
                <button
                  class="w-full p-3"
                  on:click={() => selectMessage(i)}
                  type="button"
                >
                  <div class="flex justify-between items-center">
                    <span class="font-medium"
                      >{showSyntheticMessages
                        ? "Example"
                        : `Message ${i + 1}`}</span
                    >
                    {#if !showSyntheticMessages && selectedMessageIndex === i}
                      <button
                        class="text-xs text-slate-500"
                        on:click={(e) => {
                          e.preventDefault();
                          deleteMessage(message, i);
                        }}
                      >
                        <Trash2 class="w-4 h-4" />
                      </button>
                    {/if}
                  </div>
                </button>
                {#if selectedMessageIndex === i}
                  <div
                    class="p-3 border-t border-slate-200 dark:border-slate-800"
                  >
                    <h3
                      class="text-sm font-medium mb-2 text-slate-500 dark:text-slate-400"
                    >
                      Function arguments
                    </h3>
                    <EditableArgument
                      field="record"
                      bind:selectedMessage
                      bind:form
                      bind:formErrors
                    />
                    <EditableArgument
                      field="changes"
                      bind:selectedMessage
                      bind:form
                      bind:formErrors
                    />
                    <EditableArgument
                      field="action"
                      bind:selectedMessage
                      bind:form
                      bind:formErrors
                    />
                    <EditableArgument
                      field="metadata"
                      bind:selectedMessage
                      bind:form
                      bind:formErrors
                    />
                  </div>
                {/if}
              </div>
            {/each}
          </div>
        </div>

        <!-- Right Rail: Output -->
        <div
          class="w-full border-l border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900"
        >
          <div class="p-4 flex items-center justify-between">
            <h3 class="text-lg font-semibold tracking-tight">Output</h3>
            {#if selectedMessage.time}
              <div class="flex items-center gap-1">
                <span
                  class="text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded-full font-medium cursor-help"
                >
                  {#if validating}
                    <div class="flex items-center gap-1">
                      Executing...
                      <Loader2 class="w-4 h-4 animate-spin" />
                    </div>
                  {:else}
                    Executed in
                    {selectedMessage.time >= 1000
                      ? `${(selectedMessage.time / 1000).toFixed(2)}ms`
                      : `${selectedMessage.time}μs`}
                  {/if}
                </span>
                <Popover>
                  <PopoverTrigger>
                    <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
                  </PopoverTrigger>
                  <PopoverContent
                    class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
                  >
                    <div class="text-sm space-y-2">
                      <p class="text-slate-500 dark:text-slate-400">
                        This time includes compilation overhead that only occurs
                        in the playground. Actual execution time during a sink
                        will be significantly faster.
                      </p>
                    </div>
                  </PopoverContent>
                </Popover>
              </div>
            {/if}
          </div>

          <div class="p-4">
            {#if !selectedMessage.error}
              <div
                class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300"
              >
                <div class="flex justify-center items-center p-4">
                  {#if selectedMessage.transformed !== undefined}
                    <div class="w-full overflow-auto">
                      <pre
                        class="text-slate-600 dark:text-slate-400">{JSON.stringify(
                          selectedMessage.transformed,
                          null,
                          2,
                        )}
                    </pre>
                    </div>
                  {:else}
                    <div
                      class="text-center p-6 border border-dashed border-slate-300 dark:border-slate-700 rounded-md bg-slate-50 dark:bg-slate-800/50 max-w-md"
                    >
                      <p class="text-slate-600 dark:text-slate-400">
                        {#if form.function?.type}
                          Output not available - fix your transform!
                        {:else}
                          Select a function type to view output
                        {/if}
                      </p>
                    </div>
                  {/if}
                </div>
              </div>
            {:else}
              <div class="text-red-600 font-bold text-lg">
                {selectedMessage.error.type}
              </div>

              <div
                class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300"
              >
                <div
                  class="grid font-mono"
                  style="grid-template-columns: minmax(8em, min-content) 1fr;"
                >
                  {#each errorKeyOrder as key}
                    {#if selectedMessage.error.info && selectedMessage.error.info[key]}
                      <div class="font-semibold">{key}</div>
                      <div>
                        {selectedMessage.error.info[key]}
                      </div>
                    {/if}
                  {/each}
                  <!-- Extra keys -->
                  {#if selectedMessage.error.info}
                    {#each Object.entries(selectedMessage.error.info) as [key, value]}
                      {#if !errorKeyOrder.includes(key)}
                        <div class="font-semibold">{key}</div>
                        <pre>{typeof value === "object"
                            ? JSON.stringify(value, null, 2)
                            : value}</pre>
                      {/if}
                    {/each}
                  {/if}
                </div>
              </div>
            {/if}
          </div>
        </div>
      </div>
    </div>
  </form>
</FullPageForm>
