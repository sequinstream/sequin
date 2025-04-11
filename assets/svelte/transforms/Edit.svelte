<script lang="ts">
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { onMount, onDestroy } from "svelte";
  import { Info, BookText } from "lucide-svelte";
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

  // CodeMirror imports
  import { EditorView, basicSetup } from "codemirror";
  import { EditorState } from "@codemirror/state";
  import { elixir } from "codemirror-lang-elixir";
  import { keymap } from "@codemirror/view";
  import { indentWithTab } from "@codemirror/commands";
  import { autocompletion } from "@codemirror/autocomplete";

  interface FormData {
    id: string;
    name: string;
    description: string;
    transform: {
      type: string;
      path?: string;
      code?: string;
    };
  }

  interface FormErrors {
    name?: string[];
    description?: string[];
    transform?: {
      type?: string[];
      path?: string[];
      code?: string[];
    };
  }

  interface TestMessage {
    record: string;
    changes: string;
    action: string;
    metadata: string;
    transformed: string;
    time: number;
    error: {
      type: string;
      info: any;
    };
  }

  interface Consumer {
    name: string;
  }

  export let formData: FormData;
  export let formErrors: FormErrors = {};
  export let showErrors: boolean = false;
  export let testMessages: TestMessage[] = [];
  export let syntheticTestMessages: TestMessage[] = [];
  export let validating: boolean = false;
  export let parent: string;
  export let live;
  export let usedByConsumers: Consumer[] = [];
  export let saving: boolean = false;
  export let initialCode: string;
  export let functionTransformsEnabled: boolean;
  export let functionCompletions: Array<{
    label: string;
    type: string;
    info: string;
  }>;

  let transformInternalToExternal = {
    path: "Path transform",
    function: "Function transform",
  };

  let sinkTypeToExternal = {
    http_push: "HTTP Push",
  };

  let errorKeyOrder = ["description", "snippet", "line", "column"];

  let form = {
    ...formData,
    transform: { ...formData.transform },
  };
  let selectedMessageIndex = 0;

  let isEditing = form.id !== null;
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
      saving = true;
      pushEvent("save", { transform: form }, () => {
        saving = false;
      });
    }
  }

  function handleTypeSelect(event: any) {
    form.transform.type = event.value;
    form.transform.code ||= initialCode;
  }

  function handleDelete() {
    if (usedByConsumers.length > 0) {
      showDeleteDialog = true;
    } else {
      pushEvent("delete");
    }
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

  let messagesToShow: TestMessage[] = [];
  let showSyntheticMessages = false;

  $: {
    if (testMessages.length > 0) {
      messagesToShow = testMessages;
      showSyntheticMessages = false;
    } else {
      messagesToShow = syntheticTestMessages;
      showSyntheticMessages = true;
    }
  }

  // let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";
  // let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  function handleDatabaseSelect(event: any) {
    selectedDatabaseId = event.value;
    selectedTableOid = null; // Reset table selection when database changes
    testMessages = [];
  }

  function handleTableSelect(event: any) {
    selectedTableOid = event.value;
    pushEvent("table_selected", {
      database_id: selectedDatabaseId,
      table_oid: selectedTableOid,
    });
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

  onMount(() => {
    if (databases.length === 1 && !selectedDatabaseId) {
      handleDatabaseSelect({ value: databases[0].id });
    }

    if (selectedDatabase && selectedDatabase.tables.length === 1) {
      handleTableSelect(selectedDatabase.tables[0]);
    }

    // Initialize CodeMirror for function transform if it exists
    const startState = EditorState.create({
      doc: form.transform.code || initialCode || "",
      extensions: [
        basicSetup,
        elixir(),
        autocompletion({ override: [elixirCompletions] }),
        keymap.of([indentWithTab]),
        EditorView.updateListener.of((update) => {
          if (update.docChanged) {
            form.transform.code = update.state.doc.toString();
          }
        }),
      ],
    });

    functionEditorView = new EditorView({
      state: startState,
      parent: functionEditorElement,
    });

    return () => {
      functionEditorView.destroy();
    };
  });

  $: pushEvent("validate", { transform: form });

  function handleCopyForChatGPT() {
    const currentMessage = messagesToShow[selectedMessageIndex];
    const codeErrors = formErrors.transform?.code || [];

    const prompt = `I need help creating or modifying an Elixir function transform for Sequin. Here are the details:

Transform Details:
- Name: ${form.name}
- Description: ${form.description}
- Current Function Code:
\`\`\`elixir
${form.transform.code || initialCode}
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
  "record": ${currentMessage.record},
  "changes": ${currentMessage.changes},
  "action": "${currentMessage.action}",
  "metadata": ${currentMessage.metadata}
}
\`\`\`

${showSyntheticMessages ? "Warning ⚠️: This is a synthetic test message. The actual data in your database may differ. If the specific data in the test message is important for the behavior of the transform, please capture test messages from your database." : ""}

Documentation:
${FunctionTransformSnippet}

Please help me create or modify the Elixir function transform to achieve the desired transformation. The function should take the record, changes, action, and metadata as arguments and return the transformed data.`;

    return prompt;
  }

  // Clean up timeout on component destroy
  onDestroy(() => {
    if (copyTimeout) {
      clearTimeout(copyTimeout);
    }
  });
</script>

<div class="flex flex-col h-full gap-4 max-w-screen-2xl mx-auto">
  <h1 class="text-2xl font-semibold tracking-tight">
    {#if isEditing}
      Edit Transform
    {:else}
      New Transform
    {/if}
  </h1>
  <div
    class="w-full border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
  >
    <div class="p-4 border-b border-slate-200 dark:border-slate-800">
      <div class="flex justify-between items-center">
        <h2 class="text-lg font-semibold tracking-tight">
          Transform Configuration
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
      <form on:submit={handleSubmit} class="space-y-4">
        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <Label for="name">Transform Name</Label>
            <Popover>
              <PopoverTrigger>
                <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
              </PopoverTrigger>
              <PopoverContent
                class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
              >
                <div class="text-sm space-y-2">
                  <p class="text-slate-500 dark:text-slate-400">
                    Give your transform a descriptive name to help identify its
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
            <Label for="type">Transform Type</Label>
            <Popover>
              <PopoverTrigger>
                <Info class="w-4 h-4 text-slate-500 dark:text-slate-400" />
              </PopoverTrigger>
              <PopoverContent
                class="bg-white dark:bg-slate-900 border-slate-200 dark:border-slate-800"
              >
                <div class="text-sm space-y-2">
                  <p class="text-slate-500 dark:text-slate-400">
                    Select the type of transform you want to create.
                  </p>

                  <ul
                    class="list-disc pl-4 space-y-1 text-slate-500 dark:text-slate-400"
                  >
                    <li>
                      <code class="font-mono">path</code> - Extract data from a specific
                      path in the message.
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
              value: form.transform.type,
              label: transformInternalToExternal[form.transform.type],
            }}
            disabled={isEditing}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select a transform type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem
                value="path"
                label={transformInternalToExternal.path}
              />
              <SelectItem
                value="function"
                label={transformInternalToExternal.function}
              />
            </SelectContent>
          </Select>
          {#if showErrors && formErrors.transform?.type}
            <p class="text-sm text-red-500 dark:text-red-400">
              {formErrors.transform.type[0]}
            </p>
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

        {#if form.transform.type === "path"}
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
                      Enter a path to extract data from the message. Valid paths
                      must start with one of the following:
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
                      For example, <code class="font-mono">record.id</code> to extract
                      the id from the main record.
                    </p>
                  </div>
                </PopoverContent>
              </Popover>
            </div>
            <Input
              id="path"
              bind:value={form.transform.path}
              placeholder="e.g. record.id or changes.name"
              class="font-mono max-w-xl bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
            />
            {#if showErrors && formErrors.transform?.path}
              <p class="text-sm text-red-500 dark:text-red-400">
                {formErrors.transform.path[0]}
              </p>
            {/if}
          </div>
        {/if}

        <div hidden={form.transform.type !== "function"}>
          <div class="space-y-2">
            {#if !functionTransformsEnabled}
              <div
                class="flex w-fit items-start gap-2 p-3 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md"
              >
                <Info class="w-4 h-4 text-blue-500 dark:text-blue-400 mt-0.5" />
                <div class="flex flex-col gap-1">
                  <p class="text-sm text-blue-700 dark:text-blue-300">
                    Function transforms are not enabled in Cloud.
                  </p>
                  <p class="text-sm text-blue-700 dark:text-blue-300">
                    Talk to the Sequin team if you are interested in trying them
                    out.
                  </p>
                </div>
              </div>
            {:else}
              <div class="max-w-3xl">
                <div class="flex items-center gap-2">
                  <Label for="function">Function transform</Label>
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
                          Enter Elixir code that defines a function to transform
                          the message.
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
                  class="w-full max-w-3xl max-h-full bg-slate-50 dark:bg-slate-800/50 border border-slate-200 dark:border-slate-800 rounded-md overflow-hidden"
                ></div>

                <p
                  class="-mb-2 min-h-10 text-sm text-red-500 dark:text-red-400"
                >
                  {#if formErrors.transform?.code}
                    {formErrors.transform.code[0]}
                  {/if}
                </p>
              </div>
            {/if}
          </div>
        </div>

        <div class="flex gap-2 pt-2">
          {#if form.transform.type === "function"}
            <CopyToClipboard
              textFn={handleCopyForChatGPT}
              buttonText="Copy for ChatGPT"
              successText="Copied to clipboard"
              buttonVariant="magic"
              buttonSize="default"
              className="w-48"
            />
          {/if}

          <AlertDialog bind:open={showUpdateDialog}>
            <Button
              type="submit"
              loading={validating || saving}
              disabled={validating || saving}
            >
              <span slot="loading">
                {#if saving}
                  {isEditing ? "Updating..." : "Creating..."}
                {:else}
                  Validating...
                {/if}
              </span>
              {#if isEditing}
                Update Transform
              {:else}
                Create Transform
              {/if}
            </Button>
            <AlertDialogContent>
              <AlertDialogHeader>
                <AlertDialogTitle>Update Transform</AlertDialogTitle>
                <AlertDialogDescription>
                  This transform is currently being used by the following
                  consumers:
                  <ul class="list-disc pl-4 mt-2 space-y-1">
                    {#each usedByConsumers as consumer}
                      <li class="font-mono">{consumer.name}</li>
                    {/each}
                  </ul>
                  <p class="mt-2">
                    Are you sure you want to update this transform? This may
                    affect the behavior of these consumers.
                  </p>
                </AlertDialogDescription>
              </AlertDialogHeader>
              <AlertDialogFooter>
                <AlertDialogCancel>Cancel</AlertDialogCancel>
                <AlertDialogAction
                  on:click={() => {
                    saving = true;
                    pushEvent("save", { transform: form }, () => {
                      saving = false;
                    });
                  }}
                >
                  Update Transform
                </AlertDialogAction>
              </AlertDialogFooter>
            </AlertDialogContent>
          </AlertDialog>

          {#if isEditing}
            <AlertDialog bind:open={showDeleteDialog}>
              <Button
                type="button"
                variant="destructive"
                on:click={handleDelete}
              >
                Delete Transform
              </Button>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Cannot Delete Transform</AlertDialogTitle>
                  <AlertDialogDescription>
                    This transform cannot be deleted because it is currently
                    being used by the following consumers:
                    <ul class="list-disc pl-4 mt-2 space-y-1">
                      {#each usedByConsumers as consumer}
                        <li class="font-mono">{consumer.name}</li>
                      {/each}
                    </ul>
                    <p class="mt-2">
                      Please remove this transform from all consumers before
                      deleting it.
                    </p>
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogAction>OK</AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          {/if}
        </div>
      </form>
    </div>
  </div>

  <div
    class="w-full border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
  >
    <div class="p-4 border-b border-slate-200 dark:border-slate-800">
      <h2 class="text-lg font-semibold tracking-tight">Test Messages</h2>
      <div class="pt-2 text-sm text-slate-500 dark:text-slate-400">
        To capture test messages that match the <span
          class="font-medium text-slate-700 dark:text-slate-300">real data</span
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
              Make an insert, update, or delete on the table
              {#if testMessages.length > 0}
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
              {:else if selectedDatabaseId && selectedTableOid}
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
          <Select
            onSelectedChange={handleTableSelect}
            selected={{
              value: selectedTableOid,
              label: selectedTable
                ? `${selectedTable.schema}.${selectedTable.name}`
                : undefined,
            }}
            disabled={!selectedDatabaseId}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select a table" />
            </SelectTrigger>
            <SelectContent>
              {#each selectedDatabase.tables as table}
                <SelectItem value={table.oid}
                  >{table.schema}.{table.name}</SelectItem
                >
              {/each}
            </SelectContent>
          </Select>
        </div>
      </div>
    </div>

    <div
      class="grid grid-cols-1 xl:grid-cols-2 h-full gap-4 bg-white dark:bg-slate-950 text-slate-900 dark:text-slate-50"
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
            <button
              class="w-full text-left p-3 rounded-lg border border-slate-200 dark:border-slate-800 transition-all duration-200 {selectedMessageIndex ===
              i
                ? 'bg-blue-50 dark:bg-blue-950/50 border-blue-200 dark:border-blue-800 shadow-sm'
                : 'hover:bg-slate-50 dark:hover:bg-slate-800/50'}"
              on:click={() => (selectedMessageIndex = i)}
            >
              <div class="flex justify-between items-center">
                <span class="font-medium"
                  >{showSyntheticMessages
                    ? "Example"
                    : `Message ${i + 1}`}</span
                >
              </div>
              {#if selectedMessageIndex === i}
                <div
                  class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-800"
                >
                  <h3
                    class="text-sm font-medium mb-2 text-slate-500 dark:text-slate-400"
                  >
                    Function transform arguments
                  </h3>
                  <div
                    class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300 select-text space-y-4"
                  >
                    <div>
                      <div class="font-semibold mb-1">record</div>
                      <pre>{message.record}</pre>
                    </div>
                    <div>
                      <div class="font-semibold mb-1">changes</div>
                      <pre>{message.changes}</pre>
                    </div>
                    <div>
                      <div class="font-semibold mb-1">action</div>
                      <pre>{message.action}</pre>
                    </div>
                    <div>
                      <div class="font-semibold mb-1">metadata</div>
                      <pre>{message.metadata}</pre>
                    </div>
                  </div>
                </div>
              {/if}
            </button>
          {/each}
        </div>
      </div>

      <!-- Right Rail: Transformed Output -->
      <div
        class="w-full border-l border-slate-200 dark:border-slate-800 bg-white dark:bg-slate-900"
        hidden={form.transform.type === "function" &&
          !functionTransformsEnabled}
      >
        <div class="p-4 flex items-center justify-between">
          <h3 class="text-lg font-semibold tracking-tight">
            Transformed output
          </h3>
          {#if messagesToShow[selectedMessageIndex].time}
            <div class="flex items-center gap-1">
              <span
                class="text-xs bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 px-2 py-0.5 rounded-full font-medium cursor-help"
              >
                Transformed in
                {messagesToShow[selectedMessageIndex].time >= 1000
                  ? `${(messagesToShow[selectedMessageIndex].time / 1000).toFixed(2)}ms`
                  : `${messagesToShow[selectedMessageIndex].time}μs`}
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
          {#if !messagesToShow[selectedMessageIndex].error}
            <div
              class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300"
            >
              <div class="flex justify-center items-center p-4">
                {#if messagesToShow[selectedMessageIndex].transformed !== undefined}
                  <div class="w-full overflow-auto">
                    <pre
                      class="text-slate-600 dark:text-slate-400">{JSON.stringify(
                        messagesToShow[selectedMessageIndex].transformed,
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
                      Output not available - fix your transform!
                    </p>
                  </div>
                {/if}
              </div>
            </div>
          {:else}
            <div class="text-red-600 font-bold text-lg">
              {messagesToShow[selectedMessageIndex].error.type}
            </div>

            <div
              class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300"
            >
              <div
                class="grid font-mono"
                style="grid-template-columns: minmax(8em, min-content) 1fr;"
              >
                {#each errorKeyOrder as key}
                  {#if messagesToShow[selectedMessageIndex].error.info && messagesToShow[selectedMessageIndex].error.info[key]}
                    <div class="font-semibold">{key}</div>
                    <div>
                      {messagesToShow[selectedMessageIndex].error.info[key]}
                    </div>
                  {/if}
                {/each}
                <!-- Extra keys -->
                {#if messagesToShow[selectedMessageIndex].error.info}
                  {#each Object.entries(messagesToShow[selectedMessageIndex].error.info) as [key, value]}
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
</div>
