<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Info } from "lucide-svelte";
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

  interface FormData {
    id: string;
    name: string;
    transform: {
      path: string;
    };
  }

  interface FormErrors {
    name?: string[];
    transform?: {
      path?: string[];
    };
  }

  interface TestMessage {
    original: any;
    transformed: any;
  }

  interface Consumer {
    name: string;
  }

  export let formData: FormData;
  export let formErrors: FormErrors = {};
  export let testMessages: TestMessage[] = [];
  export let validating: boolean = false;
  export let parent: string;
  export let live;
  export let usedByConsumers: Consumer[] = [];
  export let saving: boolean = false;

  let form = {
    ...formData,
    transform: { ...formData.transform, type: "path" },
  };
  let selectedMessageIndex = 0;

  let isEditing = form.id !== null;

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

  function handleDelete() {
    if (usedByConsumers.length > 0) {
      showDeleteDialog = true;
    } else {
      pushEvent("delete");
    }
  }

  let showUpdateDialog = false;
  let showDeleteDialog = false;

  $: pushEvent("validate", { transform: form });
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
      <h2 class="text-lg font-semibold tracking-tight">
        Transform Configuration
      </h2>
    </div>
    <div class="p-4">
      <form on:submit={handleSubmit} class="space-y-4">
        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <Label for="name" class="font-mono">Transform Name</Label>
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
            class="font-mono max-w-lg bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
          />
          {#if formErrors.name}
            <p class="text-sm text-red-500 dark:text-red-400 font-mono">
              {formErrors.name[0]}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <div class="flex items-center gap-2">
            <Label for="path" class="font-mono">Transform Path</Label>
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
                    <li>action - The type of change (insert/update/delete)</li>
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
            class="font-mono max-w-lg bg-slate-50 dark:bg-slate-800/50 border-slate-200 dark:border-slate-800"
          />
          {#if formErrors.transform?.path}
            <p class="text-sm text-red-500 dark:text-red-400 font-mono">
              {formErrors.transform.path[0]}
            </p>
          {/if}
        </div>

        <div class="flex gap-2">
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
    class="grid grid-cols-1 xl:grid-cols-2 h-full gap-4 bg-white dark:bg-slate-950 text-slate-900 dark:text-slate-50"
  >
    <!-- Left Rail: Test Messages and Original Message -->
    <div
      class="w-full border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
    >
      <div class="p-4 border-b border-slate-200 dark:border-slate-800">
        <div class="flex items-center justify-between">
          <h2 class="text-lg font-semibold tracking-tight">Test Messages</h2>
        </div>
      </div>
      <div class="p-4 space-y-2">
        {#if testMessages.length === 0}
          <div class="text-center py-8">
            <div class="animate-pulse space-y-4">
              <div
                class="w-12 h-12 mx-auto rounded-full bg-slate-100 dark:bg-slate-800"
              ></div>
              <div class="space-y-2">
                <div
                  class="h-4 w-48 mx-auto bg-slate-100 dark:bg-slate-800 rounded"
                ></div>
                <div
                  class="h-3 w-64 mx-auto bg-slate-100 dark:bg-slate-800 rounded"
                ></div>
              </div>
            </div>
            <p class="text-slate-500 dark:text-slate-400 mt-4">
              Waiting for database events...
            </p>
            <p
              class="text-sm text-slate-400 dark:text-slate-500 mt-2 font-mono"
            >
              Make changes to your database to generate test messages
            </p>
          </div>
        {:else}
          {#each testMessages as message, i}
            <button
              class="w-full text-left p-3 rounded-lg border border-slate-200 dark:border-slate-800 transition-all duration-200 {selectedMessageIndex ===
              i
                ? 'bg-blue-50 dark:bg-blue-950/50 border-blue-200 dark:border-blue-800 shadow-sm'
                : 'hover:bg-slate-50 dark:hover:bg-slate-800/50'}"
              on:click={() => (selectedMessageIndex = i)}
            >
              <span class="font-medium font-mono">Message {i + 1}</span>
              {#if selectedMessageIndex === i}
                <div
                  class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-800"
                >
                  <h3
                    class="text-sm font-medium mb-2 text-slate-500 dark:text-slate-400"
                  >
                    Original Message
                  </h3>
                  <pre
                    class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300">{JSON.stringify(
                      message.original,
                      null,
                      2,
                    )}</pre>
                </div>
              {/if}
            </button>
          {/each}
        {/if}
      </div>
    </div>

    <!-- Right Rail: Transformed Output -->
    <div
      class="w-full border border-slate-200 dark:border-slate-800 rounded-lg bg-white dark:bg-slate-900"
    >
      <div class="p-4 border-b border-slate-200 dark:border-slate-800">
        <h2 class="text-lg font-semibold tracking-tight">Transformed Output</h2>
      </div>
      <div class="p-4">
        {#if testMessages.length === 0}
          <div class="text-center py-8">
            <p class="text-slate-500 dark:text-slate-400 font-mono">
              Select a test message to view output
            </p>
          </div>
        {:else}
          <pre
            class="text-sm bg-slate-50 dark:bg-slate-800/50 p-3 rounded-md overflow-auto font-mono text-slate-700 dark:text-slate-300">{JSON.stringify(
              testMessages[selectedMessageIndex].transformed,
              null,
              2,
            )}</pre>
        {/if}
      </div>
    </div>
  </div>
</div>
