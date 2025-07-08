<script lang="ts">
  import {
    ArrowLeft,
    Clock,
    RotateCw,
    Database,
    Loader2,
    CirclePlay,
    Pause,
    RefreshCw,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { formatRelativeTimestamp } from "$lib/utils";
  import * as Dialog from "$lib/components/ui/dialog";
  import LinkPushNavigate from "$lib/components/LinkPushNavigate.svelte";
  export let database;
  export let activeTab;
  export let live;
  export let parent;

  function pushEvent(event: string, params = {}, callback: any = () => {}) {
    live.pushEventTo("#" + parent, event, params, callback);
  }

  function handleEdit() {
    pushEvent("edit", {});
  }

  let showDeleteConfirmDialog = false;
  let deleteConfirmDialogLoading = false;
  let deleteErrorMessage: string | null = null;

  function confirmDelete() {
    deleteConfirmDialogLoading = true;
    deleteErrorMessage = null;
    pushEvent("delete_database", {}, (res: any) => {
      deleteConfirmDialogLoading = false;
      if (res.error) {
        deleteErrorMessage = res.error;
      } else {
        showDeleteConfirmDialog = false;
      }
    });
  }

  function cancelDelete() {
    showDeleteConfirmDialog = false;
    deleteErrorMessage = null;
  }

  function handleDelete() {
    showDeleteConfirmDialog = true;
  }

  let statusTransitioning = false;
  let statusTransitionTimeout: NodeJS.Timeout | null = null;
  let displayPaused = database.paused;
  let isRestarting = false;

  $: {
    if (!statusTransitioning) {
      displayPaused = database.paused;
    }
  }

  function handleStatusTransition() {
    if (statusTransitionTimeout) {
      clearTimeout(statusTransitionTimeout);
    }

    statusTransitionTimeout = setTimeout(() => {
      statusTransitioning = false;
      statusTransitionTimeout = null;
    }, 2000);
  }

  function restartDatabase() {
    isRestarting = true;
    live.pushEventTo("#" + parent, "restart", {}, (res: any) => {
      setTimeout(() => {
        isRestarting = false;
      }, 2000);
    });
  }

  function enableDatabase() {
    displayPaused = false;
    statusTransitioning = true;
    live.pushEventTo("#" + parent, "enable", {}, () => {
      handleStatusTransition();
    });
  }

  let showPauseConfirmDialog = false;

  function confirmPause() {
    displayPaused = true;
    statusTransitioning = true;
    live.pushEventTo("#" + parent, "disable", {}, () => {
      showPauseConfirmDialog = false;
      handleStatusTransition();
    });
  }
</script>

<div class="bg-white border-b sticky top-0 z-10">
  <div class="container mx-auto px-4 py-4">
    <div class="flex items-center justify-between">
      <div class="flex items-center space-x-4">
        <LinkPushNavigate href="/databases">
          <Button variant="ghost" size="sm">
            <ArrowLeft class="h-4 w-4" />
          </Button>
        </LinkPushNavigate>
        <div class="flex items-center">
          <Database class="h-6 w-6 mr-2" />
          <h1 class="text-xl font-semibold">{database.name}</h1>
        </div>
      </div>
      <div class="flex items-center space-x-4">
        <div
          class="hidden lg:flex flex-col items-left gap-1 text-xs text-gray-500"
        >
          <div class="flex items-center gap-2">
            <Clock class="h-4 w-4" />
            <span>Created {formatRelativeTimestamp(database.inserted_at)}</span>
          </div>
          <div class="flex items-center gap-2">
            <RotateCw class="h-4 w-4" />
            <span>Updated {formatRelativeTimestamp(database.updated_at)}</span>
          </div>
        </div>
        {#if isRestarting}
          <Button variant="outline" size="sm" disabled>
            <RefreshCw class="h-4 w-4 mr-1 animate-spin" />
            Restarting...
          </Button>
        {:else}
          <Button
            variant="outline"
            size="sm"
            on:click={restartDatabase}
            disabled={statusTransitioning || displayPaused}
          >
            <RefreshCw class="h-4 w-4 mr-1" />
            Restart
          </Button>
        {/if}
        {#if statusTransitioning}
          {#if !displayPaused}
            <Button variant="outline" size="sm" disabled>
              <CirclePlay class="h-4 w-4 mr-1" />
              Resuming...
            </Button>
          {:else}
            <Button variant="outline" size="sm" disabled>
              <Pause class="h-4 w-4 mr-1" />
              Pausing...
            </Button>
          {/if}
        {:else if !displayPaused}
          <Button
            variant="outline"
            size="sm"
            on:click={() => (showPauseConfirmDialog = true)}
          >
            <Pause class="h-4 w-4 mr-1" />
            Pause
          </Button>
        {:else}
          <Button variant="outline" size="sm" on:click={enableDatabase}>
            <CirclePlay class="h-4 w-4 mr-1" />
            Resume
          </Button>
        {/if}
        <Button variant="outline" size="sm" on:click={handleEdit}>Edit</Button>
        <Button
          variant="outline"
          size="sm"
          class="text-red-600 hover:text-red-700"
          on:click={handleDelete}
        >
          Delete
        </Button>
      </div>
    </div>
  </div>
  <div class="container mx-auto px-4">
    <div class="flex space-x-4">
      <a
        href="/databases/{database.id}"
        class={`py-2 px-4 font-medium border-b-2 ${
          activeTab === "overview"
            ? "text-black border-black"
            : "text-gray-500 hover:text-gray-700 border-transparent"
        }`}
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        Overview
      </a>
      <!-- <a
        href="/databases/{database.id}/messages"
        class={`py-2 px-4 font-medium border-b-2 ${
          activeTab === "messages"
            ? "text-black border-black"
            : "text-gray-500 hover:text-gray-700 border-transparent"
        }`}
        data-phx-link="redirect"
        data-phx-link-state="push"
      >
        Messages
      </a> -->
    </div>
  </div>
</div>

<Dialog.Root bind:open={showDeleteConfirmDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title>Are you sure you want to delete this database?</Dialog.Title
      >
      <Dialog.Description>This action cannot be undone.</Dialog.Description>
    </Dialog.Header>
    {#if deleteErrorMessage}
      <p class="text-destructive text-sm mt-2 mb-4">{deleteErrorMessage}</p>
    {/if}
    <Dialog.Footer>
      <Button variant="outline" on:click={cancelDelete}>Cancel</Button>
      <Button
        variant="destructive"
        on:click={confirmDelete}
        disabled={deleteConfirmDialogLoading}
      >
        {#if deleteConfirmDialogLoading}
          <Loader2 class="mr-2 h-4 w-4 animate-spin" />
          Deleting...
        {:else}
          Delete
        {/if}
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>

<Dialog.Root bind:open={showPauseConfirmDialog}>
  <Dialog.Content>
    <Dialog.Header>
      <Dialog.Title class="leading-6">Pause database?</Dialog.Title>
      <Dialog.Description class="mb-6">
        Sequin will stop receiving changes from the database until resumed.
      </Dialog.Description>
    </Dialog.Header>
    <Dialog.Footer class="mt-4">
      <Button
        variant="outline"
        on:click={() => (showPauseConfirmDialog = false)}>Cancel</Button
      >
      <Button
        variant="secondary"
        on:click={confirmPause}
        disabled={statusTransitioning}
      >
        {#if statusTransitioning}
          Pausing...
        {:else}
          Pause
        {/if}
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
