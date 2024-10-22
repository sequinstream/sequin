<script lang="ts">
  import {
    ArrowLeft,
    Clock,
    RefreshCw,
    Database,
    Loader2,
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
            <RefreshCw class="h-4 w-4" />
            <span>Updated {formatRelativeTimestamp(database.updated_at)}</span>
          </div>
        </div>
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
      <a
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
      </a>
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
