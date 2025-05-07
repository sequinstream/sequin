<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { createEventDispatcher, onMount, onDestroy } from "svelte";

  export let title: string = "";
  let showConfirmDialog = false;
  export let showConfirmOnExit = true;

  const dispatch = createEventDispatcher();

  let listening = false;

  function handleEscapeKey(event: KeyboardEvent) {
    if (event.key === "Escape" && !showConfirmDialog) {
      event.preventDefault();
      if (showConfirmOnExit) {
        showConfirmDialog = true;
      } else {
        dispatch("close");
      }
    }
  }

  $: {
    if (!listening) {
      window.addEventListener("keydown", handleEscapeKey);
      listening = true;
    } else {
      window.removeEventListener("keydown", handleEscapeKey);
      listening = false;
    }
  }

  onMount(() => {
    window.addEventListener("keydown", handleEscapeKey);
    listening = true;
  });

  onDestroy(() => {
    window.removeEventListener("keydown", handleEscapeKey);
  });

  function confirmClose() {
    showConfirmDialog = false;
    dispatch("close");
  }

  function cancelClose() {
    showConfirmDialog = false;
  }
</script>

<div class="fixed inset-0 bg-background">
  <div class="flex flex-col h-full overflow-y-auto">
    <div class="flex justify-between items-center p-6 border-b">
      {#if $$slots.header}
        <slot name="header" />
      {:else}
        <h2 class="text-2xl font-semibold">{title}</h2>
      {/if}

      <Button
        variant="outline"
        on:click={() => {
          if (showConfirmOnExit) {
            showConfirmDialog = true;
          } else {
            dispatch("close");
          }
        }}
      >
        Exit
      </Button>
    </div>

    <div class="pt-8 pb-12">
      <slot />
    </div>

    {#if $$slots.footer}
      <div class="flex-shrink-0 border-t">
        <slot name="footer" />
      </div>
    {/if}
  </div>
</div>

<AlertDialog.Root bind:open={showConfirmDialog}>
  <AlertDialog.Content>
    <AlertDialog.Header>
      <AlertDialog.Title>Are you sure you want to exit?</AlertDialog.Title>
      <AlertDialog.Description>
        Your changes will be lost if you exit without saving.
      </AlertDialog.Description>
    </AlertDialog.Header>
    <AlertDialog.Footer>
      <AlertDialog.Cancel on:click={cancelClose}>Cancel</AlertDialog.Cancel>
      <AlertDialog.Action on:click={confirmClose}>Exit</AlertDialog.Action>
    </AlertDialog.Footer>
  </AlertDialog.Content>
</AlertDialog.Root>
