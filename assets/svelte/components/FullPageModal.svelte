<script lang="ts">
  import * as Dialog from "$lib/components/ui/dialog";
  import { Button } from "$lib/components/ui/button";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { createEventDispatcher, onMount, onDestroy } from "svelte";

  export let title: string;
  export let open = true;
  export let showConfirmDialog = false;

  const dispatch = createEventDispatcher();

  function handleEscapeKey(event: KeyboardEvent) {
    if (event.key === "Escape" && !showConfirmDialog) {
      event.preventDefault();
      showConfirmDialog = true;
    }
  }

  onMount(() => {
    window.addEventListener("keydown", handleEscapeKey);
  });

  onDestroy(() => {
    window.removeEventListener("keydown", handleEscapeKey);
  });

  function confirmClose() {
    showConfirmDialog = false;
    open = false;
    dispatch("close");
  }

  function cancelClose() {
    showConfirmDialog = false;
  }
</script>

<Dialog.Root bind:open preventScroll={false} closeOnEscape={false}>
  <Dialog.Portal>
    <Dialog.Content
      closeButton={false}
      class="w-full h-full max-w-full max-h-full"
    >
      <div id="full-page-modal"></div>
      <div class="flex flex-col h-full bg-background">
        <div class="flex justify-between items-center p-6 border-b">
          <Dialog.Title class="text-2xl font-semibold">{title}</Dialog.Title>
          <Dialog.Close asChild>
            <Button
              variant="outline"
              on:click={() => (showConfirmDialog = true)}
            >
              Exit
            </Button>
          </Dialog.Close>
        </div>

        <div class="flex-grow p-6 overflow-y-auto">
          <slot />
        </div>
      </div>
    </Dialog.Content>
  </Dialog.Portal>
</Dialog.Root>

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
