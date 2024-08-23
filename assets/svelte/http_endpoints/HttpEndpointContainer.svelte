<script lang="ts">
  import HttpEndpointForm from "./HttpEndpointForm.svelte";
  import { Button } from "$lib/components/ui/button";
  import * as Dialog from "$lib/components/ui/dialog";
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { onMount } from "svelte";

  export let formData: {
    name: string;
    base_url: string;
    headers: Record<string, string>;
  };
  export let formErrors: Record<string, string | string[]>;
  export let parent: string;
  export let live;

  let userInput = false;
  let isSubmitted = false;
  let dialogOpen = true;
  let showConfirmDialog = false;

  function pushEvent(event: string, payload = {}) {
    live.pushEventTo(`#${parent}`, event, payload);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    isSubmitted = true;
    pushEvent("save", { http_endpoint: formData });
  }

  function handleFormDataChange() {
    userInput = true;
    pushEvent("validate", { http_endpoint: formData });
  }

  function confirmClose() {
    showConfirmDialog = false;
    dialogOpen = false;
    pushEvent("form_closed");
  }

  function cancelClose() {
    showConfirmDialog = false;
  }

  function handleEscapeKey(event: KeyboardEvent) {
    if (event.key === "Escape") {
      event.preventDefault();
      if (showConfirmDialog) {
        showConfirmDialog = false;
      } else {
        showConfirmDialog = true;
      }
    }
  }

  onMount(() => {
    window.addEventListener("keydown", handleEscapeKey);
    return () => {
      window.removeEventListener("keydown", handleEscapeKey);
    };
  });

  $: {
    if (userInput) {
      pushEvent("validate", { http_endpoint: formData });
    }
  }
</script>

<Dialog.Root bind:open={dialogOpen} preventScroll={false} closeOnEscape={false}>
  <Dialog.Portal>
    <Dialog.Content
      closeButton={false}
      class="w-full h-full max-w-full max-h-full"
    >
      <div class="flex flex-col h-full">
        <div class="flex justify-between items-center p-6 border-b">
          <Dialog.Title class="text-2xl font-bold"
            >Create HTTP Endpoint</Dialog.Title
          >
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
          <form on:submit={handleSubmit} class="space-y-4">
            <HttpEndpointForm
              {formData}
              {formErrors}
              {isSubmitted}
              on:formDataChange={handleFormDataChange}
            />

            <div>
              <Button type="submit">Create HTTP Endpoint</Button>
            </div>
          </form>
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
