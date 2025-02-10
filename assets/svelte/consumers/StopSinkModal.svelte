<script lang="ts">
  import * as Dialog from "$lib/components/ui/dialog";
  import { Button } from "$lib/components/ui/button";
  import { AlertTriangle } from "lucide-svelte";

  export let open = false;
  export let consumerName: string;
  export let onClose: () => void;
  export let onConfirm: (action: "pause" | "disable") => void;
  export let loading = false;

  let selectedAction: "pause" | "disable" = "disable";

  function handleClose() {
    selectedAction = "disable";
    onClose();
  }

  function handleConfirm() {
    onConfirm(selectedAction);
  }
</script>

<Dialog.Root bind:open>
  <Dialog.Content class="sm:max-w-[500px]">
    <Dialog.Header>
      <Dialog.Title class="leading-6">
        Stop <code class="inline bg-gray-100 p-1 rounded-md text-sm"
          >{consumerName}</code
        >?
      </Dialog.Title>
      <Dialog.Description class="mb-6">
        Choose how you want to stop this sink:
      </Dialog.Description>
    </Dialog.Header>

    <div class="space-y-4 mt-4">
      <div
        role="button"
        tabindex="0"
        class="p-4 border rounded-lg cursor-pointer hover:border-primary transition-colors"
        class:border-primary={selectedAction === "disable"}
        on:click={() => (selectedAction = "disable")}
        on:keydown={(e) => e.key === "Enter" && (selectedAction = "disable")}
      >
        <div class="font-semibold mb-1">Disable</div>
        <p class="text-sm text-gray-600">
          Sink will be completely stopped and new messages won't be buffered.
          When re-enabled, processing will start from that point forward.
        </p>
      </div>

      <div
        role="button"
        tabindex="0"
        class="p-4 border rounded-lg cursor-pointer hover:border-primary transition-colors"
        class:border-primary={selectedAction === "pause"}
        on:click={() => (selectedAction = "pause")}
        on:keydown={(e) => e.key === "Enter" && (selectedAction = "pause")}
      >
        <div class="font-semibold mb-1">Pause (Temporary)</div>
        <p class="text-sm text-gray-600">
          Messages will be buffered while paused, allowing you to resume
          processing from where you left off. Best for short-term maintenance or
          temporary interruptions.
        </p>
      </div>

      {#if selectedAction === "pause"}
        <div class="bg-amber-50 border-amber-200 border rounded-lg p-4 mt-4">
          <div class="flex gap-2">
            <AlertTriangle class="h-5 w-5 text-amber-500 flex-shrink-0" />
            <p class="text-sm text-amber-800">
              Paused sinks continue to consume resources and may impact other
              sinks using the same replication slot. Consider disabling instead
              if you need to stop a sink for more than a few hours.
            </p>
          </div>
        </div>
      {/if}
    </div>

    <Dialog.Footer class="mt-6">
      <Button variant="outline" on:click={handleClose}>Cancel</Button>
      <Button variant="destructive" on:click={handleConfirm} disabled={loading}>
        {#if loading}
          {selectedAction === "pause" ? "Pausing..." : "Disabling..."}
        {:else}
          {selectedAction === "pause" ? "Pause" : "Disable"}
        {/if}
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
