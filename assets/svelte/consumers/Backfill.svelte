<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Label } from "$lib/components/ui/label";
  import Datetime from "../components/Datetime.svelte";

  export let cursor_position: {
    is_backfilling: boolean;
    cursor_type: string;
  } | null;
  export let onRun: (
    newCursorPosition: string | null,
    callback: (result: { ok: boolean }) => void,
  ) => void;
  export let onCancel: (callback: (result: { ok: boolean }) => void) => void;

  let showDialog = false;
  let newCursorPosition;
  let cursorError: string = "";
  let startPosition = "beginning";
  let cursorDate: Date | null = null;
  let isProcessing = false;

  $: isBackfilling = cursor_position?.is_backfilling;
  $: cursor_type = cursor_position?.cursor_type;

  function openDialog() {
    newCursorPosition = null;
    showDialog = true;
  }

  $: {
    if (startPosition === "beginning") {
      newCursorPosition = null;
      cursorDate = null;
    }
  }

  async function handleSubmit() {
    showDialog = false;
    isProcessing = true;
    let position;
    if (startPosition === "beginning") {
      position = null;
    } else {
      if (cursor_type?.startsWith("timestamp") && cursorDate) {
        position = cursorDate.toISOString();
      } else {
        position = newCursorPosition;
      }
    }
    onRun(position, () => {
      isProcessing = false;
    });
  }

  async function handleCancel() {
    isProcessing = true;
    onCancel(() => {
      isProcessing = false;
    });
  }
</script>

<Card class="mb-8">
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Backfills</h2>
      <Button
        variant="outline"
        on:click={isBackfilling ? handleCancel : openDialog}
        disabled={isProcessing}
      >
        {#if isProcessing}
          <span class="animate-spin mr-2">⟳</span>
          Processing...
        {:else if isBackfilling}
          Cancel backfill
        {:else}
          Run backfill
        {/if}
      </Button>
    </div>
    <p class="text-sm text-gray-500">
      {isBackfilling ? "Backfill in progress" : "No backfill in progress"}
    </p>
  </CardContent>
</Card>

<Dialog.Root bind:open={showDialog}>
  <Dialog.Content class="md:max-w-4xl overflow-visible">
    <Dialog.Header>
      <Dialog.Title>Start backfill</Dialog.Title>
      <Dialog.Description>
        Choose where you want to start the backfill from.
      </Dialog.Description>
    </Dialog.Header>
    <div class="grid gap-4 py-4">
      <RadioGroup bind:value={startPosition}>
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="beginning" id="beginning" />
          <Label for="beginning">From the beginning</Label>
        </div>
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="specific" id="specific" />
          <Label for="specific">From a specific position</Label>
        </div>
      </RadioGroup>

      {#if startPosition === "specific"}
        <div class="grid grid-cols-4 items-center gap-4 mt-4">
          <Label for="cursor" class="text-right">Position</Label>
          <div class="col-span-3">
            {#if cursor_type?.startsWith("timestamp")}
              <Datetime
                bind:value={cursorDate}
                class="z-[100]"
                bind:error={cursorError}
              />
              {#if cursorError}
                <p class="text-sm text-red-500 mt-2">{cursorError}</p>
              {/if}
            {:else if ["integer", "bigint", "smallint", "serial"].includes(cursor_type)}
              <Input type="number" id="cursor" bind:value={newCursorPosition} />
            {:else}
              <Input type="text" id="cursor" bind:value={newCursorPosition} />
            {/if}
          </div>
        </div>
      {/if}
    </div>
    <Dialog.Footer>
      <Button type="submit" on:click={handleSubmit} disabled={!!cursorError}>
        Start Backfill
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>

<style>
  :global(.overflow-visible) {
    overflow: visible !important;
  }
</style>
