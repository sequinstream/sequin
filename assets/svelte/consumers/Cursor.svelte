<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Skeleton } from "$lib/components/ui/skeleton";
  import {
    ArrowRightCircle,
    ArrowRight,
    CheckCircle,
    AlertCircle,
    RefreshCcw,
    Database,
  } from "lucide-svelte";
  import * as Alert from "$lib/components/ui/alert";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { formatNumberWithCommas } from "../utils";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Label } from "$lib/components/ui/label";
  import Datetime from "../components/Datetime.svelte";

  export let cursor_position;
  export let messages_processed_count;
  export let onRewind: (
    newCursorPosition: string | null,
    callback: (result: { ok: boolean }) => void,
  ) => void;

  let showDialog = false;
  let newCursorPosition;
  let cursorError: string = "";
  let startPosition = "beginning";
  let cursorDate: Date | null = null;
  let isRewinding = false;

  $: initial_min_cursor = cursor_position?.initial_min_cursor?.value;
  $: initial_min_cursor_type = cursor_position?.initial_min_cursor?.type;

  $: isBackfilling = cursor_position?.producer === "table_and_wal";

  $: backfillCondition = cursor_position
    ? `${cursor_position.sort_column_name} >= ${cursor_position.initial_min_cursor.value}`
    : null;

  function openDialog() {
    newCursorPosition = initial_min_cursor;
    showDialog = true;
  }

  $: {
    if (startPosition === "beginning") {
      newCursorPosition = null;
      cursorDate = null;
    } else if (initial_min_cursor_type?.startsWith("timestamp")) {
      // Parse ISO string manually to ignore timezone
      const [datePart, timePart] = initial_min_cursor.split("T");
      const [year, month, day] = datePart.split("-");
      const [hours, minutes, seconds] = timePart.split(":");
      cursorDate = new Date(
        year,
        month - 1,
        day,
        hours,
        minutes,
        parseInt(seconds),
      );
    }
  }

  async function handleSubmit() {
    showDialog = false;
    isRewinding = true;
    let position;
    if (startPosition === "beginning") {
      position = null;
    } else {
      if (initial_min_cursor_type?.startsWith("timestamp") && cursorDate) {
        position = cursorDate.toISOString();
      } else {
        position = newCursorPosition;
      }
    }
    onRewind(position, () => {
      isRewinding = false;
    });
  }
</script>

<Card class="mb-8">
  <CardContent class="p-6">
    <div class="flex justify-between items-center mb-4">
      <h2 class="text-lg font-semibold">Cursor Position</h2>
      <div class="flex items-center">
        {#if isBackfilling}
          <div class="mr-2 text-blue-500 flex items-center" title="Backfilling">
            <Database class="h-4 w-4 mr-1" />
            <span class="text-xs">Backfilling</span>
            {#if backfillCondition}
              <span class="text-xs ml-1">({backfillCondition})</span>
            {/if}
          </div>
        {/if}
        <Button
          variant="outline"
          size="sm"
          on:click={openDialog}
          disabled={isRewinding}
        >
          {#if isRewinding}
            <RefreshCcw class="h-4 w-4 mr-2 animate-spin" />
            Rewinding...
          {:else}
            <RefreshCcw class="h-4 w-4 mr-2" />
            Rewind
          {/if}
        </Button>
      </div>
    </div>
    {#if cursor_position === null}
      <div class="grid gap-4 md:grid-cols-3">
        {#each Array(3) as _}
          <div class="flex flex-col space-y-2">
            <Skeleton class="h-4 w-24" />
            <Skeleton class="h-8 w-32" />
            <Skeleton class="h-3 w-40" />
          </div>
        {/each}
      </div>
    {:else if cursor_position === "error"}
      <Alert.Root class="mt-4">
        <AlertCircle class="h-4 w-4" />
        <Alert.Title>Error</Alert.Title>
        <Alert.Description>Failed to get cursor position.</Alert.Description>
      </Alert.Root>
    {:else}
      <div class="grid gap-4 md:grid-cols-3">
        <!-- Processed Section -->
        <div class="flex flex-col">
          <span class="text-sm font-medium text-gray-500 mb-2">Processed</span>
          <div class="flex items-center">
            <CheckCircle class="h-5 w-5 text-green-500 mr-2" />
            <span class="text-xl font-bold"
              >{formatNumberWithCommas(messages_processed_count || 0)}</span
            >
          </div>
        </div>

        <!-- Processing Section -->
        <div class="flex flex-col">
          <span class="text-sm font-medium text-gray-500 mb-2">Processing</span>
          <div class="flex items-center">
            <ArrowRightCircle class="h-5 w-5 text-blue-500 mr-2" />
            <span class="text-xl font-bold"
              >{formatNumberWithCommas(
                cursor_position.processing_count || 0,
              )}</span
            >
          </div>
          {#if cursor_position.min_active_cursor && cursor_position.max_active_cursor}
            <div class="text-xs text-gray-500 mt-1 break-all">
              <span class="font-medium">Range:</span>
              <br />
              <span class="inline-block"
                >{cursor_position.min_active_cursor}</span
              >
              <span class="inline-block"> - </span>
              <span class="inline-block"
                >{cursor_position.max_active_cursor}</span
              >
            </div>
          {:else}
            <span class="text-xs text-gray-500 mt-1">No active range</span>
          {/if}
        </div>

        <!-- To Process Section -->
        <div class="flex flex-col">
          <span class="text-sm font-medium text-gray-500 mb-2">To Process</span>
          <div class="flex items-center">
            <ArrowRight class="h-5 w-5 text-yellow-500 mr-2" />
            <span class="text-xl font-bold"
              >{formatNumberWithCommas(
                cursor_position.to_process_count || 0,
              )}</span
            >
          </div>
          {#if (cursor_position.max_active_cursor || cursor_position.next_active_cursor) && cursor_position.max_possible_cursor}
            <div class="text-xs text-gray-500 mt-1 break-all">
              <span class="font-medium">Range:</span>
              <br />
              <span class="inline-block"
                >{cursor_position.max_active_cursor ||
                  cursor_position.next_active_cursor}</span
              >
              <span class="inline-block"> - </span>
              <span class="inline-block"
                >{cursor_position.max_possible_cursor}</span
              >
            </div>
          {:else}
            <span class="text-xs text-gray-500 mt-1">No range available</span>
          {/if}
        </div>
      </div>

      <!-- Additional Information -->
      {#if cursor_position.min_active_cursor === null && cursor_position.max_active_cursor === null && cursor_position.to_process_count > 0}
        <Alert.Root class="mt-4">
          <AlertCircle class="h-4 w-4" />
          <Alert.Title>No Active Cursors</Alert.Title>
          <Alert.Description>
            The consumer is not currently processing any records.
          </Alert.Description>
        </Alert.Root>
      {:else if cursor_position.to_process_count === 0}
        <Alert.Root class="mt-4" variant="default">
          <CheckCircle class="h-4 w-4" />
          <Alert.Title>All Records Processed</Alert.Title>
          <Alert.Description>
            The consumer has processed all available records.
          </Alert.Description>
        </Alert.Root>
      {/if}
    {/if}
  </CardContent>
</Card>

<Dialog.Root bind:open={showDialog}>
  <Dialog.Content class="md:max-w-4xl overflow-visible">
    <Dialog.Header>
      <Dialog.Title>Update Cursor Position</Dialog.Title>
      <Dialog.Description>
        Choose where you want the consumer to start processing.
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
          <Label for="specific">At a specific position</Label>
        </div>
      </RadioGroup>

      {#if startPosition === "specific"}
        <div class="grid grid-cols-4 items-center gap-4 mt-4">
          <Label for="cursor" class="text-right">Cursor</Label>
          <div class="col-span-3">
            {#if initial_min_cursor_type?.startsWith("timestamp")}
              <Datetime
                bind:value={cursorDate}
                class="z-[100]"
                bind:error={cursorError}
              />
              {#if cursorError}
                <p class="text-sm text-red-500 mt-2">{cursorError}</p>
              {/if}
            {:else if ["integer", "bigint", "smallint", "serial"].includes(initial_min_cursor_type)}
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
        Update
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>

<style>
  :global(.overflow-visible) {
    overflow: visible !important;
  }
</style>
