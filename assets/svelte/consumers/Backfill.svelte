<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import { Progress } from "$lib/components/ui/progress";
  import { formatNumberWithCommas, formatRelativeTimestamp } from "../utils";
  import { Loader2 } from "lucide-svelte";
  import { CheckCircle2 } from "lucide-svelte";
  import * as Dialog from "$lib/components/ui/dialog";
  import { Input } from "$lib/components/ui/input";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Label } from "$lib/components/ui/label";
  import Datetime from "../components/Datetime.svelte";

  export let cursor_position: {
    is_backfilling: boolean;
    cursor_type: string;
    backfill?: {
      rows_initial_count: number;
      rows_processed_count: number;
      rows_ingested_count: number;
      state: string;
    };
    last_completed_at: number | null;
  } | null;

  export let onRun: (
    newCursorPosition: any,
    callback: (reply: any) => void,
  ) => void;
  export let onCancel: (callback: (reply: any) => void) => void;

  let isRunning = false;
  let isCancelling = false;
  let showDialog = false;
  let newCursorPosition;
  let cursorError: string = "";
  let startPosition = "beginning";
  let cursorDate: Date | null = null;
  let validationError = "";

  $: if (cursor_position?.is_backfilling) {
    isRunning = false;
  } else {
    isCancelling = false;
    isRunning = false;
  }

  function handleRun() {
    showDialog = true;
  }

  function handleCancel() {
    isCancelling = true;
    onCancel((reply) => {
      if (!reply.ok) {
        isCancelling = false;
        // Handle error
      }
    });
  }

  function handleSubmit() {
    validationError = "";

    if (startPosition === "specific") {
      if (cursor_position?.cursor_type?.startsWith("timestamp")) {
        if (!cursorDate) {
          validationError = "Please select a valid date and time";
          return;
        }
      } else if (!newCursorPosition && newCursorPosition !== 0) {
        validationError = "Please enter a valid position value";
        return;
      }
    }

    showDialog = false;
    isRunning = true;
    let position;
    if (startPosition === "beginning") {
      position = null;
    } else {
      if (cursor_position?.cursor_type?.startsWith("timestamp") && cursorDate) {
        position = cursorDate.toISOString();
      } else {
        position = newCursorPosition;
      }
    }
    onRun(position, (reply) => {
      if (!reply.ok) {
        isRunning = false;
      }
    });
  }

  $: startPosition, (validationError = "");

  $: progress = cursor_position?.backfill
    ? cursor_position.backfill.rows_initial_count === null
      ? null
      : (cursor_position.backfill.rows_processed_count /
          cursor_position.backfill.rows_initial_count) *
        100
    : 0;
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex items-center justify-between mb-4">
      <h2 class="text-lg font-semibold">Backfill</h2>
      {#if cursor_position?.is_backfilling}
        <Button
          variant="destructive"
          on:click={handleCancel}
          disabled={isCancelling}
        >
          {#if isCancelling}
            <Loader2 class="mr-2 h-4 w-4 animate-spin" />
          {/if}
          Cancel Backfill
        </Button>
      {:else}
        <Button on:click={handleRun} disabled={isRunning}>
          {#if isRunning}
            <Loader2 class="mr-2 h-4 w-4 animate-spin" />
          {/if}
          Run Backfill
        </Button>
      {/if}
    </div>

    {#if cursor_position?.is_backfilling && cursor_position.backfill}
      <div class="space-y-4">
        <Progress
          value={progress}
          class={progress === null ? "animate-pulse" : ""}
        />

        <div class="grid grid-cols-3 gap-4 text-sm">
          <div>
            <div class="text-gray-500">Total Rows</div>
            <div class="font-medium">
              {cursor_position.backfill.rows_initial_count === null
                ? "Calculating..."
                : formatNumberWithCommas(
                    cursor_position.backfill.rows_initial_count,
                  )}
            </div>
          </div>
          <div>
            <div class="text-gray-500">Processed</div>
            <div class="font-medium">
              {formatNumberWithCommas(
                cursor_position.backfill.rows_processed_count,
              )}
            </div>
          </div>
          <div>
            <div class="text-gray-500">Ingested</div>
            <div class="font-medium">
              {formatNumberWithCommas(
                cursor_position.backfill.rows_ingested_count,
              )}
            </div>
          </div>
        </div>
      </div>
    {:else}
      <div class="space-y-4">
        {#if cursor_position?.last_completed_at}
          <div class="flex items-center space-x-2 text-sm text-gray-600">
            <CheckCircle2 class="h-4 w-4 text-green-500" />
            <span
              >Last completed {formatRelativeTimestamp(
                new Date(cursor_position.last_completed_at).toISOString(),
              )}</span
            >
          </div>
        {:else}
          <p class="text-sm text-gray-500">
            Run a backfill to process all existing records in the table.
          </p>
        {/if}
      </div>
    {/if}
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
            {#if cursor_position?.cursor_type?.startsWith("timestamp")}
              <Datetime
                bind:value={cursorDate}
                class="z-[100]"
                bind:error={cursorError}
              />
              {#if cursorError}
                <p class="text-sm text-red-500 mt-2">{cursorError}</p>
              {/if}
            {:else if ["integer", "bigint", "smallint", "serial"].includes(cursor_position?.cursor_type)}
              <Input type="number" id="cursor" bind:value={newCursorPosition} />
            {:else}
              <Input type="text" id="cursor" bind:value={newCursorPosition} />
            {/if}

            {#if validationError}
              <p class="text-sm text-red-500 mt-2">{validationError}</p>
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
