<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import { Progress } from "$lib/components/ui/progress";
  import { formatNumberWithCommas } from "../utils";
  import { Loader2 } from "lucide-svelte";

  export let cursor_position: {
    is_backfilling: boolean;
    cursor_type: string;
    backfill?: {
      rows_initial_count: number;
      rows_processed_count: number;
      rows_ingested_count: number;
      state: string;
    };
  } | null;

  export let onRun: (
    newCursorPosition: any,
    callback: (reply: any) => void,
  ) => void;
  export let onCancel: (callback: (reply: any) => void) => void;

  let isRunning = false;
  let isCancelling = false;

  $: if (cursor_position?.is_backfilling) {
    isRunning = false;
  } else {
    isCancelling = false;
  }

  function handleRun() {
    isRunning = true;
    onRun(null, (reply) => {
      if (!reply.ok) {
        isRunning = false;
        // Handle error
      }
    });
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
      <p class="text-sm text-gray-500">
        Run a backfill to process all existing records in the table.
      </p>
    {/if}
  </CardContent>
</Card>
