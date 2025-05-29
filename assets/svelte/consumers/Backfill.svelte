<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import { Progress } from "$lib/components/ui/progress";
  import { formatNumberWithCommas, formatRelativeTimestamp } from "../utils";
  import { Loader2 } from "lucide-svelte";
  import { CheckCircle2 } from "lucide-svelte";
  import * as Popover from "$lib/components/ui/popover";
  import { Info } from "lucide-svelte";
  import { ArrowDownSquare } from "lucide-svelte";

  // Simplified props interface
  interface BackfillProps {
    cursor_position: {
      is_backfilling: boolean;
      cursor_type: string;
      backfill?: {
        rows_initial_count: number;
        rows_processed_count: number;
        rows_ingested_count: number;
        state: string;
      };
      last_completed_backfill?: {
        rows_processed_count: number;
        rows_ingested_count: number;
        completed_at: string;
        inserted_at: string;
      };
    } | null;
  }

  interface Database {
    id: string;
    name: string;
    pg_major_version: number;
  }

  export let database: Database;
  export let cursor_position: BackfillProps["cursor_position"];
  export let onRun: () => void;
  export let onCancel: (callback: (reply: any) => void) => void;

  let isCancelling = false;

  // Check if backfills are supported based on PostgreSQL version
  $: isBackfillSupported = database.pg_major_version >= 14;

  function handleRun() {
    onRun();
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

  // Computed progress value
  $: progress = cursor_position?.backfill
    ? cursor_position.backfill.rows_initial_count === null
      ? null
      : (cursor_position.backfill.rows_processed_count /
          cursor_position.backfill.rows_initial_count) *
        100
    : 0;

  function getElapsedTime(start: string, end: string): string {
    const startDate = new Date(start);
    const endDate = new Date(end);
    const diffMs = endDate.getTime() - startDate.getTime();

    const days = Math.floor(diffMs / (24 * 60 * 60 * 1000));
    const hours = Math.floor(
      (diffMs % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000),
    );
    const minutes = Math.floor((diffMs % (60 * 60 * 1000)) / 60000);
    const seconds = Math.floor((diffMs % 60000) / 1000);
    const ms = diffMs % 1000;

    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    if (seconds > 0) return `${seconds}s`;
    return `${ms}ms`;
  }
</script>

<Card>
  <CardContent class="p-6">
    <div class="flex items-center justify-between mb-4">
      <h2 class="text-lg font-semibold flex items-center gap-2">
        Backfill
        {#if cursor_position?.last_completed_backfill}
          <span
            class="flex items-center gap-2 text-sm font-normal text-gray-600"
          >
            <CheckCircle2 class="h-4 w-4 text-green-500" />
            last completed {formatRelativeTimestamp(
              cursor_position.last_completed_backfill.completed_at,
            )}
          </span>
        {:else}
          <Popover.Root>
            <Popover.Trigger asChild let:builder>
              <Button
                builders={[builder]}
                variant="link"
                class="text-muted-foreground hover:text-foreground p-0"
              >
                <Info class="h-4 w-4" />
              </Button>
            </Popover.Trigger>
            <Popover.Content class="w-80">
              <div class="grid gap-4">
                <div class="space-y-2">
                  <p class="text-sm text-muted-foreground font-normal">
                    Run a <a
                      href="https://sequinstream.com/docs/reference/backfills"
                      target="_blank"
                      rel="noopener noreferrer"
                      class="text-primary underline"
                    >
                      backfill
                    </a> to ingest records from the table.
                  </p>
                </div>
              </div>
            </Popover.Content>
          </Popover.Root>
        {/if}
      </h2>
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
        <Button
          variant="outline"
          size="sm"
          on:click={handleRun}
          disabled={!isBackfillSupported}
        >
          <ArrowDownSquare class="mr-2 h-4 w-4" />
          Run Backfill
        </Button>
      {/if}
    </div>

    {#if !isBackfillSupported}
      <div>
        <p class="text-sm">
          Backfills are not supported for PostgreSQL 12 and 13.
          <a
            href="https://docs.sequinstream.com/reference/databases#postgresql-12-and-13"
            target="_blank"
            rel="noopener noreferrer"
            class="underline font-medium"
          >
            Learn more
          </a>
        </p>
      </div>
    {/if}

    {#if cursor_position?.is_backfilling && cursor_position.backfill}
      <div class="space-y-4">
        <div class="grid grid-cols-3 gap-4">
          <div class="flex flex-col items-center">
            <span class="text-lg font-medium">
              {cursor_position.backfill.rows_initial_count === null
                ? "Calculating..."
                : formatNumberWithCommas(
                    cursor_position.backfill.rows_initial_count,
                  )}
            </span>
            <span class="text-sm text-gray-500">Total Rows</span>
          </div>
          <div class="flex flex-col items-center">
            <span class="text-lg font-medium">
              {formatNumberWithCommas(
                cursor_position.backfill.rows_processed_count,
              )}
            </span>
            <span class="text-sm text-gray-500">Processed</span>
          </div>
          <div class="flex flex-col items-center">
            <span class="text-lg font-medium">
              {formatNumberWithCommas(
                cursor_position.backfill.rows_ingested_count,
              )}
            </span>
            <span class="text-sm text-gray-500">Ingested</span>
          </div>
        </div>

        <div class="space-y-2 max-w-md mx-auto">
          <Progress
            value={progress}
            class={`h-2 ${progress === null ? "animate-pulse" : ""}`}
          />
          <p class="text-sm text-gray-500 text-center">
            {progress
              ? `${progress.toFixed(1)}% Complete`
              : "Calculating progress..."}
          </p>
        </div>
      </div>
    {:else if cursor_position?.last_completed_backfill}
      <p class="text-sm text-gray-500">
        Processed {formatNumberWithCommas(
          cursor_position.last_completed_backfill.rows_processed_count,
        )} and ingested {formatNumberWithCommas(
          cursor_position.last_completed_backfill.rows_ingested_count,
        )} records in {getElapsedTime(
          cursor_position.last_completed_backfill.inserted_at,
          cursor_position.last_completed_backfill.completed_at,
        )}
      </p>
    {:else if isBackfillSupported}
      <p class="text-sm text-gray-500">
        Run a backfill to process existing records in the table.
      </p>
    {/if}
  </CardContent>
</Card>

<style>
  :global(.overflow-visible) {
    overflow: visible !important;
  }
</style>
