<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Skeleton } from "$lib/components/ui/skeleton";
  import {
    ArrowRightCircle,
    ArrowRight,
    CheckCircle,
    AlertCircle,
  } from "lucide-svelte";
  import * as Alert from "$lib/components/ui/alert";
  import { formatNumberWithCommas } from "../utils";

  export let cursor_position;
  export let messages_processed_count;
</script>

<Card class="mb-8">
  <CardContent class="p-6">
    <h2 class="text-lg font-semibold mb-4">Cursor Position</h2>
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
                cursor_position.processing_count || 0
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
                cursor_position.to_process_count || 0
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
