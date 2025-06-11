<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Progress } from "$lib/components/ui/progress";
  import { Badge } from "$lib/components/ui/badge";
  import { Loader2, ArrowDownSquare, X } from "lucide-svelte";
  import { formatNumberWithCommas, formatRelativeTimestamp } from "../utils";
  import TableWithDrawer from "$lib/components/TableWithDrawer.svelte";
  import * as Dialog from "$lib/components/ui/dialog";
  import BackfillForm from "../components/BackfillForm.svelte";
  import type { Consumer } from "./types";

  // Receive necessary props
  export let backfills: any[];
  export let totalCount: number = 0;
  export let pageSize: number;
  export let page: number;
  export let live;
  export let consumer: Consumer;

  let loading = false;
  let selectedBackfill = null;
  let isDrawerOpen = false;
  let isCancelling = false;

  // Backfill form state
  let showBackfillDialog = false;
  let backfillForm = {
    selectedTableOids: [],
  };
  let backfillFormErrors: Record<string, string> = {};
  let isSubmittingBackfill = false;

  // Check if backfills are supported based on PostgreSQL version
  $: isBackfillSupported = consumer?.database?.pg_major_version >= 14;
  $: pageCount = Math.ceil(totalCount / pageSize);

  $: {
    // if there is a selected backfill, update it when the backfills list changes
    if (selectedBackfill) {
      selectedBackfill = backfills.find(
        (backfill) => backfill.id === selectedBackfill.id,
      );
    }
  }

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < pageCount) {
      page = newPage;
      loading = true;
      live.pushEvent("change_backfills_page", { page: page }, (reply) => {
        loading = false;
      });
    }
  }

  function openDrawer(backfill) {
    selectedBackfill = backfill;
    isDrawerOpen = true;
  }

  function closeDrawer() {
    isDrawerOpen = false;
    selectedBackfill = null;
    isCancelling = false;
  }

  function formatDate(dateString: string) {
    if (!dateString) return "N/A";
    const date = new Date(dateString);
    const now = new Date();
    const yesterday = new Date(now);
    yesterday.setDate(yesterday.getDate() - 1);

    const isToday = date.toDateString() === now.toDateString();
    const isYesterday = date.toDateString() === yesterday.toDateString();

    const timeString = date.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });

    if (isToday) {
      return `Today at ${timeString}`;
    } else if (isYesterday) {
      return `Yesterday at ${timeString}`;
    } else {
      return `${date.toLocaleDateString()} at ${timeString}`;
    }
  }

  function getStateColor(state: string) {
    switch (state) {
      case "active":
        return "bg-blue-200 text-blue-800";
      case "completed":
        return "bg-green-200 text-green-800";
      case "cancelled":
        return "bg-gray-200 text-gray-800";
      default:
        return "bg-gray-200 text-gray-800";
    }
  }

  function getBackfillDescription(backfill: any) {
    if (backfill.state === "active") {
      if (backfill.rows_initial_count === null) {
        return "Calculating...";
      }
      const progress = backfill.progress ? backfill.progress.toFixed(1) : "0";
      return `${progress}% complete (${formatNumberWithCommas(backfill.rows_processed_count)} of ${formatNumberWithCommas(backfill.rows_initial_count)} rows)`;
    } else if (backfill.state === "completed") {
      const completedAt = backfill.completed_at
        ? formatRelativeTimestamp(backfill.completed_at)
        : "unknown time";
      return `Backfilled ${formatNumberWithCommas(backfill.rows_ingested_count)} rows ${completedAt}`;
    } else if (backfill.state === "cancelled") {
      const cancelledAt = backfill.canceled_at
        ? formatRelativeTimestamp(backfill.canceled_at)
        : "unknown time";
      return `Cancelled ${cancelledAt}`;
    }
    return "";
  }

  function cancelBackfill(backfillId: string) {
    isCancelling = true;
    live.pushEvent("cancel_backfill", { backfill_id: backfillId }, (reply) => {
      isCancelling = false;
      if (reply.ok) {
        closeDrawer();
      }
    });
  }

  function handleBackfillSubmit() {
    isSubmittingBackfill = true;
    showBackfillDialog = false;

    live.pushEvent("run-backfill", backfillForm, (reply) => {
      isSubmittingBackfill = false;
      if (!reply.ok) {
        backfillFormErrors = reply.errors || {};
      }
    });
  }

  function openBackfillDialog() {
    backfillForm = {
      selectedTableOids: [],
    };
    backfillFormErrors = {};
    showBackfillDialog = true;
  }

  function getElapsedTime(start: string, end: string): string {
    if (!start || !end) return "N/A";
    const startDate = new Date(start);
    const endDate = new Date(end);
    const diffMs = endDate.getTime() - startDate.getTime();

    const days = Math.floor(diffMs / (24 * 60 * 60 * 1000));
    const hours = Math.floor(
      (diffMs % (24 * 60 * 60 * 1000)) / (60 * 60 * 1000),
    );
    const minutes = Math.floor((diffMs % (60 * 60 * 1000)) / 60000);
    const seconds = Math.floor((diffMs % 60000) / 1000);

    if (days > 0) return `${days}d ${hours}h`;
    if (hours > 0) return `${hours}h ${minutes}m`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
  }
</script>

<TableWithDrawer
  title="Backfills"
  items={backfills}
  {pageSize}
  {loading}
  {page}
  {pageCount}
  onPageChange={changePage}
  onRowClick={openDrawer}
  {isDrawerOpen}
  onCloseDrawer={closeDrawer}
>
  <svelte:fragment slot="titleActions">
    <Button
      variant="outline"
      size="sm"
      on:click={openBackfillDialog}
      disabled={!isBackfillSupported}
    >
      <ArrowDownSquare class="h-4 w-4 mr-1" />
      Run Backfill
    </Button>
  </svelte:fragment>

  <svelte:fragment slot="emptyState">
    <h1 class="text-2xl font-bold">No Backfills</h1>
    <h2 class="text-gray-600">
      {#if !isBackfillSupported}
        Backfills are not supported for PostgreSQL {consumer?.database
          ?.pg_major_version}.
        <a
          href="https://docs.sequinstream.com/reference/databases#postgresql-12-and-13"
          target="_blank"
          rel="noopener noreferrer"
          class="underline font-medium"
        >
          Learn more
        </a>
      {:else}
        Run a backfill to process existing records in the table.
      {/if}
    </h2>
  </svelte:fragment>

  <svelte:fragment slot="header">
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      State
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Table
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Progress
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Started
    </th>
  </svelte:fragment>

  <svelte:fragment slot="sampleRow">
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
  </svelte:fragment>

  <svelte:fragment slot="row" let:item>
    <td class="px-2 py-1 whitespace-nowrap text-2xs">
      <Badge class={getStateColor(item.state)}>
        {item.state}
      </Badge>
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-900">
      {item.table_name}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-600">
      {getBackfillDescription(item)}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
      {formatDate(item.inserted_at)}
    </td>
  </svelte:fragment>

  <svelte:fragment slot="drawerTitle">Backfill Details</svelte:fragment>

  <svelte:fragment slot="drawerContent">
    {#if selectedBackfill}
      <div class="space-y-4">
        <!-- Backfill Details -->
        <div class="bg-gray-50 p-4 rounded-lg space-y-2">
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">State:</span>
            <Badge class={getStateColor(selectedBackfill.state)}>
              {selectedBackfill.state}
            </Badge>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Table:</span>
            <span class="text-sm text-gray-900"
              >{selectedBackfill.table_name}</span
            >
          </div>
          {#if selectedBackfill.rows_initial_count !== null}
            <div class="flex justify-between items-center">
              <span class="text-sm font-medium text-gray-500">Total Rows:</span>
              <span class="text-sm text-gray-900">
                {formatNumberWithCommas(selectedBackfill.rows_initial_count)}
              </span>
            </div>
          {/if}
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500"
              >Rows Processed:</span
            >
            <span class="text-sm text-gray-900">
              {formatNumberWithCommas(selectedBackfill.rows_processed_count)}
            </span>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Rows Ingested:</span
            >
            <span class="text-sm text-gray-900">
              {formatNumberWithCommas(selectedBackfill.rows_ingested_count)}
            </span>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Started:</span>
            <span class="text-sm text-gray-900">
              {formatDate(selectedBackfill.inserted_at)}
            </span>
          </div>
          {#if selectedBackfill.completed_at}
            <div class="flex justify-between items-center">
              <span class="text-sm font-medium text-gray-500">Completed:</span>
              <span class="text-sm text-gray-900">
                {formatDate(selectedBackfill.completed_at)}
              </span>
            </div>
            <div class="flex justify-between items-center">
              <span class="text-sm font-medium text-gray-500">Duration:</span>
              <span class="text-sm text-gray-900">
                {getElapsedTime(
                  selectedBackfill.inserted_at,
                  selectedBackfill.completed_at,
                )}
              </span>
            </div>
          {/if}
          {#if selectedBackfill.canceled_at}
            <div class="flex justify-between items-center">
              <span class="text-sm font-medium text-gray-500">Cancelled:</span>
              <span class="text-sm text-gray-900">
                {formatDate(selectedBackfill.canceled_at)}
              </span>
            </div>
          {/if}
        </div>

        <!-- Progress Section (for active backfills) -->
        {#if selectedBackfill.state === "active"}
          <div class="bg-gray-50 p-4 rounded-lg space-y-4">
            <h3 class="text-sm font-semibold">Progress</h3>
            {#if selectedBackfill.rows_initial_count === null}
              <div class="space-y-2">
                <Progress value={null} class="h-2 animate-pulse" />
                <p class="text-sm text-gray-500 text-center">
                  Calculating total rows...
                </p>
              </div>
            {:else if selectedBackfill.progress !== null}
              <div class="space-y-2">
                <Progress value={selectedBackfill.progress} class="h-2" />
                <p class="text-sm text-gray-500 text-center">
                  {selectedBackfill.progress.toFixed(1)}% Complete
                </p>
              </div>
            {/if}
          </div>
        {/if}

        <!-- Actions Section -->
        {#if selectedBackfill.state === "active"}
          <div class="bg-gray-50 p-4 rounded-lg space-y-4">
            <h3 class="text-sm font-semibold">Actions</h3>
            <Button
              variant="destructive"
              size="sm"
              on:click={() => cancelBackfill(selectedBackfill.id)}
              disabled={isCancelling}
              class="flex items-center space-x-2"
            >
              {#if isCancelling}
                <Loader2 class="h-4 w-4 animate-spin" />
              {:else}
                <X class="h-4 w-4" />
              {/if}
              <span>Cancel Backfill</span>
            </Button>
          </div>
        {/if}
      </div>
    {/if}
  </svelte:fragment>
</TableWithDrawer>

<!-- Backfill Dialog -->
<Dialog.Root bind:open={showBackfillDialog}>
  <Dialog.Content class="md:max-w-4xl overflow-visible">
    <Dialog.Header>
      <Dialog.Title>
        Start backfill for {consumer.database.name} database
      </Dialog.Title>
    </Dialog.Header>
    <div class="grid gap-4 py-4">
      <BackfillForm
        tables_included_in_source={consumer.tables_included_in_source}
        form={backfillForm}
      />
    </div>
    <Dialog.Footer>
      <Button
        type="submit"
        on:click={handleBackfillSubmit}
        disabled={isSubmittingBackfill}
      >
        {#if isSubmittingBackfill}
          <Loader2 class="mr-2 h-4 w-4 animate-spin" />
        {/if}
        <ArrowDownSquare class="mr-2 h-4 w-4" />
        Start Backfill
      </Button>
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>

<style>
  .text-2xs {
    font-size: 0.65rem;
    line-height: 1rem;
  }
</style>
