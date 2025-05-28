<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import { Switch } from "$lib/components/ui/switch";
  import { Play, Pause, Loader2 } from "lucide-svelte";
  import TableWithDrawer from "$lib/components/TableWithDrawer.svelte";

  // Props
  export let consumer: { type: string };
  export let trace: {
    events: any[];
    total_count: number;
    page_size: number;
    page: number;
    page_count: number;
    loading: boolean;
    paused: boolean;
    show_acked: boolean;
  };

  let page = trace.page;
  let loading = trace.loading;
  let paused = trace.paused;
  let showAcked = trace.show_acked;
  let selectedEvent = null;
  let isDrawerOpen = false;

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < trace.page_count) {
      page = newPage;
      loading = true;
      // TODO: Implement page change event
    }
  }

  function togglePause() {
    paused = !paused;
    // TODO: Implement pause/resume event
  }

  function toggleShowAcked() {
    showAcked = !showAcked;
    // TODO: Implement show acked toggle event
  }

  function openDrawer(event) {
    selectedEvent = event;
    isDrawerOpen = true;
  }

  function closeDrawer() {
    isDrawerOpen = false;
    selectedEvent = null;
  }

  function formatDate(dateString: string) {
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
</script>

<TableWithDrawer
  title="Trace Events"
  items={trace.events}
  pageSize={trace.page_size}
  {loading}
  {page}
  pageCount={trace.page_count}
  onPageChange={changePage}
  onRowClick={openDrawer}
  {isDrawerOpen}
  onCloseDrawer={closeDrawer}
>
  <svelte:fragment slot="titleActions">
    <Button
      variant="outline"
      size="sm"
      class="flex items-center space-x-2"
      on:click={toggleShowAcked}
    >
      <Switch checked={showAcked} />
      <span>Show Acked</span>
    </Button>

    <Button
      variant={paused ? "default" : "outline"}
      size="sm"
      on:click={togglePause}
    >
      {#if paused}
        <Play class="h-4 w-4 mr-1" /> Resume Updates
      {:else}
        <Pause class="h-4 w-4 mr-1" /> Pause Updates
      {/if}
    </Button>
  </svelte:fragment>

  <svelte:fragment slot="emptyState">
    {#if showAcked}
      Trace events will appear here when they are pending, delivered, or
      acknowledged.
    {:else}
      You have no pending trace events. Try toggling "Show Acked" to see
      acknowledged events.
    {/if}
  </svelte:fragment>

  <svelte:fragment slot="header">
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Timestamp
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Type
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Status
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Message
    </th>
  </svelte:fragment>

  <svelte:fragment slot="sampleRow">
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
  </svelte:fragment>

  <svelte:fragment slot="row" let:item>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
      {formatDate(item.timestamp)}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs">
      {item.type}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs">
      <span
        class="inline-flex items-center px-2 py-0.5 rounded text-xs font-medium bg-gray-200 text-gray-800"
      >
        {item.status}
      </span>
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs">
      {item.message}
    </td>
  </svelte:fragment>

  <svelte:fragment slot="drawerTitle">Trace Event Details</svelte:fragment>

  <svelte:fragment slot="drawerContent">
    {#if selectedEvent}
      <div class="space-y-4">
        <!-- Event Details -->
        <div class="bg-gray-50 p-4 rounded-lg space-y-2">
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Timestamp:</span>
            <span class="text-sm text-gray-900"
              >{formatDate(selectedEvent.timestamp)}</span
            >
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Type:</span>
            <span class="text-sm text-gray-900">{selectedEvent.type}</span>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Status:</span>
            <span class="text-sm text-gray-900">{selectedEvent.status}</span>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Message:</span>
            <span class="text-sm text-gray-900">{selectedEvent.message}</span>
          </div>
        </div>

        <!-- Additional event data can be added here -->
      </div>
    {/if}
  </svelte:fragment>
</TableWithDrawer>

<style>
  .text-2xs {
    font-size: 0.65rem;
    line-height: 1rem;
  }
</style>
