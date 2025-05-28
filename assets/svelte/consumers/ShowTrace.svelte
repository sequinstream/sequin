<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Switch } from "$lib/components/ui/switch";
  import { Play, Pause, Loader2 } from "lucide-svelte";
  import TableWithDrawer from "$lib/components/TableWithDrawer.svelte";

  // Props
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

  export let live;
  export let parent;

  let page = trace.page;
  let loading = trace.loading;
  let paused = trace.paused;
  let selectedEvent = null;
  let isDrawerOpen = false;

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < trace.page_count) {
      page = newPage;
      loading = true;
      live.pushEventTo(`#${parent}`, "trace_change_page", { page: newPage });
    }
  }

  function togglePause() {
    paused = !paused;
    if (paused) {
      live.pushEventTo(`#${parent}`, "trace_stop");
    } else {
      live.pushEventTo(`#${parent}`, "trace_start");
    }
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

  function formatContent(content: any): string {
    try {
      return JSON.stringify(content, null, 2);
    } catch (e) {
      return String(content);
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
      variant={paused ? "default" : "outline"}
      size="sm"
      on:click={togglePause}
    >
      {#if paused}
        <Play class="h-4 w-4 mr-1" /> Start Trace
      {:else}
        <Pause class="h-4 w-4 mr-1" /> Stop Trace
      {/if}
    </Button>
  </svelte:fragment>

  <svelte:fragment slot="emptyState">
    No trace events yet. Hit "Start trace" then wait for messages to flow
    through this sink.
  </svelte:fragment>

  <svelte:fragment slot="header">
    <th class="px-2 py-1"></th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Timestamp
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Message
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Content
    </th>
  </svelte:fragment>

  <svelte:fragment slot="sampleRow">
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
  </svelte:fragment>

  <svelte:fragment slot="row" let:item>
    <td class="px-2 w-4">
      {#if item.status === "info"}
        <div class="px-[2px] py-[10px] bg-blue-500 text-xs rounded-sm"></div>
      {:else if item.status === "error"}
        <div class="px-[2px] py-[10px] bg-red-500 text-xs rounded-sm"></div>
      {:else if item.status === "warning"}
        <div class="px-[2px] py-[10px] bg-orange-500 text-xs rounded-sm"></div>
      {/if}
    </td>
    <td class="px-2 py-1 w-fit whitespace-nowrap text-2xs text-gray-500">
      {formatDate(item.timestamp)}
    </td>
    <td class="px-2 py-1 w-fit whitespace-nowrap text-2xs">
      {item.message}
    </td>
    <td
      class="px-2 py-1 mr-auto w-full whitespace-nowrap text-2xs text-gray-500 font-mono overflow-hidden max-w-lg"
    >
      {formatContent(item.content)}
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

        <!-- Content -->
        {#if selectedEvent.content}
          <div class="bg-gray-50 p-4 rounded-lg">
            <h3 class="text-sm font-medium text-gray-500 mb-2">Content:</h3>
            <pre
              class="text-sm text-gray-900 whitespace-pre-wrap overflow-x-auto font-mono">{formatContent(
                selectedEvent.content,
              )}</pre>
          </div>
        {/if}
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
