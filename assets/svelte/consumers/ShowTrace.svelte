<script lang="ts">
  import { Button } from "$lib/components/ui/button";
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
  };

  export let live;
  export let parent;

  let page = trace.page;
  let paused = trace.paused;
  let loading = trace.loading;
  let selectedEvent = null;
  let isDrawerOpen = false;

  $: paused = trace.paused;

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

  function formatEventAsRow(event: any): string {
    try {
      if (event === null || event === undefined) return "";

      // For the table row view, we want a compact representation
      const parts = [];

      // Show error first as it is likely to be important
      if (event.error) {
        parts.push(`Error: ${String(event.error)}`);
      }

      if (event.req_request) {
        parts.push(
          `Request: ${event.req_request.method} ${event.req_request.url}`,
        );
      }

      if (event.req_response) {
        parts.push(`Response: ${event.req_response.status}`);
      }

      return parts.join(" | ");
    } catch (e) {
      return String(event);
    }
  }

  function formatRequest(req: any): string {
    if (!req) return "";
    return `${req.method} ${req.url}\n\nHeaders: ${JSON.stringify(req.headers, null, 2)}\n\nBody: ${JSON.stringify(req.body, null, 2)}`;
  }

  function formatResponse(resp: any): string {
    if (!resp) return "";
    return `Status: ${resp.status}\n\nHeaders: ${JSON.stringify(resp.headers, null, 2)}\n\nBody: ${JSON.stringify(resp.body, null, 2)}`;
  }

  function formatError(error: any): string {
    if (!error) return "";
    return JSON.stringify(error, null, 2);
  }

  function formatExtra(extra: any): string {
    if (!extra) return "";
    return JSON.stringify(extra, null, 2);
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
    {#if paused}
      <h1 class="text-2xl font-bold">No Trace Events</h1>
      <h2 class="text-gray-600">
        No trace events yet. Hit "Start Trace" then wait for messages to flow
        through this sink.
      </h2>
    {:else}
      <h1 class="text-2xl font-bold">No Trace Events</h1>
      <div class="flex items-center gap-2 text-gray-600">
        <Loader2 class="h-5 w-5 animate-spin" />
        <h2>Waiting for trace events...</h2>
      </div>
    {/if}
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
      Event
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
    <td class="px-2 py-1 w-fit whitespace-nowrap text-xs text-gray-500">
      {formatDate(item.published_at)}
    </td>
    <td
      class="px-2 py-1 w-fit whitespace-nowrap text-xs overflow-hidden max-w-xs"
    >
      {item.message}
    </td>
    <td
      class="px-2 py-1 mr-auto w-full whitespace-nowrap text-xs text-gray-500 font-mono overflow-hidden max-w-xs"
    >
      {formatEventAsRow(item)}
    </td>
  </svelte:fragment>

  <svelte:fragment slot="drawerTitle">Trace Event Details</svelte:fragment>

  <svelte:fragment slot="drawerContent">
    {#if selectedEvent}
      <div class="space-y-4">
        <!-- Basic Event Details -->
        <div class="bg-gray-50 p-4 rounded-lg space-y-2">
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Timestamp:</span>
            <span class="text-sm text-gray-900"
              >{formatDate(selectedEvent.published_at)}</span
            >
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

        <!-- Request -->
        {#if selectedEvent.req_request}
          <div class="bg-gray-50 p-4 rounded-lg">
            <h3 class="text-sm font-medium text-gray-500 mb-2">Request:</h3>
            <pre
              class="text-sm text-gray-900 whitespace-pre-wrap overflow-x-auto font-mono">{formatRequest(
                selectedEvent.req_request,
              )}</pre>
          </div>
        {/if}

        <!-- Response -->
        {#if selectedEvent.req_response}
          <div class="bg-gray-50 p-4 rounded-lg">
            <h3 class="text-sm font-medium text-gray-500 mb-2">Response:</h3>
            <pre
              class="text-sm text-gray-900 whitespace-pre-wrap overflow-x-auto font-mono">{formatResponse(
                selectedEvent.req_response,
              )}</pre>
          </div>
        {/if}

        <!-- Error -->
        {#if selectedEvent.error}
          <div class="bg-gray-50 p-4 rounded-lg">
            <h3 class="text-sm font-medium text-gray-500 mb-2">Error:</h3>
            <pre
              class="text-sm text-gray-900 whitespace-pre-wrap overflow-x-auto font-mono">{formatError(
                selectedEvent.error,
              )}</pre>
          </div>
        {/if}

        <!-- Extra -->
        {#if selectedEvent.extra && Object.keys(selectedEvent.extra).length > 0}
          <div class="bg-gray-50 p-4 rounded-lg">
            <h3 class="text-sm font-medium text-gray-500 mb-2">Extra:</h3>
            <pre
              class="text-sm text-gray-900 whitespace-pre-wrap overflow-x-auto font-mono">{formatExtra(
                selectedEvent.extra,
              )}</pre>
          </div>
        {/if}
      </div>
    {/if}
  </svelte:fragment>
</TableWithDrawer>
