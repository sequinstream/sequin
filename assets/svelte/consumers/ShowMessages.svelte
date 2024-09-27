<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import { Switch } from "$lib/components/ui/switch";
  import {
    ArrowUpRight,
    ChevronLeft,
    ChevronRight,
    Loader2,
    X,
    Play,
    Pause,
    RefreshCw,
  } from "lucide-svelte";
  import { slide, fade } from "svelte/transition";

  // Receive necessary props
  export let messages: any[];
  export let totalCount: number = 0;
  export let pageSize: number;
  export let live;
  export let paused: boolean = false;
  export let showAcked: boolean = true;

  let page: number = 0;
  let loading = false;
  let selectedMessage = null;
  let isDrawerOpen = false;
  let messageData = null;
  let messageDataError = null;
  let isLoadingMessageData = false;

  // New state variables for logs
  let messageLogs = null;
  let isLoadingMessageLogs = false;
  let messageLogsError = null;
  let isRefreshingLogs = false;

  let rowHeight = 0; // Will be calculated
  let totalAvailableHeight = 0;

  // Add this function to determine the message state
  function getMessageState(message) {
    if (message.type === "acknowledged_message") {
      return "acknowledged";
    } else if (
      message.deliver_count > 0 &&
      new Date(message.not_visible_until) > new Date()
    ) {
      return "delivered";
    } else {
      return "pending";
    }
  }

  // Add this function to get the appropriate color for the state
  function getStateColor(state) {
    switch (state) {
      case "pending":
        return "bg-gray-200";
      case "delivered":
        return "bg-blue-200";
      case "acknowledged":
        return "bg-green-200";
      default:
        return "bg-gray-200";
    }
  }

  onMount(() => {
    // Calculate row height after the component is mounted
    const sampleRow = document.querySelector("tr.sample-row");
    if (sampleRow) {
      rowHeight = sampleRow.getBoundingClientRect().height;
    }

    // Calculate the available height
    const updatePageSize = () => {
      const headerElement = document.querySelector(".header");
      const messagesHeaderElement = document.querySelector(".messages-header");
      const messagesFooterElement = document.querySelector(".messages-footer");
      let headerHeight = 0;
      let messagesHeaderHeight = 0;
      let messagesFooterHeight = 0;

      if (headerElement) {
        headerHeight = headerElement.getBoundingClientRect().height;
      }

      if (messagesHeaderElement) {
        messagesHeaderHeight =
          messagesHeaderElement.getBoundingClientRect().height;
      }

      if (messagesFooterElement) {
        messagesFooterHeight =
          messagesFooterElement.getBoundingClientRect().height;
      }

      totalAvailableHeight =
        window.innerHeight -
        headerHeight -
        messagesHeaderHeight -
        messagesFooterHeight -
        200;

      if (rowHeight > 0) {
        const calculatedPageSize = Math.floor(totalAvailableHeight / rowHeight);
        console.log("calculatedPageSize", calculatedPageSize);
        if (calculatedPageSize !== pageSize) {
          pageSize = calculatedPageSize;
          // Send new pageSize to backend
          live.pushEvent("update_page_size", { page_size: pageSize });
        }
      }
    };

    // Initial calculation
    updatePageSize();

    // Add event listener for window resize
    window.addEventListener("resize", updatePageSize);

    return () => {
      window.removeEventListener("resize", updatePageSize);
    };
  });

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < Math.ceil(totalCount / pageSize)) {
      page = newPage;
      loading = true;
      live.pushEvent("change_page", { page: page }, (reply) => {
        loading = false;
      });
    }
  }

  function togglePause() {
    paused = !paused;
    if (paused) {
      live.pushEvent("pause_updates", {});
    } else {
      live.pushEvent("resume_updates", {});
    }
  }

  // Function to map log status to color
  function getLogLevelColor(status) {
    switch (status?.toLowerCase()) {
      case "error":
        return "bg-red-500";
      case "warn":
      case "warning":
        return "bg-yellow-500";
      case "info":
        return "bg-blue-500";
      case "debug":
        return "bg-gray-500";
      default:
        return "bg-gray-400";
    }
  }

  function openDrawer(message) {
    selectedMessage = message;
    isDrawerOpen = true;
    messageData = null;
    messageDataError = null;
    isLoadingMessageData = true;

    // Initialize log variables
    messageLogs = null;
    isLoadingMessageLogs = true;
    messageLogsError = null;

    live.pushEvent(
      "fetch_message_data",
      { message_id: message.id },
      (reply) => {
        isLoadingMessageData = false;
        if (reply.error) {
          messageDataError = reply.error;
        } else {
          messageData = reply.data;
        }
      }
    );

    live.pushEvent(
      "fetch_message_logs",
      { trace_id: message.trace_id },
      (reply) => {
        isLoadingMessageLogs = false;
        if (reply.error) {
          messageLogsError = reply.error;
        } else {
          messageLogs = reply.logs;
        }
      }
    );
  }

  function closeDrawer() {
    isDrawerOpen = false;
    selectedMessage = null;
    messageData = null;
    messageDataError = null;

    // Reset log variables
    messageLogs = null;
    isLoadingMessageLogs = false;
    messageLogsError = null;
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

  function isHighDeliveryCount(count: number) {
    return count > 1;
  }

  function refreshLogs() {
    isRefreshingLogs = true;
    isLoadingMessageLogs = true;
    messageLogsError = null;

    live.pushEvent(
      "fetch_message_logs",
      { trace_id: selectedMessage.trace_id },
      (reply) => {
        isLoadingMessageLogs = false;
        isRefreshingLogs = false;
        if (reply.error) {
          messageLogsError = reply.error;
        } else {
          messageLogs = reply.logs;
        }
      }
    );
  }

  function toggleShowAcked() {
    showAcked = !showAcked;
    live.pushEvent("toggle_show_acked", { show_acked: showAcked });
    changePage(0);
  }

  $: pageCount = Math.ceil(totalCount / pageSize);
</script>

<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <!-- Header Div -->
    <div class="flex items-center justify-between mb-4 messages-header">
      <h1 class="text-2xl font-bold">Messages</h1>
      <div class="flex items-center space-x-4">
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
      </div>
    </div>

    {#if loading}
      <div
        class="absolute inset-0 bg-white bg-opacity-50 flex items-center justify-center z-10"
      >
        <Loader2 class="h-6 w-6 animate-spin text-gray-500" />
      </div>
    {/if}
    {#if messages.length === 0}
      <div
        class="w-full bg-white border border-gray-300 rounded-lg p-6 text-center space-y-2 min-h-96 flex flex-col items-center justify-center"
      >
        <h1 class="text-2xl font-bold">No Messages</h1>
        <h2 class="text-gray-600">
          {#if showAcked}
            Messages will appear here when they are delivered, pending, or
            acknowledged.
          {:else}
            You have no pending messages. Try toggling "Show Acked" to see
            acknowledged messages.
          {/if}
        </h2>
      </div>
    {:else}
      <table class="w-full bg-white border border-gray-300">
        <thead>
          <tr class="bg-gray-100">
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              ID
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              State
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              PKs
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              Commit LSN
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              Deliver Count
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              Last Delivered At
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              Not Visible Until
            </th>
            <th
              class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
            >
              Inserted At
            </th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-200">
          <tr
            class="sample-row"
            style="visibility: hidden; position: absolute;"
          >
            <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
              sample
            </td>
            <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
              sample
            </td>
            <td class="px-2 py-1 whitespace-nowrap text-2xs">sample</td>
            <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
              sample
            </td>
          </tr>
          {#each messages as message}
            <tr
              class="relative hover:bg-gray-50 cursor-pointer"
              on:click={() => openDrawer(message)}
            >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{message.id}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs">
                <span
                  class={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getStateColor(
                    getMessageState(message)
                  )} text-gray-800`}
                >
                  {getMessageState(message)}
                </span>
              </td>
              <td class="px-2 py-1 whitespace-nowrap text-2xs">
                {message.record_pks}
              </td>
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{message.commit_lsn}</td
              >
              <td
                class="px-2 py-1 whitespace-nowrap text-2xs"
                class:text-red-600={isHighDeliveryCount(message.deliver_count)}
                class:font-bold={isHighDeliveryCount(message.deliver_count)}
              >
                {message.deliver_count}
              </td>
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
                {message.last_delivered_at
                  ? formatDate(message.last_delivered_at)
                  : "N/A"}
              </td>
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
                {message.not_visible_until
                  ? formatDate(message.not_visible_until)
                  : "N/A"}
              </td>
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{formatDate(message.inserted_at)}</td
              >
            </tr>
          {/each}
        </tbody>
      </table>
      <!-- Pagination Div -->
      <div class="flex items-center justify-between mt-4 messages-footer">
        <div class="flex space-x-2">
          <Button
            variant="outline"
            size="sm"
            on:click={() => changePage(page - 1)}
            disabled={page === 0}
            class="flex items-center"
          >
            <ChevronLeft class="h-4 w-4 mr-1" />
            <span>Previous</span>
          </Button>
          <Button
            variant="outline"
            size="sm"
            on:click={() => changePage(page + 1)}
            disabled={page >= pageCount - 1}
            class="flex items-center"
          >
            <span>Next</span>
            <ChevronRight class="h-4 w-4 ml-1" />
          </Button>
        </div>
        <div class="text-sm text-gray-600">
          Page {page + 1} of {pageCount}
        </div>
      </div>
    {/if}
  </div>
</div>

<!-- Side Drawer -->
{#if isDrawerOpen}
  <div class="fixed inset-0 overflow-hidden z-50">
    <div class="absolute inset-0 overflow-hidden">
      <div
        class="absolute inset-0 bg-gray-500 bg-opacity-75 transition-opacity"
        on:click={closeDrawer}
        transition:fade={{ duration: 200 }}
        role="button"
        tabindex="0"
        on:keydown={(e) => e.key === "Enter" && closeDrawer()}
      ></div>
      <section
        class="absolute inset-y-0 right-0 pl-10 max-w-full flex sm:pl-16"
      >
        <div
          class="w-screen max-w-2xl"
          transition:slide={{ duration: 300, axis: "x" }}
        >
          <div
            class="h-full flex flex-col py-6 bg-white shadow-xl overflow-y-scroll"
          >
            <div class="px-4 sm:px-6">
              <div class="flex items-start justify-between">
                <h2 class="text-lg font-medium text-gray-900">
                  Message Details
                </h2>
                <div class="ml-3 h-7 flex items-center">
                  <button
                    on:click={closeDrawer}
                    class="bg-white rounded-md text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                  >
                    <span class="sr-only">Close panel</span>
                    <X class="h-6 w-6" />
                  </button>
                </div>
              </div>
            </div>
            <div class="mt-6 relative flex-1 px-4 sm:px-6">
              {#if selectedMessage}
                <div class="space-y-8">
                  <!-- Message Details Section -->
                  <div class="bg-gray-50 p-4 rounded-lg space-y-2">
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500">ID:</span>
                      <span class="text-sm text-gray-900"
                        >{selectedMessage.id}</span
                      >
                    </div>
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >State:</span
                      >
                      <span
                        class={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getStateColor(
                          getMessageState(selectedMessage)
                        )} text-gray-800`}
                      >
                        {getMessageState(selectedMessage)}
                      </span>
                    </div>
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >Record PKs:</span
                      >
                      <span class="text-sm text-gray-900"
                        >{selectedMessage.record_pks}</span
                      >
                    </div>
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >Commit LSN:</span
                      >
                      <span class="text-sm text-gray-900"
                        >{selectedMessage.commit_lsn}</span
                      >
                    </div>
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >Deliver Count:</span
                      >
                      <span class="text-sm text-gray-900"
                        >{selectedMessage.deliver_count}</span
                      >
                    </div>
                  </div>

                  <!-- Message Data Section -->
                  <div>
                    <h3 class="text-lg font-semibold mb-4">Message Data</h3>
                    {#if isLoadingMessageData}
                      <div class="flex justify-center items-center h-32">
                        <Loader2 class="h-8 w-8 animate-spin text-gray-500" />
                      </div>
                    {:else if messageDataError}
                      <div
                        class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative"
                        role="alert"
                      >
                        <strong class="font-bold">Error:</strong>
                        <span class="block sm:inline">{messageDataError}</span>
                      </div>
                    {:else if messageData}
                      <pre
                        class="bg-gray-100 p-4 rounded-lg overflow-x-auto"><code
                          >{JSON.stringify(messageData, null, 2)}</code
                        ></pre>
                    {/if}
                  </div>

                  <!-- Timestamps Section -->
                  <div>
                    <h3 class="text-lg font-semibold mb-4">Timestamps</h3>
                    <div class="bg-gray-50 p-4 rounded-lg space-y-2">
                      <div class="flex justify-between items-center">
                        <span class="text-sm font-medium text-gray-500"
                          >Last Delivered At:</span
                        >
                        <span class="text-sm text-gray-900">
                          {selectedMessage.last_delivered_at
                            ? formatDate(selectedMessage.last_delivered_at)
                            : "N/A"}
                        </span>
                      </div>
                      <div class="flex justify-between items-center">
                        <span class="text-sm font-medium text-gray-500"
                          >Not Visible Until:</span
                        >
                        <span class="text-sm text-gray-900">
                          {selectedMessage.not_visible_until
                            ? formatDate(selectedMessage.not_visible_until)
                            : "N/A"}
                        </span>
                      </div>
                      <div class="flex justify-between items-center">
                        <span class="text-sm font-medium text-gray-500"
                          >Inserted At:</span
                        >
                        <span class="text-sm text-gray-900"
                          >{formatDate(selectedMessage.inserted_at)}</span
                        >
                      </div>
                    </div>
                  </div>

                  <!-- Logs Section -->
                  <div>
                    <div class="flex justify-between items-center mb-4">
                      <h3 class="text-lg font-semibold">Logs</h3>
                      <Button
                        variant="outline"
                        size="sm"
                        on:click={refreshLogs}
                        disabled={isRefreshingLogs}
                      >
                        {#if isRefreshingLogs}
                          <Loader2 class="h-4 w-4 mr-2 animate-spin" />
                        {:else}
                          <RefreshCw class="h-4 w-4 mr-2" />
                        {/if}
                        Refresh
                      </Button>
                    </div>
                    {#if messageLogsError}
                      <div
                        class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded relative"
                        role="alert"
                      >
                        <strong class="font-bold">Error:</strong>
                        <span class="block sm:inline">{messageLogsError}</span>
                      </div>
                    {:else if messageLogs && messageLogs.length > 0}
                      <div class="overflow-x-auto">
                        <table class="w-full">
                          <tbody>
                            {#each messageLogs as log}
                              <tr>
                                <td class="py-1" colspan="3">
                                  <div
                                    class="flex border border-gray-200 rounded overflow-hidden"
                                  >
                                    <div
                                      class={`w-1 ${getLogLevelColor(log.status)}`}
                                    ></div>
                                    <div class="flex-grow p-2">
                                      <div class="text-xs text-gray-500 mb-1">
                                        {new Date(
                                          log.timestamp
                                        ).toLocaleString()}
                                      </div>
                                      <div class="text-sm text-gray-900">
                                        {log.message}
                                      </div>
                                    </div>
                                  </div>
                                </td>
                              </tr>
                            {/each}
                          </tbody>
                        </table>
                      </div>
                    {:else if isLoadingMessageLogs}
                      <div class="space-y-2 animate-pulse">
                        {#each Array(3) as _}
                          <div
                            class="flex border border-gray-200 rounded overflow-hidden"
                          >
                            <div class="w-1 bg-gray-200"></div>
                            <div class="flex-grow p-2">
                              <div
                                class="w-1/3 h-2 my-2 bg-gray-200 rounded-full"
                              ></div>
                              <div
                                class="w-2/3 h-3 my-2 bg-gray-200 rounded-full"
                              ></div>
                            </div>
                          </div>
                        {/each}
                      </div>
                    {:else}
                      <div class="text-sm text-gray-500">
                        No logs available for this message.
                      </div>
                    {/if}
                  </div>
                </div>
              {/if}
            </div>
          </div>
        </div>
      </section>
    </div>
  </div>
{/if}

<style>
  table {
    border-collapse: separate;
    border-spacing: 0;
  }

  .text-2xs {
    font-size: 0.65rem;
    line-height: 1rem;
  }

  tr {
    position: relative;
  }

  .overflow-y-scroll {
    scrollbar-width: thin;
    scrollbar-color: #cbd5e0 #f7fafc;
  }

  .overflow-y-scroll::-webkit-scrollbar {
    width: 8px;
  }

  .overflow-y-scroll::-webkit-scrollbar-track {
    background: #f7fafc;
  }

  .overflow-y-scroll::-webkit-scrollbar-thumb {
    background-color: #cbd5e0;
    border-radius: 4px;
  }

  .overflow-x-auto {
    overflow-x: auto;
  }

  td {
    vertical-align: top;
  }

  .w-1 {
    width: 4px;
  }

  .text-sm {
    font-size: 0.875rem;
    line-height: 1.25rem;
  }

  .text-gray-500 {
    color: #6b7280;
  }

  .text-gray-900 {
    color: #111827;
  }

  tr + tr {
    margin-top: 0.5rem;
  }

  /* You can adjust or add more styles as needed */
</style>
