<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Loader2,
    Play,
    Pause,
    RotateCw,
    Check,
    ChevronDown,
    Info,
  } from "lucide-svelte";
  import * as Popover from "$lib/components/ui/popover";
  import { toast } from "svelte-sonner";
  import TableWithDrawer from "$lib/components/TableWithDrawer.svelte";

  // Receive necessary props
  export let messages: any[];
  export let totalCount: number = 0;
  export let pageSize: number;
  export let live;
  export let paused: boolean = false;
  export let showAcked: boolean = true;
  export let consumer: { type: string };
  export let metrics: { messages_failing_count: number };
  export let showAckId: string | null;

  let page: number = 0;
  let loading = false;
  let selectedMessage = null;
  let selectedMessageAcked = false;
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
  let resettingMessageVisibility = false;

  // Add this new state variable near the top with other state variables
  let isAcknowledging = false;
  let isResettingAll = false;
  let isPopoverOpen = false;

  // Add these new state variables
  let messageShapeOpen = false;
  let logsOpen = true;
  let transformedMessageOpen = false;

  // Add computed property for message delivery state
  $: isMessageDelivered =
    selectedMessage?.state === "delivered" ||
    selectedMessage?.state === "acknowledged";

  $: pageCount = Math.ceil(totalCount / pageSize);

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
        if (calculatedPageSize !== pageSize) {
          pageSize = calculatedPageSize;
          // Send new pageSize to backend
          live.pushEvent("update_page_size", { page_size: pageSize });
        }
      }
    };

    // Initial calculation
    updatePageSize();

    // If showAckId is provided, find and open the message drawer for that message
    if (showAckId && messages.length > 0) {
      const messageToShow = messages.find((msg) => msg.ack_id === showAckId);
      if (messageToShow) {
        openDrawer(messageToShow);
      }
    }

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

  function getMessageStateColor(state) {
    switch (state) {
      case "blue":
        return "bg-blue-200";
      case "yellow":
        return "bg-yellow-200";
      case "green":
        return "bg-green-200";
      case "red":
        return "bg-red-200";
      default:
        return "bg-gray-200";
    }
  }

  function openDrawer(message) {
    selectedMessage = message;

    selectedMessageAcked =
      selectedMessage?.state === "acknowledged" ||
      (selectedMessage?.state === "delivered" &&
        consumer.type !== "sequin_stream");

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
      { message_ack_id: message.ack_id },
      (reply) => {
        isLoadingMessageData = false;
        if (reply.error) {
          messageDataError = reply.error;
        } else {
          messageData = reply.data;
        }
      },
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
      },
    );
  }

  function closeDrawer() {
    isDrawerOpen = false;
    selectedMessage = null;
    selectedMessageAcked = false;
    messageData = null;
    messageDataError = null;
    resettingMessageVisibility = false;

    // Reset log variables
    messageLogs = null;
    isLoadingMessageLogs = false;
    messageLogsError = null;
    messageShapeOpen = false;
    logsOpen = true;
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
      },
    );
  }

  function toggleShowAcked() {
    showAcked = !showAcked;
    live.pushEvent("toggle_show_acked", { show_acked: showAcked });
    changePage(0);
  }

  function resetMessageVisibility(ackId) {
    live.pushEvent("reset_message_visibility", { ack_id: ackId }, (reply) => {
      resettingMessageVisibility = false;

      if (reply.error) {
        // Handle error (e.g., show a toast notification)
        toast.error(`Failed to reset message visibility: ${reply.error}`);
      } else {
        // Optionally, update the message state locally or refresh the drawer
        selectedMessage = reply.updated_message;
        refreshLogs();
      }
    });
  }

  function acknowledgeMessage(ackId) {
    isAcknowledging = true;
    live.pushEvent("acknowledge_message", { ack_id: ackId }, (reply) => {
      isAcknowledging = false;
      if (reply.ok) {
        selectedMessageAcked = true;

        if (reply.updated_message) {
          selectedMessage = reply.updated_message;
        }

        refreshLogs();
      }
    });
  }

  function handleResetAll() {
    isPopoverOpen = false;
    isResettingAll = true;
    live.pushEvent("reset_all_visibility", {}, (reply) => {
      isResettingAll = false;
    });
  }
</script>

<TableWithDrawer
  title="Messages"
  items={messages}
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
      class="flex items-center space-x-2"
      on:click={toggleShowAcked}
    >
      <Switch checked={showAcked} />
      <span>Show Acked</span>
    </Button>

    <Popover.Root bind:open={isPopoverOpen}>
      <Popover.Trigger asChild let:builder>
        <Button
          builders={[builder]}
          variant="outline"
          size="sm"
          disabled={metrics.messages_failing_count === 0 || isResettingAll}
          class="flex items-center space-x-2"
        >
          {#if isResettingAll}
            <Loader2 class="h-4 w-4 mr-1 animate-spin" />
          {:else}
            <RotateCw class="h-4 w-4 mr-1" />
          {/if}
          <span>Redeliver All</span>
        </Button>
      </Popover.Trigger>
      <Popover.Content class="w-80 p-4">
        <div class="grid gap-4">
          <div class="space-y-2">
            <h4 class="font-medium leading-none">Confirm Redeliver All</h4>
            <p class="text-sm text-muted-foreground">
              This will clear the backoff for all messages, causing them to be
              redelivered. Are you sure you want to continue?
            </p>
          </div>
          <div class="flex justify-end gap-2">
            <Button
              variant="outline"
              size="sm"
              on:click={() => (isPopoverOpen = false)}
            >
              Cancel
            </Button>
            <Button variant="default" size="sm" on:click={handleResetAll}>
              Confirm
            </Button>
          </div>
        </div>
      </Popover.Content>
    </Popover.Root>

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
    <h1 class="text-2xl font-bold">No Messages</h1>
    <h2 class="text-gray-600">
      {#if showAcked}
        Messages will appear here when they are pending, delivered, or
        acknowledged.
      {:else}
        You have no pending messages. Try toggling "Show Acked" to see
        acknowledged messages.
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
      PKs
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
      Backoff Until
    </th>
    <th
      class="px-2 py-1 text-left text-2xs font-medium text-gray-500 uppercase tracking-wider"
    >
      Committed At
    </th>
  </svelte:fragment>

  <svelte:fragment slot="sampleRow">
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs">sample</td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">sample</td>
  </svelte:fragment>

  <svelte:fragment slot="row" let:item>
    <td class="px-2 py-1 whitespace-nowrap text-2xs w-fit">
      <span
        class={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getMessageStateColor(item.state_color)} text-gray-800`}
      >
        {item.state}
      </span>
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
      >{item.table_schema}.{item.table_name}</td
    >
    <td class="px-2 py-1 whitespace-nowrap text-2xs">{item.record_pks}</td>
    <td
      class="px-2 py-1 whitespace-nowrap text-2xs"
      class:text-red-600={isHighDeliveryCount(item.deliver_count)}
      class:font-bold={isHighDeliveryCount(item.deliver_count)}
    >
      {item.deliver_count}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
      {item.last_delivered_at ? formatDate(item.last_delivered_at) : "N/A"}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500">
      {item.not_visible_until ? formatDate(item.not_visible_until) : "N/A"}
    </td>
    <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
      >{formatDate(item.commit_timestamp)}</td
    >
  </svelte:fragment>

  <svelte:fragment slot="drawerTitle">Message Details</svelte:fragment>

  <svelte:fragment slot="drawerContent">
    {#if selectedMessage}
      <div class="space-y-4">
        <!-- Message Details & Timestamps Combined -->
        <div class="bg-gray-50 p-4 rounded-lg space-y-2">
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">State:</span>
            <span
              class={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${getMessageStateColor(selectedMessage.state_color)} text-gray-800`}
            >
              {selectedMessage.state}
            </span>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Record PKs:</span>
            <span class="text-sm text-gray-900"
              >{selectedMessage.record_pks}</span
            >
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Commit LSN:</span>
            <span class="text-sm text-gray-900"
              >{selectedMessage.commit_lsn}</span
            >
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500">Deliver Count:</span
            >
            <span class="text-sm text-gray-900"
              >{selectedMessage.deliver_count}</span
            >
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500"
              >Last Delivered:</span
            >
            <span class="text-sm text-gray-900">
              {selectedMessage.last_delivered_at
                ? formatDate(selectedMessage.last_delivered_at)
                : "N/A"}
            </span>
          </div>
          <div class="flex justify-between items-center">
            <span class="text-sm font-medium text-gray-500"
              >Not visible until:</span
            >
            <span class="text-sm text-gray-900">
              {selectedMessage.not_visible_until
                ? formatDate(selectedMessage.not_visible_until)
                : "N/A"}
            </span>
          </div>
        </div>

        <!-- Actions Section -->
        <div class="bg-gray-50 p-4 rounded-lg space-y-4">
          <div class="flex items-center justify-between">
            <h3 class="text-sm font-semibold">Actions</h3>
            <Popover.Root>
              <Popover.Trigger asChild let:builder>
                <Button builders={[builder]} variant="ghost" size="sm">
                  <Info class="h-4 w-4" />
                </Button>
              </Popover.Trigger>
              <Popover.Content class="w-80">
                <div class="p-4 space-y-2">
                  <h4 class="font-medium">About Actions</h4>
                  <p class="text-sm text-gray-600">
                    <strong>Redeliver</strong>
                    <br />
                    Reset visibility window to immediately redeliver backed-off messages.<br
                    /><br />
                    <strong>Acknowledge</strong>
                    <br />
                    Remove message from the sink's delivery queue.
                  </p>
                </div>
              </Popover.Content>
            </Popover.Root>
          </div>

          <div class="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              on:click={() => resetMessageVisibility(selectedMessage.ack_id)}
              disabled={selectedMessage.state !== "backing off"}
              class="flex items-center space-x-2"
            >
              <RotateCw class="h-4 w-4" />
              <span>Redeliver</span>
            </Button>

            <Button
              variant="outline"
              size="sm"
              on:click={() => acknowledgeMessage(selectedMessage.ack_id)}
              disabled={selectedMessageAcked || isAcknowledging}
              class="flex items-center space-x-2"
            >
              {#if isAcknowledging}
                <Loader2 class="h-4 w-4 animate-spin" />
              {:else}
                <Check class="h-4 w-4" />
              {/if}
              <span>Acknowledge</span>
            </Button>
          </div>
        </div>

        <!-- Message Shape Accordion -->
        <div class="border rounded-lg">
          <button
            class="w-full px-4 py-2 flex flex-col items-start"
            on:click={() =>
              !isMessageDelivered && (messageShapeOpen = !messageShapeOpen)}
            class:opacity-50={isMessageDelivered}
            class:cursor-not-allowed={isMessageDelivered}
          >
            <div class="w-full flex justify-between items-center">
              <span class="font-medium">Message Shape</span>
              {#if !isMessageDelivered}
                <div class:rotate-180={messageShapeOpen}>
                  <ChevronDown class="h-4 w-4 transform transition-transform" />
                </div>
              {/if}
            </div>
            {#if isMessageDelivered}
              <span class="text-sm text-gray-500 mt-1"
                >Message payloads are not retained after delivery</span
              >
            {/if}
          </button>

          {#if messageShapeOpen && !isMessageDelivered}
            <div class="px-4 pb-4">
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
                {#if selectedMessage.transformed_message}
                  <span class="text-sm font-medium">Original Message</span>
                {/if}
                <pre class="bg-gray-100 p-4 rounded-lg overflow-x-auto"><code
                    >{JSON.stringify(messageData, null, 2)}</code
                  ></pre>
              {/if}

              <!-- Transformed Message Section -->
              {#if selectedMessage.transformed_message}
                <div class="mt-4">
                  <span class="text-sm font-medium">Transformed Message</span>
                  <div class="mt-2">
                    <pre
                      class="bg-gray-100 p-4 rounded-lg overflow-x-auto"><code
                        >{JSON.stringify(
                          selectedMessage.transformed_message,
                          null,
                          2,
                        )}</code
                      ></pre>
                  </div>
                </div>
              {/if}
            </div>
          {/if}
        </div>

        <!-- Logs Accordion (open by default) -->
        <div class="border rounded-lg">
          <button
            class="w-full px-4 py-2 flex justify-between items-center"
            on:click={() => (logsOpen = !logsOpen)}
          >
            <span class="font-medium">Logs</span>
            <div class="flex items-center gap-2">
              <Button
                variant="ghost"
                size="sm"
                on:click={(e) => {
                  e.stopPropagation();
                  refreshLogs();
                }}
                disabled={isRefreshingLogs}
              >
                {#if isRefreshingLogs}
                  <Loader2 class="h-4 w-4 animate-spin" />
                {:else}
                  <RotateCw class="h-4 w-4" />
                {/if}
              </Button>
              <div class:rotate-180={logsOpen}>
                <ChevronDown class="h-4 w-4 transform transition-transform" />
              </div>
            </div>
          </button>

          {#if logsOpen}
            <div class="px-4 pb-4">
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
                                  {new Date(log.timestamp).toLocaleString()}
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
          {/if}
        </div>
      </div>
    {/if}
  </svelte:fragment>
</TableWithDrawer>

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
