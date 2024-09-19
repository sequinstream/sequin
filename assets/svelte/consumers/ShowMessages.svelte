<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    ArrowUpRight,
    ChevronLeft,
    ChevronRight,
    Loader2,
    X,
    Play,
    Pause,
  } from "lucide-svelte";
  import { slide, fade } from "svelte/transition";

  // Receive necessary props
  export let consumer: { id: string };
  export let messages: any[];
  export let totalCount: number = 0;
  export let pageSize: number;
  export let live;
  export let paused: boolean = false;

  let page: number = 0;
  let loading = false;
  let selectedMessage = null;
  let isDrawerOpen = false;
  let messageData = null;
  let messageDataError = null;
  let isLoadingMessageData = false;

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < Math.ceil(totalCount / pageSize)) {
      page = newPage;
      loading = true;
      live.pushEvent("change_page", { page: page }, (reply) => {
        loading = false;
        // Handle the reply if needed
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

  function openDrawer(message) {
    selectedMessage = message;
    isDrawerOpen = true;
    messageData = null;
    messageDataError = null;
    isLoadingMessageData = true;
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
  }

  function closeDrawer() {
    isDrawerOpen = false;
    selectedMessage = null;
    messageData = null;
    messageDataError = null;
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

  $: startIndex = page * pageSize + 1;
  $: endIndex = Math.min((page + 1) * pageSize, totalCount);
  $: totalPages = Math.ceil(totalCount / pageSize);

  $: {
    console.log("totalPages", totalPages);
    console.log("page", page);
    console.log("totalCount", totalCount);
    console.log("pageSize", pageSize);
    console.log("startIndex", startIndex);
    console.log("endIndex", endIndex);
  }
</script>

<div class="flex flex-col flex-1">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
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
          Messages will appear here when they are delivered or pending delivery.
          Acknowledged messages are not displayed.
        </h2>
      </div>
    {:else}
      <div class="flex items-center justify-between mb-4">
        <h1 class="text-2xl font-bold">Messages</h1>
        <div>
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
              Type
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
          {#each messages as message}
            <tr
              class="relative hover:bg-gray-50 cursor-pointer"
              on:click={() => openDrawer(message)}
            >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{message.id}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{message.type}</td
              >
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
      <div class="flex items-center justify-between mt-4">
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
            disabled={endIndex >= totalCount}
            class="flex items-center"
          >
            <span>Next</span>
            <ChevronRight class="h-4 w-4 ml-1" />
          </Button>
        </div>
        <div class="text-sm text-gray-600">
          Showing {startIndex} – {endIndex} of {totalCount}
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
                        >Type:</span
                      >
                      <span class="text-sm text-gray-900"
                        >{selectedMessage.type}</span
                      >
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

                  <!-- Consumer Section -->
                  <div>
                    <h3 class="text-lg font-semibold mb-4">Consumer</h3>
                    <div
                      class="bg-gray-100 p-4 rounded-lg flex justify-between items-center"
                    >
                      <h4 class="font-medium">{consumer.id}</h4>
                      <a href={`/consumers/${consumer.id}`}>
                        <Button variant="outline" size="sm">
                          View Consumer
                          <ArrowUpRight class="h-4 w-4 ml-2" />
                        </Button>
                      </a>
                    </div>
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
</style>
