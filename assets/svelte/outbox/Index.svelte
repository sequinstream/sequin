<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    ArrowUpRight,
    Radio,
    Pause,
    Play,
    ChevronLeft,
    ChevronRight,
    Loader2,
    X,
  } from "lucide-svelte";
  import { slide, fade } from "svelte/transition";

  export let consumer_id: string | null;
  export let messages: any[];
  export let consumers: any[];
  export let live;
  export let totalCount: number = 0;
  export let pageSize: number;

  let paused: boolean = false;
  let page: number = 0;
  let selectedConsumer = consumer_id;
  let loading = false;
  let selectedMessage = null;
  let isDrawerOpen = false;
  let messageData = null;
  let messageDataError = null;
  let isLoadingMessageData = false;

  // Add this function to set the default consumer
  function setDefaultConsumer() {
    if (!selectedConsumer && consumers.length > 0) {
      selectedConsumer = consumers[0].id;
      selectConsumer();
    }
  }

  // Add this function to update URL params
  function updateUrlParams() {
    const params = new URLSearchParams(window.location.search);

    if (selectedConsumer) {
      params.set("consumer", selectedConsumer);
    } else {
      params.delete("consumer");
    }

    if (paused) {
      params.set("paused", "true");
    } else {
      params.delete("paused");
    }

    if (page > 0) {
      params.set("page", page.toString());
    } else {
      params.delete("page");
    }

    const newUrl = `/outbox${params.toString() ? "?" + params.toString() : ""}`;
    history.pushState(null, "", newUrl);
  }

  function selectConsumer() {
    if (selectedConsumer) {
      loading = true;
      live.pushEvent(
        "select_consumer",
        { consumer_id: selectedConsumer },
        (reply) => {
          loading = false;
          // Handle the reply if needed
        }
      );
      updateUrlParams();
    }
  }

  function changePage(newPage: number) {
    if (newPage >= 0 && newPage < Math.ceil(totalCount / pageSize)) {
      page = newPage;
      loading = true;
      live.pushEvent("change_page", { page: page }, (reply) => {
        loading = false;
        // Handle the reply if needed
      });
      updateUrlParams();
    }
  }

  function togglePause() {
    paused = !paused;
    if (paused) {
      live.pushEvent("pause_updates", {});
    } else {
      live.pushEvent("resume_updates", {});
    }
    updateUrlParams();
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

  $: {
    if (consumer_id) {
      selectedConsumer = consumer_id;
    } else {
      setDefaultConsumer();
    }
  }

  onMount(() => {
    setDefaultConsumer();

    const params = new URLSearchParams(window.location.search);
    page = parseInt(params.get("page") || "0");
    paused = params.get("paused") === "true";
    consumer_id = params.get("consumer") || null;
  });

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
</script>

<div class="container mx-auto p-4">
  <div class="flex justify-between items-center mb-6">
    <h1 class="text-3xl font-bold flex items-center gap-2">
      <Radio class="h-6 w-6" />
      Outbox
    </h1>
    <div class="flex items-center gap-4">
      <Button variant="outline" on:click={togglePause} disabled={loading}>
        {#if paused}
          <Play class="h-4 w-4" />
        {:else}
          <Pause class="h-4 w-4" />
        {/if}
      </Button>
    </div>
  </div>

  <div class="mb-6 flex items-center gap-4">
    <select
      bind:value={selectedConsumer}
      on:change={selectConsumer}
      class="select-filter text-sm"
      disabled={consumers.length === 0 || loading}
    >
      {#each consumers as consumer}
        <option value={consumer.id}>{consumer.name} ({consumer.type})</option>
      {/each}
    </select>
  </div>

  {#if consumers.length === 0}
    <div class="w-full flex items-center justify-center min-h-[400px]">
      <div class="w-full max-w-4xl bg-white pt-6 pb-8 rounded-lg text-center">
        <div class="text-center py-8 w-2/3 mx-auto">
          <h2 class="text-2xl font-bold mb-4">No Consumers Found</h2>
          <p class="text-gray-600 mb-4">
            You need to create a consumer before you can view the outbox.
          </p>
          <p class="text-gray-600 mb-6">
            Once created, you'll be able to view and manage outgoing messages
            for each consumer.
          </p>
          <a href="/consumers/new" class="inline-block">
            <Button variant="default" class="mb-4">
              Create Consumer
              <ArrowUpRight class="h-4 w-4 ml-2" />
            </Button>
          </a>
        </div>
        <div
          class="mb-6 drop-shadow-lg relative pb-[56.25%] overflow-hidden max-w-full"
        >
          <div
            class="wistia_responsive_padding"
            style="padding:59.38% 0 0 0;position:relative;"
          >
            <div
              class="wistia_responsive_wrapper"
              style="height:100%;left:0;position:absolute;top:0;width:100%;"
            >
              <div
                class="wistia_embed wistia_async_hw5rdi12ss seo=false videoFoam=true"
                style="height:100%;position:relative;width:100%"
              >
                &nbsp;
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  {:else if messages.length > 0}
    <div class="overflow-x-auto max-w-full relative">
      {#if loading}
        <div
          class="absolute inset-0 bg-white bg-opacity-50 flex items-center justify-center z-10"
        >
          <Loader2 class="h-6 w-6 animate-spin text-gray-500" />
        </div>
      {/if}
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
    </div>

    <div class="flex justify-between items-center mt-4">
      <div>
        Showing {startIndex} to {endIndex} of {totalCount} messages
      </div>
      <div class="flex gap-2">
        <Button
          variant="outline"
          on:click={() => changePage(page - 1)}
          disabled={page === 0 || loading}
        >
          <ChevronLeft class="h-4 w-4" />
          Previous
        </Button>
        <Button
          variant="outline"
          on:click={() => changePage(page + 1)}
          disabled={endIndex === totalCount || loading}
        >
          Next
          <ChevronRight class="h-4 w-4" />
        </Button>
      </div>
    </div>
  {:else if selectedConsumer}
    <div class="w-full flex items-center justify-center min-h-[400px]">
      <div class="w-full max-w-4xl bg-white pt-6 pb-8 rounded-lg text-center">
        <h2 class="text-2xl font-bold mb-4">Message Outbox</h2>
        <p class="text-gray-600 mb-4">
          No messages found for the selected consumer.
        </p>
        <p class="text-gray-600">
          Messages will be displayed here as soon as the consumer starts
          processing them.
        </p>
      </div>
    </div>
  {:else}
    <div class="w-full flex items-center justify-center min-h-[400px]">
      <div class="w-full max-w-4xl bg-white pt-6 pb-8 rounded-lg text-center">
        <h2 class="text-2xl font-bold mb-4">Select a Consumer</h2>
        <p class="text-gray-600">
          Please select a consumer from the dropdown above to view its outbox
          messages.
        </p>
      </div>
    </div>
  {/if}
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
                      <h4 class="font-medium">{selectedConsumer}</h4>
                      <a href={`/consumers/${selectedConsumer}`}>
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
  .select-filter {
    @apply px-3 py-2 pr-8 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500;
    appearance: none;
    background-image: url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e");
    background-position: right 0.5rem center;
    background-repeat: no-repeat;
    background-size: 1.5em 1.5em;
  }

  table {
    border-collapse: separate;
    border-spacing: 0;
  }

  th {
    @apply px-2 py-1 text-2xs font-medium text-gray-500 uppercase tracking-wider;
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
