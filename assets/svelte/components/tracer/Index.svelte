<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    Pause,
    Play,
    ChevronLeft,
    ChevronRight,
    X,
    ArrowUpRight,
    Activity,
  } from "lucide-svelte";
  import { slide, fade } from "svelte/transition";

  export let live: any;
  export let trace_state: any;
  export let consumers: any;
  export let databases: any;
  export let tables: any;
  export let paused: boolean = false;

  let selectedDatabase = "";
  let selectedConsumer = "";
  let selectedTable = "";
  let selectedState = "";

  const spanTypeMapping = {
    received: { frontend: "Delivered", backend: "received" },
    filtered: { frontend: "Excluded by filters", backend: "filtered" },
    replicated: { frontend: "Replicated", backend: "replicated" },
    ingested: { frontend: "Ingested", backend: "ingested" },
    acked: { frontend: "Acked", backend: "acked" },
  };

  const states = Object.entries(spanTypeMapping).map(([key, value]) => ({
    frontend: value.frontend,
    backend: value.backend,
  }));

  function getFrontendSpanType(backendType: string): string {
    return spanTypeMapping[backendType.toLowerCase()]?.frontend || backendType;
  }

  function getBackendSpanType(frontendType: string): string {
    return (
      Object.values(spanTypeMapping).find((v) => v.frontend === frontendType)
        ?.backend || frontendType
    );
  }

  const getStateColor = (state: string) => {
    switch (state.toLowerCase()) {
      case "replicated":
        return "bg-purple-100 text-purple-800";
      case "excluded by filters":
        return "bg-gray-200 text-gray-800";
      case "ingested":
        return "bg-blue-100 text-blue-800";
      case "delivered":
        return "bg-blue-100 text-blue-800";
      case "acked":
        return "bg-green-100 text-green-800";
      default:
        return "bg-gray-100 text-gray-800";
    }
  };

  const getVerticalLineColor = (spanTypes: string[]) => {
    const mappedTypes = spanTypes.map((type) =>
      getFrontendSpanType(type).toLowerCase()
    );
    if (mappedTypes.includes("acked")) return "bg-green-500";
    if (mappedTypes.includes("excluded by filters")) return "bg-gray-400";
    if (mappedTypes.filter((type) => type === "delivered").length > 1)
      return "bg-red-500";
    if (
      mappedTypes.some((type) =>
        ["replicated", "delivered", "ingested"].includes(type)
      )
    )
      return "bg-blue-500";
    return "bg-transparent";
  };

  function getSpanColor(spanType: string) {
    switch (getFrontendSpanType(spanType).toLowerCase()) {
      case "replicated":
        return "bg-purple-500";
      case "excluded by filters":
        return "bg-gray-400";
      case "ingested":
        return "bg-blue-500";
      case "delivered":
        return "bg-blue-500";
      case "acked":
        return "bg-green-500";
      default:
        return "bg-gray-400";
    }
  }

  const getErrorMessage = (spanTypes: string[]) => {
    const deliveredCount = spanTypes.filter(
      (type) => getFrontendSpanType(type).toLowerCase() === "delivered"
    ).length;
    const isAcked = spanTypes.some(
      (type) => getFrontendSpanType(type).toLowerCase() === "acked"
    );
    if (deliveredCount > 1) {
      return {
        message: `Redelivered x${deliveredCount}`,
        color: isAcked ? "text-gray-500" : "text-red-500",
      };
    }
    return { message: "", color: "" };
  };

  let currentPage = 1;
  let itemsPerPage = 50;

  $: totalPages = Math.ceil((trace_state?.total_count || 0) / itemsPerPage);

  function nextPage() {
    if (currentPage < totalPages) {
      currentPage++;
      updatePage();
    }
  }

  function prevPage() {
    if (currentPage > 1) {
      currentPage--;
      updatePage();
    }
  }

  function updatePage() {
    const searchParams = new URLSearchParams(window.location.search);
    searchParams.set("page", currentPage.toString());
    const newUrl = `?${searchParams.toString()}`;
    window.history.pushState(null, "", newUrl);
    window.dispatchEvent(new Event("popstate"));
  }

  function updateFilters() {
    const searchParams = new URLSearchParams(window.location.search);

    if (selectedDatabase) {
      searchParams.set("database", selectedDatabase);
    } else {
      searchParams.delete("database");
    }

    if (selectedConsumer) {
      searchParams.set("consumer", selectedConsumer);
    } else {
      searchParams.delete("consumer");
    }

    if (selectedTable) {
      searchParams.set("table", selectedTable);
    } else {
      searchParams.delete("table");
    }

    if (selectedState) {
      searchParams.set("state", selectedState);
    } else {
      searchParams.delete("state");
    }

    if (paused) {
      searchParams.set("paused", "true");
    } else {
      searchParams.delete("paused");
    }

    const newUrl = searchParams.toString()
      ? `?${searchParams.toString()}`
      : window.location.pathname;
    window.history.pushState(null, "", newUrl);
    window.dispatchEvent(new Event("popstate"));
  }

  function togglePause() {
    paused = !paused;
    if (paused) {
      live.pushEvent("pause_updates", {});
    } else {
      live.pushEvent("resume_updates", {});
    }
    updateFilters();
  }

  onMount(() => {
    const searchParams = new URLSearchParams(window.location.search);
    selectedDatabase = searchParams.get("database") || "";
    selectedConsumer = searchParams.get("consumer") || "";
    selectedTable = searchParams.get("table") || "";
    selectedState = searchParams.get("state") || "";
    paused = searchParams.get("paused") === "true";
    currentPage = parseInt(searchParams.get("page") || "1");
  });

  let selectedTrace = null;
  let isDrawerOpen = false;

  $: {
    if (trace_state && selectedTrace) {
      const updatedTrace = trace_state.message_traces.find(
        (trace) =>
          trace.trace_id === selectedTrace.trace_id &&
          trace.consumer_id === selectedTrace.consumer_id
      );
      if (updatedTrace) {
        selectedTrace = updatedTrace;
      }
    }
  }

  function openDrawer(trace) {
    selectedTrace = trace;
    isDrawerOpen = true;
  }

  function closeDrawer() {
    isDrawerOpen = false;
  }
</script>

<div class="container mx-auto p-4">
  <div class="flex justify-between items-center mb-6">
    <h1 class="text-3xl font-bold flex items-center gap-2">
      <Activity class="text-gray-800 animate-pulse" />
      Trace
    </h1>
    <div class="flex items-center gap-4">
      <Button variant="outline" on:click={togglePause}>
        {#if paused}
          <Play class="h-4 w-4" />
        {:else}
          <Pause class="h-4 w-4" />
        {/if}
      </Button>
    </div>
  </div>

  <div class="mb-6 flex flex-wrap gap-4">
    <select
      bind:value={selectedDatabase}
      on:change={updateFilters}
      class="select-filter text-sm"
    >
      <option value="">All Databases</option>
      {#each databases as database}
        <option value={database.id}>{database.name}</option>
      {/each}
    </select>

    <select
      bind:value={selectedConsumer}
      on:change={updateFilters}
      class="select-filter text-sm"
    >
      <option value="">All Consumers</option>
      {#each consumers as consumer}
        <option value={consumer.id}>{consumer.name}</option>
      {/each}
    </select>

    <select
      bind:value={selectedTable}
      on:change={updateFilters}
      class="select-filter text-sm"
    >
      <option value="">All Tables</option>
      {#each tables as table}
        <option value={table.name}>{table.name}</option>
      {/each}
    </select>

    <select
      bind:value={selectedState}
      on:change={updateFilters}
      class="select-filter text-sm"
    >
      <option value="">All States</option>
      {#each states as state}
        <option value={state.backend}>{state.frontend}</option>
      {/each}
    </select>
  </div>

  {#if trace_state && trace_state.message_traces.length > 0}
    <div class="overflow-x-auto max-w-full">
      <table class="w-full bg-white border border-gray-300">
        <thead>
          <tr class="bg-gray-100">
            <th class="w-1"></th>
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >Date</th
            >
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >Consumer</th
            >
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >Database</th
            >
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >Table</th
            >
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
              >Primary Keys</th
            >
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[100px]"
              >State</th
            >
            <th
              class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[120px]"
              >Errors</th
            >
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-200">
          {#each trace_state.message_traces as trace}
            <tr
              class="relative hover:bg-gray-50 cursor-pointer"
              on:click={() => openDrawer(trace)}
            >
              <td class="w-1 p-0">
                <div
                  class={`w-1 h-full absolute left-0 top-0 ${getVerticalLineColor(trace.span_types)}`}
                ></div>
              </td>
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{new Date(trace.date).toLocaleString()}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{trace.consumer.name}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{trace.database.name}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{trace.table}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                >{trace.primary_keys}</td
              >
              <td class="px-2 py-1 whitespace-nowrap text-2xs min-w-[100px]">
                <span
                  class={`px-1 py-0.5 inline-flex text-2xs leading-3 font-semibold rounded-full ${getStateColor(getFrontendSpanType(trace.state))}`}
                >
                  {getFrontendSpanType(trace.state)}
                </span>
              </td>
              <td
                class="px-2 py-1 whitespace-nowrap text-2xs font-semibold min-w-[120px]"
                class:text-red-500={getErrorMessage(trace.span_types).color ===
                  "text-red-500"}
                class:text-gray-500={getErrorMessage(trace.span_types).color ===
                  "text-gray-500"}
              >
                {getErrorMessage(trace.span_types).message}
              </td>
            </tr>
          {/each}
        </tbody>
      </table>
    </div>

    <!-- Pagination controls -->
    <div class="mt-4 flex items-center justify-between">
      <div>
        Showing {(currentPage - 1) * itemsPerPage + 1} to {Math.min(
          currentPage * itemsPerPage,
          trace_state?.total_count || 0
        )} of {trace_state?.total_count || 0} entries
      </div>
      <div class="flex items-center space-x-2">
        <Button
          variant="outline"
          on:click={prevPage}
          disabled={currentPage === 1}
        >
          <ChevronLeft class="h-4 w-4" />
          Previous
        </Button>
        <span class="text-sm">Page {currentPage} of {totalPages}</span>
        <Button
          variant="outline"
          on:click={nextPage}
          disabled={currentPage === totalPages}
        >
          Next
          <ChevronRight class="h-4 w-4" />
        </Button>
      </div>
    </div>
  {:else}
    <div class="w-full flex items-center justify-center min-h-[400px]">
      <div class="w-full max-w-4xl bg-white p-8 rounded-lg text-center">
        {#if databases.length === 0}
          <h2 class="text-2xl font-bold mb-4">No Databases Found</h2>
          <p class="text-gray-600 mb-4">
            You need to create a database before tracing can occur.
          </p>
          <div class="mb-6 video-container">
            <iframe
              src="https://www.youtube.com/embed/pi5g3Tzagxk"
              title="YouTube video player"
              frameborder="0"
              allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
              allowfullscreen
            ></iframe>
          </div>
          <a href="/databases" class="inline-block">
            <Button variant="default">
              Create Database
              <ArrowUpRight class="h-4 w-4 ml-2" />
            </Button>
          </a>
        {:else}
          <h2 class="text-2xl font-bold mb-4">Trace started</h2>
          <p class="text-gray-600">
            Messages from your database will appear here shortly
          </p>
        {/if}
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
          class="w-screen max-w-md"
          transition:slide={{ duration: 300, axis: "x" }}
        >
          <div
            class="h-full flex flex-col py-6 bg-white shadow-xl overflow-y-scroll"
          >
            <div class="px-4 sm:px-6">
              <div class="flex items-start justify-between">
                <h2 class="text-lg font-medium text-gray-900">Trace Details</h2>
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
              {#if selectedTrace}
                <div class="space-y-8">
                  <!-- Trace Details Section -->
                  <div class="bg-gray-50 p-4 rounded-lg space-y-2">
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >Trace ID:</span
                      >
                      <span class="text-sm text-gray-900"
                        >{selectedTrace.trace_id}</span
                      >
                    </div>
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >Primary Keys:</span
                      >
                      <span class="text-sm text-gray-900"
                        >{selectedTrace.primary_keys}</span
                      >
                    </div>
                    <div class="flex justify-between items-center">
                      <span class="text-sm font-medium text-gray-500"
                        >State:</span
                      >
                      <span
                        class={`text-sm px-2 py-1 rounded-full ${getStateColor(getFrontendSpanType(selectedTrace.state))}`}
                      >
                        {getFrontendSpanType(selectedTrace.state)}
                      </span>
                    </div>
                  </div>

                  <!-- Spans Section -->
                  <div>
                    <h3 class="text-lg font-semibold mb-4">Spans</h3>
                    <div class="overflow-x-auto">
                      <table class="min-w-full bg-white border border-gray-300">
                        <thead>
                          <tr class="bg-gray-100">
                            <th class="w-1"></th>
                            <th
                              class="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                              >Event</th
                            >
                            <th
                              class="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                              >Timestamp</th
                            >
                            <th
                              class="px-2 py-1 text-left text-xs font-medium text-gray-500 uppercase tracking-wider"
                              >Duration</th
                            >
                          </tr>
                        </thead>
                        <tbody class="divide-y divide-gray-200">
                          {#each selectedTrace.spans as span}
                            <tr class="relative hover:bg-gray-50">
                              <td class="w-1 p-0">
                                <div
                                  class={`w-1 h-full absolute left-0 top-0 ${getSpanColor(span.type)}`}
                                ></div>
                              </td>
                              <td
                                class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                                >{getFrontendSpanType(span.type)}</td
                              >
                              <td
                                class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                              >
                                {new Date(span.timestamp).toLocaleString()}
                              </td>
                              <td
                                class="px-2 py-1 whitespace-nowrap text-2xs text-gray-500"
                              >
                                {span.duration ? `${span.duration}ms` : "-"}
                              </td>
                            </tr>
                          {/each}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  <!-- Consumer Section -->
                  <div>
                    <h3 class="text-lg font-semibold mb-4">Consumer</h3>
                    <div
                      class="bg-gray-100 p-4 rounded-lg flex justify-between items-center"
                    >
                      <h4 class="font-medium">{selectedTrace.consumer.name}</h4>
                      <a href={`/consumers/${selectedTrace.consumer.id}`}>
                        <Button variant="outline" size="sm">
                          View Consumer
                          <ArrowUpRight class="h-4 w-4 ml-2" />
                        </Button>
                      </a>
                    </div>
                  </div>

                  <!-- Database Section -->
                  <div>
                    <h3 class="text-lg font-semibold mb-4">Database</h3>
                    <div
                      class="bg-gray-100 p-4 rounded-lg flex flex-col space-y-2"
                    >
                      <div class="flex justify-between items-center">
                        <h4 class="font-medium">
                          {selectedTrace.database.name}
                        </h4>
                        <a href={`/databases/${selectedTrace.database.id}`}>
                          <Button variant="outline" size="sm">
                            View Database
                            <ArrowUpRight class="h-4 w-4 ml-2" />
                          </Button>
                        </a>
                      </div>
                      <div class="text-sm text-gray-600">
                        Table: {selectedTrace.table}
                      </div>
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
  /* Add any additional styles to make it look more like Datadog's Trace page */
  table {
    border-collapse: separate;
    border-spacing: 0;
  }

  th,
  td {
    border-bottom: 1px solid #e2e8f0;
  }

  th {
    @apply px-2 py-1 text-2xs font-medium text-gray-500 uppercase tracking-wider;
  }

  tr:hover {
    background-color: #f8fafc;
  }

  select {
    @apply px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500;
  }

  .select-filter {
    @apply px-3 py-2 pr-8 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500;
    appearance: none;
    background-image: url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e");
    background-position: right 0.5rem center;
    background-repeat: no-repeat;
    background-size: 1.5em 1.5em;
  }

  .text-2xs {
    font-size: 0.65rem;
    line-height: 1rem;
  }

  tr {
    position: relative;
  }

  td:first-child {
    padding-left: 4px;
  }

  /* Add styles for the drawer if needed */
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

  /* If you need to adjust the table width in the drawer */
  .min-w-full {
    min-width: 100%;
  }

  .video-container {
    position: relative;
    padding-bottom: 56.25%; /* 16:9 aspect ratio */
    height: 0;
    overflow: hidden;
    max-width: 100%;
  }

  .video-container iframe {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
  }
</style>
