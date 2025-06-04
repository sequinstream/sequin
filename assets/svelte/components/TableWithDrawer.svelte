<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import { ChevronLeft, ChevronRight, Loader2, X } from "lucide-svelte";
  import { slide, fade } from "svelte/transition";

  // Props
  export let title: string;
  export let items: any[];
  export let pageSize: number;
  export let loading: boolean = false;
  export let page: number = 0;
  export let pageCount: number;

  // Events
  export let onPageChange: (page: number) => void;
  export let onRowClick: (item: any) => void;

  // Drawer state
  export let isDrawerOpen: boolean = false;
  export let onCloseDrawer: () => void;

  let rowHeight = 0;
  let totalAvailableHeight = 0;

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
          onPageChange(page);
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
</script>

<div class="flex flex-col flex-1 max-w-8xl">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <!-- Header Div -->
    <div class="flex items-center justify-between mb-4 messages-header">
      <h1 class="text-2xl font-bold">{title}</h1>
      <div class="flex items-center space-x-4">
        <slot name="titleActions" />
      </div>
    </div>

    {#if loading}
      <div
        class="absolute inset-0 bg-white bg-opacity-50 flex items-center justify-center z-10"
      >
        <Loader2 class="h-6 w-6 animate-spin text-gray-500" />
      </div>
    {/if}

    {#if items.length === 0}
      <div
        class="w-full bg-white border border-gray-300 rounded-lg p-6 text-center space-y-2 min-h-96 flex flex-col items-center justify-center"
      >
        <slot name="emptyState" />
      </div>
    {:else}
      <div class="w-full overflow-hidden">
        <table class="w-full bg-white border border-gray-300">
          <thead>
            <tr class="bg-gray-100">
              <slot name="header" />
            </tr>
          </thead>
          <tbody class="divide-y divide-gray-200">
            <tr
              class="sample-row"
              style="visibility: hidden; position: absolute;"
            >
              <slot name="sampleRow" />
            </tr>
            {#each items as item}
              <tr
                class="relative hover:bg-gray-50 cursor-pointer"
                on:click={() => onRowClick(item)}
              >
                <slot name="row" {item} />
              </tr>
            {/each}
          </tbody>
        </table>
      </div>

      <!-- Pagination Div -->
      <div class="flex items-center justify-between mt-4 messages-footer">
        <div class="flex space-x-2">
          <Button
            variant="outline"
            size="sm"
            on:click={() => onPageChange(page - 1)}
            disabled={page === 0}
            class="flex items-center"
          >
            <ChevronLeft class="h-4 w-4 mr-1" />
            <span>Previous</span>
          </Button>
          <Button
            variant="outline"
            size="sm"
            on:click={() => onPageChange(page + 1)}
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
        on:click={onCloseDrawer}
        transition:fade={{ duration: 200 }}
        role="button"
        tabindex="0"
        on:keydown={(e) => e.key === "Enter" && onCloseDrawer()}
      ></div>
      <section class="absolute inset-y-0 right-0 max-w-full flex">
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
                  <slot name="drawerTitle" />
                </h2>
                <div class="ml-3 h-7 flex items-center">
                  <button
                    on:click={onCloseDrawer}
                    class="bg-white rounded-md text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                  >
                    <span class="sr-only">Close panel</span>
                    <X class="h-6 w-6" />
                  </button>
                </div>
              </div>
            </div>
            <div class="mt-6 relative flex-1 px-4 sm:px-6">
              <slot name="drawerContent" />
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

  .text-sm {
    font-size: 0.875rem;
    line-height: 1.25rem;
  }

  .text-gray-900 {
    color: #111827;
  }

  tr + tr {
    margin-top: 0.5rem;
  }
</style>
