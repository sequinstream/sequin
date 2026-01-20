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

  // New state for keyboard navigation
  let keyboardNavigatingPageDir: "previous" | "next" | null = null;
  let selectedItemIndex: number = -1;
  let selectedItem: any = null;

  let rowHeight = 0;
  let totalAvailableHeight = 0;

  // Handle keyboard navigation
  function handleKeyDown(event: KeyboardEvent) {
    if (!isDrawerOpen) {
      // When drawer is closed, arrow keys select first/last message
      switch (event.key) {
        case "ArrowUp":
          event.preventDefault();
          if (items.length > 0) {
            handleRowClick(items[items.length - 1], items.length - 1);
          }
          break;
        case "ArrowDown":
          event.preventDefault();
          if (items.length > 0) {
            handleRowClick(items[0], 0);
          }
          break;
      }
      return;
    }

    // When drawer is open, use existing navigation logic
    switch (event.key) {
      case "ArrowUp":
        event.preventDefault();
        navigateToPrevious();
        break;
      case "ArrowDown":
        event.preventDefault();
        navigateToNext();
        break;
      case "Escape":
        event.preventDefault();
        onCloseDrawer();
        break;
    }
  }

  function navigateToPrevious() {
    if (selectedItemIndex > 0) {
      selectedItemIndex--;
      selectedItem = items[selectedItemIndex];
      onRowClick(selectedItem);
    } else if (selectedItemIndex === 0 && page > 0) {
      // Go to previous page and select last item
      onPageChange(page - 1);
      keyboardNavigatingPageDir = "previous";
      // selectedItemIndex will be set when new items load
    }
  }

  function navigateToNext() {
    if (selectedItemIndex < items.length - 1) {
      selectedItemIndex++;
      selectedItem = items[selectedItemIndex];
      onRowClick(selectedItem);
    } else if (selectedItemIndex === items.length - 1 && page < pageCount - 1) {
      // Go to next page and select first item
      onPageChange(page + 1);
      keyboardNavigatingPageDir = "next";
      // selectedItemIndex will be set when new items load
    }
  }

  // Handle row click and update selected index
  function handleRowClick(item: any, index: number) {
    selectedItemIndex = index;
    selectedItem = item;
    onRowClick(item);
  }

  // Reset selected index when items change (e.g., page change)
  $: if (items && isDrawerOpen) {
    // Try to find the index of the currently selected item
    const matchingIndex = items.findIndex(
      (item) => JSON.stringify(item) === JSON.stringify(selectedItem),
    );
    if (matchingIndex >= 0) {
      selectedItemIndex = matchingIndex;
    } else if (keyboardNavigatingPageDir === "previous") {
      keyboardNavigatingPageDir = null;
      selectedItemIndex = items.length - 1;
    } else if (keyboardNavigatingPageDir === "next") {
      keyboardNavigatingPageDir = null;
      selectedItemIndex = 0;
    } else {
      selectedItemIndex = -1;
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

    // Add keyboard event listener
    window.addEventListener("keydown", handleKeyDown);

    return () => {
      window.removeEventListener("resize", updatePageSize);
      window.removeEventListener("keydown", handleKeyDown);
    };
  });
</script>

<div class="flex flex-col flex-1 max-w-8xl">
  <!-- Content container with overflow handling -->
  <div class="container mx-auto px-4 py-8 flex-1 overflow-y-auto">
    <!-- Header Div -->
    <div class="flex items-center justify-between mb-4 messages-header gap-4">
      <h1 class="text-2xl font-bold">{title}</h1>
      <div class="flex items-center space-x-4 flex-wrap">
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
            {#each items as item, index}
              <tr
                class="relative hover:bg-gray-50 cursor-pointer {isDrawerOpen &&
                selectedItemIndex === index
                  ? 'bg-blue-50 border-l-4 border-blue-500'
                  : ''}"
                on:click={() => handleRowClick(item, index)}
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
            <!-- Keyboard shortcuts hint -->
            <div class="px-4 sm:px-6 py-3 border-t border-gray-100">
              <div
                class="text-xs text-gray-400 flex items-center justify-center space-x-4"
              >
                <span class="flex items-center space-x-1">
                  <kbd
                    class="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-xs font-mono"
                    >↑</kbd
                  >
                  <kbd
                    class="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-xs font-mono"
                    >↓</kbd
                  >
                  <span>navigate</span>
                </span>
                <span class="flex items-center space-x-1">
                  <kbd
                    class="px-1.5 py-0.5 bg-gray-100 text-gray-600 rounded text-xs font-mono"
                    >Esc</kbd
                  >
                  <span>close</span>
                </span>
              </div>
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
