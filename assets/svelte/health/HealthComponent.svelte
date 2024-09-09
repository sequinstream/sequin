<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import { slide } from "svelte/transition";
  import { ChevronDown } from "lucide-svelte";
  import { onMount } from "svelte";
  import type { Health, Check } from "./Types";

  export let health: Health;
  export let expanded = false;

  const toggleExpanded = () => (expanded = !expanded);

  const statusColor = {
    healthy: "border-green-500",
    error: "border-red-500",
    warning: "border-yellow-500",
    initializing: "border-blue-500",
  };

  const checkStatusColor = {
    healthy: "text-green-600",
    error: "text-red-600",
    warning: "text-yellow-600",
    initializing: "text-blue-600",
  };

  $: checkCounts = Object.values(health.checks).reduce(
    (acc, check: Check) => {
      acc[check.status]++;
      return acc;
    },
    { healthy: 0, error: 0, warning: 0, initializing: 0 } as Record<
      Health["status"],
      number
    >
  );

  $: statusMessage = (() => {
    const count = checkCounts[health.status];
    if (count === 0) return "";

    const totalChecks = Object.keys(health.checks).length;

    if (health.status === "healthy" && count === totalChecks) {
      return totalChecks === 1
        ? "The health check is passing"
        : `All ${totalChecks} health checks are passing`;
    } else if (health.status === "error") {
      return `${count} health check${count > 1 ? "s are" : " is"} failing`;
    } else if (health.status === "warning") {
      return `${count} health check${count > 1 ? "s are" : " is"} warning`;
    } else if (health.status === "initializing") {
      const healthyCount = checkCounts.healthy;
      return `${healthyCount} of ${totalChecks} check${totalChecks > 1 ? "s" : ""} healthy, ${count} waiting`;
    } else {
      return `${count} health check${count > 1 ? "s are" : " is"} ${health.status}`;
    }
  })();

  $: lastError = Object.values(health.checks).find(
    (check): check is Check => "error" in check
  )?.error;

  $: hasChecks = Object.keys(health.checks).length > 0;

  let containerElement: HTMLElement;
  let expandedContentElement: HTMLElement;

  function updateExpandedContentPosition() {
    if (containerElement && expandedContentElement) {
      const computedStyle = window.getComputedStyle(containerElement);
      const borderLeftWidth = parseFloat(computedStyle.borderLeftWidth);
      const borderRightWidth = parseFloat(computedStyle.borderRightWidth);

      expandedContentElement.style.position = "absolute";
      expandedContentElement.style.top = "100%";
      expandedContentElement.style.left = `-${borderLeftWidth}px`;
      expandedContentElement.style.width = `calc(100% + ${borderLeftWidth + borderRightWidth}px)`;
      expandedContentElement.style.marginTop = "-2px"; // Adjust this value if needed
    }
  }

  function handleScroll() {
    if (expanded && hasChecks) {
      updateExpandedContentPosition();
    }
  }

  // Add this function to handle clickaway
  function handleClickOutside(event: MouseEvent) {
    if (
      expanded &&
      containerElement &&
      !containerElement.contains(event.target as Node)
    ) {
      expanded = false;
    }
  }

  onMount(() => {
    document.addEventListener("click", handleClickOutside);
    window.addEventListener("scroll", handleScroll);
    window.addEventListener("resize", updateExpandedContentPosition);

    return () => {
      document.removeEventListener("click", handleClickOutside);
      window.removeEventListener("scroll", handleScroll);
      window.removeEventListener("resize", updateExpandedContentPosition);
    };
  });

  $: if (expanded && hasChecks) {
    // Use setTimeout to ensure the DOM has updated
    setTimeout(updateExpandedContentPosition, 0);
  }

  // Update position on scroll and resize
  if (typeof window !== "undefined") {
    window.addEventListener("scroll", handleScroll);
    window.addEventListener("resize", updateExpandedContentPosition);
  }

  $: initializingProgress =
    health.status === "initializing"
      ? (checkCounts.healthy / Object.keys(health.checks).length) * 100
      : 0;
</script>

<div
  bind:this={containerElement}
  class={`inline-block p-4 rounded-lg border-2 ${statusColor[health.status]} relative ${
    expanded && hasChecks ? "rounded-b-none border-b-0" : ""
  }`}
>
  <div class="flex items-center justify-between mb-2">
    <div class="flex items-center">
      <HealthIcon status={health.status} />
      <h2 class="text-lg font-medium ml-2">{health.name}</h2>
    </div>
    {#if hasChecks}
      <button
        on:click={toggleExpanded}
        class="text-gray-500 hover:text-gray-700 transition-colors duration-200"
        aria-label={expanded ? "Collapse" : "Expand"}
      >
        <div
          class="transform transition-transform duration-200"
          class:rotate-180={expanded}
        >
          <ChevronDown size={20} />
        </div>
      </button>
    {/if}
  </div>
  <p class="text-xs {checkStatusColor[health.status]}">{statusMessage}</p>

  {#if health.status === "initializing"}
    <div class="mt-2 h-1 bg-gray-200 rounded-full overflow-hidden">
      <div
        class="h-full bg-blue-500 transition-all duration-300 ease-in-out"
        style="width: {initializingProgress}%"
      ></div>
    </div>
  {/if}

  {#if expanded && hasChecks}
    <div
      bind:this={expandedContentElement}
      transition:slide={{ duration: 300 }}
      class="absolute space-y-2 bg-white border-2 border-t-0 rounded-b-lg shadow-lg p-4"
      style="border-color: inherit; z-index: 10;"
    >
      {#each Object.entries(health.checks) as [checkId, check]}
        <div class="py-3 border-b last:border-b-0">
          <div class="flex items-center">
            <HealthIcon status={check.status} />
            <span class="font-medium ml-2 text-sm">{check.name}</span>
          </div>
          {#if check.message}
            <p class="text-xs text-gray-400 mt-1 ml-7">{check.message}</p>
          {/if}
          {#if check.error}
            <p
              class="text-xs text-red-600 mt-2 ml-1 bg-gray-100 font-mono p-2 rounded"
            >
              Error: {check.error.message}
            </p>
          {/if}
        </div>
      {/each}
    </div>
  {/if}
</div>
