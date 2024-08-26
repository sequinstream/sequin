<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import { slide } from "svelte/transition";
  import { ChevronDown } from "lucide-svelte";

  export let health: {
    status: "healthy" | "warning" | "unhealthy";
    message: string;
    checks: Array<{
      name: string;
      status: "healthy" | "warning" | "unhealthy";
      message: string;
      error?: {
        code: string;
        message: string;
        service: string;
        details?: string;
      };
    }>;
    component: string;
  };
  export let expanded = false;

  const toggleExpanded = () => (expanded = !expanded);

  const statusColor = {
    healthy: "border-green-500",
    warning: "border-yellow-500",
    unhealthy: "border-red-500",
  };

  const checkStatusColor = {
    healthy: "text-green-600",
    warning: "text-yellow-600",
    unhealthy: "text-red-600",
  };

  $: checkCounts = health.checks.reduce(
    (acc, check) => {
      acc[check.status]++;
      return acc;
    },
    { healthy: 0, warning: 0, unhealthy: 0 }
  );

  // Update statusMessage to use more natural language
  $: statusMessage = (() => {
    const count = checkCounts[health.status];
    if (count === 0) return "";

    const totalChecks = health.checks.length;

    if (health.status === "healthy" && count === totalChecks) {
      return `All ${totalChecks} health checks are passing`;
    } else if (health.status === "warning") {
      return `${count} health check${count > 1 ? "s have" : " has"} a warning`;
    } else if (health.status === "unhealthy") {
      return `${count} health check${count > 1 ? "s are" : " is"} failing`;
    } else {
      return `${count} health check${count > 1 ? "s are" : " is"} ${health.status}`;
    }
  })();

  $: lastError = health.checks.find((check) => check.error)?.error;

  // Add this line to track the container's dimensions
  let containerElement: HTMLElement;
  let containerRect: DOMRect;

  $: if (containerElement) {
    containerRect = containerElement.getBoundingClientRect();
  }
</script>

<div
  bind:this={containerElement}
  class={`inline-block p-4 rounded-lg border-2 ${statusColor[health.status]} relative ${
    expanded ? "rounded-b-none border-b-0" : ""
  }`}
>
  <div class="flex items-center justify-between mb-2">
    <div class="flex items-center">
      <HealthIcon status={health.status} />
      <h2 class="text-lg font-medium ml-2">{health.component}</h2>
    </div>
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
  </div>
  <p class="mb-2 text-sm">{health.message}</p>
  <p class="text-xs {checkStatusColor[health.status]}">{statusMessage}</p>

  {#if expanded}
    <div
      transition:slide={{ duration: 300 }}
      class="space-y-2 absolute bg-white z-10 border-2 border-t-0 rounded-b-lg shadow-lg p-4"
      style="top: {containerRect ? containerRect.height - 2 + 'px' : 'auto'}; 
             left: -2px; 
             right: -2px; 
             width: calc(100% + 4px); 
             border-color: inherit;"
    >
      {#each health.checks as check}
        <div class="py-3 border-b last:border-b-0">
          <div class="flex items-center">
            <HealthIcon status={check.status} />
            <span class="font-medium ml-2 text-sm">{check.name}</span>
          </div>
          <p class="text-xs text-gray-600 mt-2 ml-1">{check.message}</p>
          {#if check.error}
            <p class="text-xs text-red-600 mt-2 ml-1">
              Error: {check.error.message}
            </p>
          {/if}
        </div>
      {/each}
    </div>
  {/if}
</div>
