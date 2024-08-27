<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import { slide } from "svelte/transition";
  import { ChevronDown } from "lucide-svelte";

  type Check = {
    name: string;
    status: "healthy" | "error" | "warning" | "initializing";
    error?: {
      message: string;
    };
  };

  type Health = {
    entity_kind: string;
    entity_id: string;
    status: "healthy" | "error" | "warning" | "initializing";
    checks: Record<string, Check>;
  };

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
      return `All ${totalChecks} health checks are passing`;
    } else if (health.status === "error") {
      return `${count} health check${count > 1 ? "s are" : " is"} failing`;
    } else if (health.status === "warning") {
      return `${count} health check${count > 1 ? "s are" : " is"} warning`;
    } else if (health.status === "initializing") {
      return `${count} health check${count > 1 ? "s are" : " is"} initializing`;
    } else {
      return `${count} health check${count > 1 ? "s are" : " is"} ${health.status}`;
    }
  })();

  $: lastError = Object.values(health.checks).find(
    (check): check is Check => "error" in check
  )?.error;

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
      <h2 class="text-lg font-medium ml-2">{health.entity_kind}</h2>
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
  <p class="mb-2 text-sm">Entity ID: {health.entity_id}</p>
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
      {#each Object.entries(health.checks) as [checkId, check]}
        <div class="py-3 border-b last:border-b-0">
          <div class="flex items-center">
            <HealthIcon status={check.status} />
            <span class="font-medium ml-2 text-sm">{check.name}</span>
          </div>
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
