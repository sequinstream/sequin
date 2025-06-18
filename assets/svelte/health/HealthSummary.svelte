<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import { MoreHorizontal, RotateCw, Pause, StopCircle } from "lucide-svelte";
  import type { Health, Check } from "./Types";
  import * as Popover from "$lib/components/ui/popover";
  import { Button } from "$lib/components/ui/button";

  export let health: Health;
  export let pushEvent: (
    event: string,
    data: any,
    cb: (val: any) => void,
  ) => void;
  export let status: "active" | "paused" | "disabled" = "active";

  let healthRefreshing = false;

  const statusColor = {
    healthy: "border-green-500",
    error: "border-red-500",
    warning: "border-yellow-500",
    initializing: "border-blue-500",
    paused: "border-gray-400",
    disabled: "border-gray-400",
    notice: "border-blue-400",
  };

  const checkStatusColor = {
    healthy: "text-green-600",
    error: "text-red-600",
    warning: "text-yellow-600",
    initializing: "text-blue-600",
    paused: "text-amber-600",
    disabled: "text-gray-600",
    notice: "text-blue-500",
  };

  function handleRefreshClicked() {
    if (healthRefreshing) return;

    healthRefreshing = true;
    const minLoadingTime = Math.random() * 250 + 250; // Random time between 250-500ms
    const startTime = Date.now();

    pushEvent("refresh_health", {}, () => {
      // Add a debounce, so we don't show the loading state too fast
      const elapsedTime = Date.now() - startTime;
      const remainingTime = Math.max(0, minLoadingTime - elapsedTime);

      setTimeout(() => {
        healthRefreshing = false;
      }, remainingTime);
    });
  }

  $: checkCounts = Object.values(health.checks).reduce(
    (acc, check: Check) => {
      acc[check.status]++;
      return acc;
    },
    { healthy: 0, error: 0, warning: 0, initializing: 0, notice: 0 } as Record<
      Health["status"],
      number
    >,
  );

  $: statusMessage = (() => {
    // Add status checks first
    if (status === "paused") {
      return "Sink is paused - messages are being buffered";
    } else if (status === "disabled") {
      return "Sink is disabled - no messages are being processed";
    }

    // Then continue with existing health status logic
    if (health.status === "paused") return "Paused";

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
    } else if (health.status === "initializing" && checkCounts.healthy > 0) {
      return `${checkCounts.healthy} check${checkCounts.healthy > 1 ? "s" : ""} healthy, ${count} waiting`;
    } else if (health.status === "initializing" && checkCounts.healthy === 0) {
      return `${count} health check${count > 1 ? "s are" : " is"} waiting`;
    } else {
      return `${count} health check${count > 1 ? "s are" : " is"} ${health.status}`;
    }
  })();

  $: displayStatus =
    status === "paused"
      ? "paused"
      : status === "disabled"
        ? "disabled"
        : health.status;
</script>

<div
  class={`inline-block p-4 rounded-lg border-2 ${statusColor[health.status]} relative`}
>
  <div class="flex items-center justify-between mb-2">
    <div class="flex items-center">
      {#if status === "paused"}
        <Pause class="h-4 w-4 text-amber-600 mr-2" />
      {:else if status === "disabled"}
        <StopCircle class="h-4 w-4 text-gray-600 mr-2" />
      {:else}
        <HealthIcon status={health.status} />
      {/if}
      <h2 class="text-lg font-medium ml-2">Health</h2>
    </div>
    <div class="flex items-center gap-2">
      <Button
        disabled={healthRefreshing}
        variant="outline"
        size="icon"
        on:click={handleRefreshClicked}
      >
        <RotateCw class="h-4 w-4" />
      </Button>
      <Popover.Root>
        <Popover.Trigger asChild let:builder>
          <Button builders={[builder]} variant="outline" size="icon">
            <MoreHorizontal class="h-4 w-4" />
          </Button>
        </Popover.Trigger>
        <Popover.Content class="w-64">
          <div class="space-y-2">
            {#each Object.entries(health.checks) as [checkId, check]}
              <div class="py-2 border-b last:border-b-0">
                <div class="flex items-center">
                  <HealthIcon status={check.status} />
                  <span class="font-medium ml-2 text-sm">{check.name}</span>
                </div>
                {#if check.error}
                  <p
                    class="text-xs text-red-600 mt-2 ml-1 bg-gray-100 font-mono p-2 rounded"
                  >
                    Error: {check.error.message}
                  </p>
                {/if}
                {#if check.status === "stale"}
                  <p
                    class="text-xs text-yellow-600 mt-2 ml-1 bg-gray-100 font-mono p-2 rounded"
                  >
                    No update received from this check in the expected time
                    interval. This is usually a bug - contact the Sequin team.
                  </p>
                {/if}
              </div>
            {/each}
          </div>
        </Popover.Content>
      </Popover.Root>
    </div>
  </div>
  <p class="text-xs {checkStatusColor[displayStatus]}">
    {statusMessage}
  </p>
</div>
