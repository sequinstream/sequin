<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import { MoreHorizontal } from "lucide-svelte";
  import type { Health, Check } from "./Types";
  import * as Popover from "$lib/components/ui/popover";
  import { Button } from "$lib/components/ui/button";

  export let health: Health;

  const statusColor = {
    healthy: "border-green-500",
    error: "border-red-500",
    warning: "border-yellow-500",
    initializing: "border-blue-500",
    paused: "border-gray-400",
  };

  const checkStatusColor = {
    healthy: "text-green-600",
    error: "text-red-600",
    warning: "text-yellow-600",
    initializing: "text-blue-600",
    paused: "text-gray-500",
  };

  $: checkCounts = Object.values(health.checks).reduce(
    (acc, check: Check) => {
      acc[check.status]++;
      return acc;
    },
    { healthy: 0, error: 0, warning: 0, initializing: 0 } as Record<
      Health["status"],
      number
    >,
  );

  $: statusMessage = (() => {
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
    } else if (health.status === "initializing") {
      const healthyCount = checkCounts.healthy;
      return `${healthyCount} of ${totalChecks} check${totalChecks > 1 ? "s" : ""} healthy, ${count} waiting`;
    } else {
      return `${count} health check${count > 1 ? "s are" : " is"} ${health.status}`;
    }
  })();
</script>

<div
  class={`inline-block p-4 rounded-lg border-2 ${statusColor[health.status]} relative`}
>
  <div class="flex items-center justify-between mb-2">
    <div class="flex items-center">
      <HealthIcon status={health.status} />
      <h2 class="text-lg font-medium ml-2">Health</h2>
    </div>
    <Popover.Root>
      <Popover.Trigger asChild let:builder>
        <Button builders={[builder]} variant="ghost" size="icon">
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
  <p class="text-xs {checkStatusColor[health.status]}">
    {statusMessage}
  </p>
</div>
