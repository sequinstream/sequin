<script lang="ts">
  import type { status } from "./Types";
  import { Check, AlertTriangle, X, Loader, Pause } from "lucide-svelte";

  export let status: status;

  const iconMap = {
    healthy: Check,
    unhealthy: X,
    error: X,
    warning: AlertTriangle,
    initializing: Loader,
    paused: Pause,
    notice: AlertTriangle,
  };

  const iconColor = {
    healthy: "text-green-600",
    unhealthy: "text-red-600",
    error: "text-red-600",
    warning: "text-yellow-600",
    initializing: "text-blue-600",
    paused: "text-gray-500",
    notice: "text-blue-500",
  };

  $: Icon = iconMap[status] || AlertTriangle;
  $: color = iconColor[status] || "text-gray-600";
</script>

{#if Icon}
  <svelte:component
    this={Icon}
    class={`w-4 h-4 mr-1 ${color} ${status === "initializing" ? "animate-spin" : ""}`}
  />
{/if}
