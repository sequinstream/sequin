<script lang="ts">
  import type { status } from "./Types";
  import { Check, AlertTriangle, X, Loader } from "lucide-svelte";

  export let status: status;

  const iconMap = {
    healthy: Check,
    error: X,
    warning: AlertTriangle,
    initializing: Loader,
  };

  const iconColor = {
    healthy: "text-green-600",
    error: "text-red-600",
    warning: "text-yellow-600",
    initializing: "text-blue-600",
  };

  $: Icon = iconMap[status] || AlertTriangle;
  $: color = iconColor[status] || "text-gray-600";
</script>

{#if Icon}
  <svelte:component
    this={Icon}
    class={`w-4 h-4 ml-1 ${color} ${status === "initializing" ? "animate-spin" : ""}`}
  />
{/if}
