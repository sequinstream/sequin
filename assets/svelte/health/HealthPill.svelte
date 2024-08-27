<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";

  export let status: "healthy" | "warning" | "error" | "initializing";

  const pillStyle = {
    healthy: "bg-green-100 text-green-800",
    warning: "bg-yellow-100 text-yellow-800",
    error: "bg-red-100 text-red-800",
    initializing: "bg-blue-100 text-blue-800",
  };

  $: displayStatus = status === "initializing" ? "waiting" : status;
</script>

<div
  class={`inline-flex items-center px-2 py-1 rounded-full ${pillStyle[status]} mx-auto`}
>
  <span class="font-medium capitalize text-sm">{displayStatus}</span>
  <HealthIcon {status} />
</div>
