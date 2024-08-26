<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";

  export let status: "healthy" | "warning" | "unhealthy";

  const pillStyle = {
    healthy: "bg-green-100 text-green-800",
    warning: "bg-yellow-100 text-yellow-800",
    unhealthy: "bg-red-100 text-red-800",
  };
</script>

<div
  class={`inline-flex items-center px-2 py-1 rounded-full ${pillStyle[status]} mx-auto`}
>
  <span class="font-medium capitalize text-sm">{status}</span>
  <HealthIcon {status} />
</div>
