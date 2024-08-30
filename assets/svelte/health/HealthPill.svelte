<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import type { status } from "./Types";
  import { Badge } from "$lib/components/ui/badge";
  export let status: status;

  $: displayStatus = status === "initializing" ? "waiting" : status;
</script>

<!-- <div
  class={`inline-flex items-center px-2 py-1 rounded-full ${pillStyle[status]} mx-auto`}
>
  <span class="font-medium capitalize text-sm">{displayStatus}</span>
  <HealthIcon {status} />
</div> -->

<Badge variant={status}>
  <HealthIcon {status} />
  <span class="capitalize">{displayStatus}</span>
</Badge>
