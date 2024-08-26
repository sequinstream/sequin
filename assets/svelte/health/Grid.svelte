<script lang="ts">
  import HealthIcon from "./HealthIcon.svelte";
  import HealthComponent from "./HealthComponent.svelte";
  import HealthPill from "./HealthPill.svelte";

  export let healthy: any;
  export let warning: any;
  export let unhealthy: any;
</script>

<div class="container mx-auto px-4 py-8">
  <h1 class="text-2xl font-semibold mb-6">Health Dashboard</h1>

  <div class="grid grid-cols-3 gap-6">
    <!-- Pill row -->
    <HealthPill status={healthy.status} />
    <HealthPill status={warning.status} />
    <HealthPill status={unhealthy.status} />

    <!-- Component row -->
    <HealthComponent health={healthy} expanded={false} />
    <HealthComponent health={warning} expanded={false} />
    <HealthComponent health={unhealthy} expanded={false} />
  </div>
</div>
