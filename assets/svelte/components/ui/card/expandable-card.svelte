<script lang="ts">
  import { Card, CardContent, CardHeader, CardTitle } from ".";
  import { ChevronDown } from "lucide-svelte";

  export let expanded: boolean = false;
  export let disabled: boolean = false;

  function handleClick() {
    if (disabled) return;
    expanded = !expanded;
  }
</script>

<Card>
  <CardHeader class="flex flex-row items-center justify-between">
    <button
      type="button"
      class="flex flex-row items-center justify-between w-full"
      on:click={handleClick}
      {disabled}
    >
      <CardTitle class="flex items-center gap-2">
        <slot name="title" />
      </CardTitle>
      <div
        class="transition-transform duration-200"
        class:rotate-180={expanded}
        class:text-gray-300={disabled}
      >
        <ChevronDown class="h-4 w-4" />
      </div>
    </button>
  </CardHeader>

  <CardContent>
    <div class="space-y-2">
      {#if !expanded}
        <slot name="summary" />
      {:else}
        <slot name="content" />
      {/if}
    </div>
  </CardContent>
</Card>
