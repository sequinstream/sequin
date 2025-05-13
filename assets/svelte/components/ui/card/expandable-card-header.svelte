<script lang="ts">
  import { CardHeader, CardTitle } from ".";
  import { ChevronDown } from "lucide-svelte";
  import { Button as ButtonPrimitive } from "bits-ui";

  export let expanded: boolean = false;
  export let disabled: boolean = false;
  export let builders: $$Props["builders"] = [];

  function handleClick() {
    if (disabled) return;
    expanded = !expanded;
  }
</script>

<CardHeader class="flex flex-row items-center justify-between">
  <ButtonPrimitive.Root
    class="flex flex-row items-center justify-between w-full cursor-pointer"
    on:click={handleClick}
    type="button"
    {builders}
    {disabled}
  >
    <CardTitle class="flex items-center gap-2">
      <slot />
    </CardTitle>
    <div
      class="transition-transform duration-200"
      class:rotate-180={expanded}
      class:text-gray-300={disabled}
    >
      <ChevronDown class="h-4 w-4" />
    </div>
  </ButtonPrimitive.Root>
</CardHeader>
