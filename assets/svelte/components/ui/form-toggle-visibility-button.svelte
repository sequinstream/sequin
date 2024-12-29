<script lang="ts">
  import { cn } from "$lib/utils.js";
  import { EyeIcon, EyeOffIcon } from "lucide-svelte";
  type $$Props = {
    class?: string;
    isVisible: boolean;
    label: string;
    onToggleVisibility: () => void;
  };

  let className: $$Props["class"] = undefined;
  export { className as class, isVisible, label, onToggleVisibility };
</script>

<div class={cn("flex flex-col gap-2", className)} {...$$restProps}>
  <slot />
</div>

<button
  type="button"
  class={cn("absolute inset-y-0 right-0 flex items-center pr-3", className)}
  {...$$restProps}
  on:click={onToggleVisibility}
  aria-label={isVisible ? `Hide ${label}` : `Show ${label}`}
>
  {#if isVisible}
    <EyeOffIcon class="h-4 w-4 text-gray-400" />
  {:else}
    <EyeIcon class="h-4 w-4 text-gray-400" />
  {/if}
</button>
