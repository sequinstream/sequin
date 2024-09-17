<script lang="ts">
  import { Button as ButtonPrimitive } from "bits-ui";
  import { type Events, type Props, buttonVariants } from "./index.ts";
  import { cn } from "$lib/utils.ts";
  import { LoaderCircle } from "lucide-svelte";
  import { onDestroy } from "svelte";

  type $$Props = Props;
  type $$Events = Events;

  export let variant: $$Props["variant"] = "default";
  export let size: $$Props["size"] = "default";
  export let builders: $$Props["builders"] = [];
  export let loading: $$Props["loading"] = false;
  export let minLoadingTime: number = 500; // Minimum loading time in milliseconds
  export let className: $$Props["class"] = undefined;
  export { className as class };

  let isLoading = false;
  let loadingStartTime: number | null = null;
  let timeoutHandle: ReturnType<typeof setTimeout> | null = null;

  $: if (loading) {
    if (!isLoading) {
      isLoading = true;
      loadingStartTime = Date.now();
    }
  } else if (isLoading) {
    const elapsed = Date.now() - (loadingStartTime || 0);
    const remaining = minLoadingTime - elapsed;

    if (remaining > 0) {
      timeoutHandle = setTimeout(() => {
        isLoading = false;
        loadingStartTime = null;
        timeoutHandle = null;
      }, remaining);
    } else {
      isLoading = false;
      loadingStartTime = null;
    }
  }

  onDestroy(() => {
    if (timeoutHandle) {
      clearTimeout(timeoutHandle);
    }
  });
</script>

<ButtonPrimitive.Root
  {builders}
  class={cn(buttonVariants({ variant, size, className }))}
  type="button"
  {...$$restProps}
  disabled={isLoading || $$restProps.disabled}
  on:click
  on:keydown
>
  {#if isLoading}
    <LoaderCircle class="w-4 h-4 mr-2 animate-spin" />
    <span>
      <slot name="loading">Loading...</slot>
    </span>
  {:else}
    <slot />
  {/if}
</ButtonPrimitive.Root>
