<script lang="ts">
  import { Button as ButtonPrimitive } from "bits-ui";
  import { type Events, type Props, buttonVariants } from "./index.ts";
  import { cn } from "$lib/utils.ts";
  import { LoaderCircle } from "lucide-svelte";

  type $$Props = Props;
  type $$Events = Events;

  let className: $$Props["class"] = undefined;
  export let variant: $$Props["variant"] = "default";
  export let size: $$Props["size"] = "default";
  export let builders: $$Props["builders"] = [];
  export let loading: $$Props["loading"] = false;
  export { className as class };
</script>

<ButtonPrimitive.Root
  {builders}
  class={cn(buttonVariants({ variant, size, className }))}
  type="button"
  {...$$restProps}
  disabled={loading || $$restProps.disabled}
  on:click
  on:keydown
>
  {#if loading}
    <LoaderCircle class="w-4 h-4 mr-2 animate-spin" />
  {/if}
  <slot />
</ButtonPrimitive.Root>
