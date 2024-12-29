<script lang="ts">
  import { cn } from "$lib/utils.js";
  type $$Props = { class?: string };

  let className: $$Props["class"] = undefined;
  export { className as class };
</script>

<p class={cn("text-destructive text-sm", className)} {...$$restProps}>
  <slot />
</p>
