<script lang="ts">
  import { cn } from "$lib/utils.js";
  type $$Props = { class?: string };

  let className: $$Props["class"] = undefined;
  export { className as class };
</script>

<div class={cn("flex flex-col gap-2", className)} {...$$restProps}>
  <slot />
</div>
