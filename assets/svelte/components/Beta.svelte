<script lang="ts">
  export let size: "sm" | "md" | "lg" = "md";
  export let variant: "default" | "subtle" = "default";
</script>

<span
  class="beta-tag {size} {variant}"
  title="This feature is currently in beta"
>
  BETA
</span>

<style>
  .beta-tag {
    display: inline-flex;
    align-items: center;
    font-weight: 600;
    letter-spacing: 0.02em;
    border-radius: 4px;
    padding: 2px 6px;
    font-size: 0.75rem;
    line-height: 1;
    text-transform: uppercase;
    background: linear-gradient(135deg, #6366f1 0%, #4f46e5 100%);
    color: white;
  }

  .beta-tag.sm {
    font-size: 0.65rem;
    padding: 1px 4px;
  }

  .beta-tag.lg {
    font-size: 0.85rem;
    padding: 3px 8px;
  }

  .beta-tag.subtle {
    background: linear-gradient(135deg, #e0e7ff 0%, #c7d2fe 100%);
    color: #4f46e5;
  }
</style>
