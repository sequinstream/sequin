<!-- https://github.com/woutdp/live_svelte?tab=readme-ov-file#liveview-live-navigation-events -->
<script>
  export let href;
</script>

<a
  {href}
  {...$$restProps}
  on:click={() => window.scrollTo({ top: 0, behavior: "smooth" })}
  data-phx-link="patch"
  data-phx-link-state="push"
>
  {#if $$slots.default}
    <slot />
  {/if}
</a>
