<!-- https://github.com/woutdp/live_svelte?tab=readme-ov-file#liveview-live-navigation-events -->
<script>
  export let href;
</script>

<a {href} {...$$restProps} data-phx-link="redirect" data-phx-link-state="push">
  {#if $$slots.default}
    <slot />
  {/if}
</a>
