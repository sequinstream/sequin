<script>
  import { crossfade } from "svelte/transition";
  import { cubicOut } from "svelte/easing";

  export let content;
  let isSupported = navigator.clipboard && navigator.clipboard.writeText;
  let showCheck = false;

  const [send, receive] = crossfade({
    duration: 100,
    easing: cubicOut,
  });

  const handleCopy = () => {
    navigator.clipboard.writeText(content).then(() => {
      showCheck = true;
      setTimeout(() => {
        showCheck = false;
      }, 1500);
    });
  };
</script>

{#if isSupported}
  <button
    on:click|preventDefault={handleCopy}
    class="relative flex items-center justify-center w-6 h-6 {$$props.class ||
      ''}"
  >
    {#if showCheck}
      <div
        class="absolute"
        in:send={{ key: "check" }}
        out:receive={{ key: "check" }}
      >
        <icon
          class="hero-check-circle cursor-pointer text-gray-500 icon-transition"
        />
      </div>
    {:else}
      <div
        class="absolute"
        in:send={{ key: "clip" }}
        out:receive={{ key: "clip" }}
      >
        <icon
          class="hero-clipboard-document cursor-pointer text-gray-500 icon-transition"
        />
      </div>
    {/if}
  </button>
{/if}

<style>
  .icon-transition {
    transition: transform 0.3s ease-in-out;
  }
</style>
