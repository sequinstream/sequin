<script>
  import { crossfade } from "svelte/transition";
  import { cubicOut } from "svelte/easing";

  export let content;
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

<button
  on:click={handleCopy}
  class="relative flex items-center justify-center w-8 h-8"
>
  {#if showCheck}
    <div class="absolute" in:send out:receive>
      <icon
        class="hero-check-circle cursor-pointer text-gray-500 icon-transition"
      />
    </div>
  {:else}
    <div class="absolute" in:send out:receive>
      <icon
        class="hero-clipboard-document cursor-pointer text-gray-500 icon-transition"
      />
    </div>
  {/if}
</button>

<style>
  .icon-transition {
    transition: transform 0.3s ease-in-out;
  }
</style>
