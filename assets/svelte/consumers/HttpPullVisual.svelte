<script>
  import { onMount } from "svelte";
  import { tweened } from "svelte/motion";
  import { cubicOut } from "svelte/easing";

  let dotX = tweened(0, { duration: 1500, easing: cubicOut });
  let dotScale = tweened(0.5);
  let dotOpacity = tweened(1);
  let checkmarkProgress = tweened(0);
  let checkmarkOpacity = tweened(0);

  onMount(() => {
    animatePull();
  });

  async function animatePull() {
    while (true) {
      // Move dot to "server" side
      dotOpacity.set(1);
      await dotX.set(-160);

      // Pause at "server" side
      await new Promise((resolve) => setTimeout(resolve, 300));

      // Grow dot
      await dotScale.set(1);

      // Pause after growing
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Return to "client" side
      await dotX.set(0);

      // Show checkmark
      await checkmarkProgress.set(1);
      await checkmarkOpacity.set(1);

      // Pause with checkmark visible
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Fade out dot and checkmark
      await Promise.all([dotOpacity.set(0), checkmarkOpacity.set(0)]);

      // Random delay before next cycle
      await new Promise((resolve) =>
        setTimeout(resolve, Math.random() * 2000 + 1000),
      );

      // Reset for next cycle
      dotScale.set(0.5, { duration: 0 });
      checkmarkProgress.set(0, { duration: 0 });
    }
  }
</script>

<div class="w-full h-48 mb-4 relative bg-green-50 rounded-lg">
  <svg viewBox="0 0 200 100" class="w-full h-full">
    <text x="170" y="30" class="text-xs fill-green-400">GET</text>
    <path
      d="M20,50 L180,50"
      stroke="#D1FAE5"
      stroke-width="8"
      stroke-linecap="round"
    />
    <circle
      cx={180 + $dotX}
      cy="50"
      r={8 * $dotScale}
      fill="#34D399"
      opacity={$dotOpacity}
    />
    <path
      d={`M${176 + $dotX} 50 L${180 + $dotX} 54 L${184 + $dotX} 46`}
      stroke="white"
      stroke-width="2"
      stroke-linecap="round"
      stroke-linejoin="round"
      fill="none"
      opacity={$checkmarkOpacity}
      stroke-dasharray="1"
      stroke-dashoffset={1 - $checkmarkProgress}
    />
    <text x="10" y="80" class="text-[10px] fill-green-400">Sequin</text>
    <text x="150" y="80" class="text-[10px] fill-green-400">Your app</text>
  </svg>
</div>

<style>
  /* Add any component-specific styles here */
</style>
