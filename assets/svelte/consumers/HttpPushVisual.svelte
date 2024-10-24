<script>
  import { onMount } from "svelte";
  import { tweened } from "svelte/motion";
  import { cubicOut } from "svelte/easing";

  let dotX = tweened(0, { duration: 1500, easing: cubicOut });
  let dotOpacity = tweened(1);
  let checkmarkProgress = tweened(0);
  let checkmarkOpacity = tweened(0);

  let animationPhase = 0;

  onMount(() => {
    animatePush();
  });

  async function animatePush() {
    while (true) {
      // Move dot to the end
      dotOpacity.set(1);
      await dotX.set(160);

      // Pause at the end
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Show checkmark
      await checkmarkProgress.set(1);
      await checkmarkOpacity.set(1);

      // Pause with checkmark visible
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Fade out dot and checkmark
      await Promise.all([dotOpacity.set(0), checkmarkOpacity.set(0)]);

      // Random delay before next cycle
      await new Promise((resolve) =>
        setTimeout(resolve, Math.random() * 2000 + 1000),
      );

      // Reset for next cycle
      dotX.set(0, { duration: 0 });
      checkmarkProgress.set(0, { duration: 0 });
    }
  }
</script>

<div class="w-full h-48 mb-4 relative bg-blue-50 rounded-lg">
  <svg viewBox="0 0 200 100" class="w-full h-full">
    <text x="10" y="30" class="text-xs fill-blue-400">POST</text>
    <path
      d="M20,50 L180,50"
      stroke="#E0E7FF"
      stroke-width="8"
      stroke-linecap="round"
    />
    <g transform="translate({$dotX}, 0)">
      <circle cx="20" cy="50" r="8" fill="#818CF8" opacity={$dotOpacity} />
      <path
        d="M17 50 L20 53 L23 47"
        stroke="white"
        stroke-width="2"
        stroke-linecap="round"
        stroke-linejoin="round"
        fill="none"
        opacity={$checkmarkOpacity}
        stroke-dasharray="1"
        stroke-dashoffset={1 - $checkmarkProgress}
      />
    </g>
    <text x="10" y="80" class="text-[10px] fill-blue-400">Sequin</text>
    <text x="150" y="80" class="text-[10px] fill-blue-400">Your app</text>
  </svg>
</div>

<style>
  /* Add any component-specific styles here */
</style>
