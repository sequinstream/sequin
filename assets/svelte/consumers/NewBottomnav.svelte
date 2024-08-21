<script lang="ts">
  import { Button } from "$lib/components/ui/button";

  export let currentStep: string;
  export let onBack: () => void;
  export let onContinue: () => void;
</script>

<div
  class="flex flex-shrink-0 h-16 z-10 justify-center w-full sticky bottom-0 bg-surface-base border-t border-carbon-100"
>
  <div class="flex items-center justify-between w-full px-8 max-w-[1288px]">
    <div>
      {#if currentStep !== "select_stream"}
        <Button variant="outline" on:click={onBack}>
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            class="w-4 h-4 mr-2"
          >
            <path
              fill-rule="evenodd"
              d="M12.768 6.36a1 1 0 0 1-.128 1.408L7.562 12l5.078 4.232a1 1 0 1 1-1.28 1.536l-6-5a1 1 0 0 1 0-1.536l6-5a1 1 0 0 1 1.408.128Z"
              clip-rule="evenodd"
            />
            <path
              fill-rule="evenodd"
              d="M5 12a1 1 0 0 1 1-1h14a1 1 0 1 1 0 2H6a1 1 0 0 1-1-1Z"
              clip-rule="evenodd"
            />
          </svg>
          Go back
        </Button>
      {/if}
    </div>
    <div>
      {#if currentStep === "select_stream"}
        <p class="text-sm leading-4 text-carbon-200 font-semibold">
          Select a stream type to continue
        </p>
      {:else if currentStep === "select_consumer"}
        <p class="text-sm leading-4 text-carbon-200 font-semibold">
          Select a consumer type to continue
        </p>
      {:else if currentStep === "confirmation"}
        <Button on:click={onContinue}>Create Consumer</Button>
      {:else}
        <Button on:click={onContinue}>
          Continue
          <icon class="w-4 h-4 ml-2 hero-arrow-right" aria-hidden="true" />
        </Button>
      {/if}
    </div>
  </div>
</div>
