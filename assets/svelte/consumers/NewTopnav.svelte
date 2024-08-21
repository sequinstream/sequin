<script lang="js">
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { Button } from "$lib/components/ui/button";

  export let currentStep;

  $: steps = [
    {
      id: "selectStream",
      label: "Select stream",
      active:
        currentStep === "select_stream" || currentStep === "select_consumer",
    },
    {
      id: "selectTable",
      label: "Select table",
      active:
        currentStep === "select_table" || currentStep === "configure_filters",
    },
    {
      id: "configure",
      label: "Configure consumer",
      active: currentStep === "configure_consumer",
    },
    {
      id: "finalize",
      label: "Finalize consumer",
      active: currentStep === "confirmation",
    },
  ];
</script>

<div
  class="flex flex-shrink-0 h-16 z-10 justify-center w-full sticky top-0 bg-surface-base border-b border-carbon-100"
>
  <div class="flex items-center justify-between w-full px-8">
    <h3 class="font-heading font-medium text-lg text-base">New consumer</h3>
    <div class="flex items-center justify-center flex-grow mx-8">
      {#each steps as step, index}
        {#if index > 0}
          <div class="flex h-px bg-carbon-100 flex-grow mx-4 max-w-[48px]" />
        {/if}
        <div class="flex gap-2 items-center">
          <div
            class="flex-shrink-0 flex items-center justify-center w-4 h-4 rounded-full transition-all duration-300 ease-in-out
            {step.active
              ? 'bg-matcha-600 border-2 border-matcha-200 bg-slate-500'
              : 'border border-carbon-300'}"
          />
          <div class="hidden sm:block">
            <span
              class="text-md leading-5 font-semibold
              {step.active ? 'text-matcha-600' : 'text-carbon-600'}"
            >
              {step.label}
            </span>
          </div>
        </div>
      {/each}
    </div>
    <AlertDialog.Root>
      <AlertDialog.Trigger asChild let:builder>
        <Button builders={[builder]} variant="outline">Exit</Button>
      </AlertDialog.Trigger>
      <AlertDialog.Content>
        <AlertDialog.Header>
          <AlertDialog.Title>Are you sure you want to exit?</AlertDialog.Title>
          <AlertDialog.Description>
            Your progress will be lost if you exit now. This action cannot be
            undone.
          </AlertDialog.Description>
        </AlertDialog.Header>
        <AlertDialog.Footer>
          <AlertDialog.Cancel>Cancel</AlertDialog.Cancel>
          <AlertDialog.Action>
            <a
              href="/consumers"
              data-phx-link="redirect"
              data-phx-link-state="push"
              on:click|stopPropagation
            >
              Exit
            </a>
          </AlertDialog.Action>
        </AlertDialog.Footer>
      </AlertDialog.Content>
    </AlertDialog.Root>
  </div>
</div>
