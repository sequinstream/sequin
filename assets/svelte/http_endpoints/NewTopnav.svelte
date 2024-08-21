<script lang="ts">
  import * as AlertDialog from "$lib/components/ui/alert-dialog";
  import { Button } from "$lib/components/ui/button";
</script>

<div
  class="flex flex-shrink-0 h-16 z-10 justify-center w-full sticky top-0 bg-surface-base border-b border-carbon-100"
>
  <div class="flex items-center justify-between w-full px-8">
    <h3 class="font-heading font-medium text-lg text-base">New consumer</h3>
    <div class="flex items-center justify-center flex-grow mx-8">
      <div class="flex gap-2 items-center">
        <div
          class="flex-shrink-0 flex items-center justify-center border border-matcha-600 bg-matcha-600 w-3.5 h-3.5 text-sm rounded-full"
        ></div>
        <div>
          <span class="text-md leading-5 text-matcha-600 font-semibold"
            >Select model</span
          >
        </div>
      </div>
      <div class="flex h-px bg-carbon-100 flex-grow mx-4 max-w-[48px]"></div>
      <div class="flex gap-2 items-center">
        <div
          class="flex-shrink-0 flex items-center justify-center border border-carbon-300 w-3.5 h-3.5 text-sm rounded-full"
        ></div>
        <div>
          <span class="text-md leading-5 text-carbon-600"
            >Select destination</span
          >
        </div>
      </div>
      <div class="flex h-px bg-carbon-100 flex-grow mx-4 max-w-[48px]"></div>
      <div class="flex gap-2 items-center">
        <div
          class="flex-shrink-0 flex items-center justify-center border border-carbon-300 w-3.5 h-3.5 text-sm rounded-full"
        ></div>
        <div>
          <span class="text-md leading-5 text-carbon-600">Configure sync</span>
        </div>
      </div>
      <div class="flex h-px bg-carbon-100 flex-grow mx-4 max-w-[48px]"></div>
      <div class="flex gap-2 items-center">
        <div
          class="flex-shrink-0 flex items-center justify-center border border-carbon-300 w-3.5 h-3.5 text-sm rounded-full"
        ></div>
        <div>
          <span class="text-md leading-5 text-carbon-600">Finalize sync</span>
        </div>
      </div>
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
            >
              Exit
            </a>
          </AlertDialog.Action>
        </AlertDialog.Footer>
      </AlertDialog.Content>
    </AlertDialog.Root>
  </div>
</div>
