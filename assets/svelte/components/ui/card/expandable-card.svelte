<script lang="ts">
  import { Card, CardContent } from ".";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Info } from "lucide-svelte";
  import ExpandableCardHeader from "./expandable-card-header.svelte";

  export let expanded: boolean = false;
  export let disabled: boolean = false;
  export let titleTooltip: string | null = null;
</script>

<Card>
  {#if titleTooltip}
    <Tooltip.Root
      openDelay={10}
      class="flex flex-row items-center justify-between"
    >
      <Tooltip.Trigger asChild let:builder>
        <ExpandableCardHeader builders={[builder]} bind:expanded {disabled}>
          <slot name="title" />
        </ExpandableCardHeader>
      </Tooltip.Trigger>
      <Tooltip.Content class="max-w-xs space-y-2">
        <p class="text-xs text-gray-500">
          {titleTooltip}
        </p>
      </Tooltip.Content>
    </Tooltip.Root>
  {:else}
    <ExpandableCardHeader bind:expanded {disabled}>
      <slot name="title" />
    </ExpandableCardHeader>
  {/if}

  <CardContent>
    <div class="space-y-2">
      {#if disabled || !expanded}
        <slot name="summary" />
      {:else}
        <slot name="content" />
      {/if}
    </div>
  </CardContent>
</Card>
