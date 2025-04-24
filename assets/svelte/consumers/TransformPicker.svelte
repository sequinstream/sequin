<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Plus, RotateCwIcon, CheckIcon } from "lucide-svelte";
  import {
    Table,
    TableBody,
    TableCell,
    TableRow,
  } from "$lib/components/ui/table";

  export let transforms: Array<{
    id: string;
    name: string;
    type: string;
    description: string;
    sink_type?: string;
  }>;
  export let selectedTransformId: string = "none";
  export let title: string = "Transform";
  export let onTransformChange: (transformId: string) => void;
  export let refreshTransforms: () => void;
  export let transformTypes: string[] = ["function", "path"];
  export let showTypeLabel: boolean = true;
  export let typeLabelKey: string = "type";
  export let refreshState: "idle" | "refreshing" | "done" = "idle";
  export let createNewQueryParams: string = "";

  $: createNewLink = `/transforms/new${createNewQueryParams}`;
  $: filteredTransforms = transforms.filter((t) =>
    transformTypes.includes(t.type),
  );
  $: hasNoneOption = $$slots["none-option"] !== undefined;
  $: hasValidOptions = filteredTransforms.length > 0 || hasNoneOption;

  function handleTransformClick(transformId: string) {
    // Only allow selection of "none" if the none-option slot exists
    if (transformId === "none" && !hasNoneOption) return;
    onTransformChange(transformId);
  }
</script>

<div class="space-y-4">
  <div class="">
    <div class="flex justify-between mb-4">
      <!-- Show either the informational slot or the error message -->
      <div class="flex items-center">
        {#if hasValidOptions}
          <slot></slot>
        {:else}
          <div class="text-red-600">
            No valid {title.toLowerCase()}s available. Please create a {title.toLowerCase()}
            first.
          </div>
        {/if}
      </div>

      <!-- Buttons on the right side -->
      <div class="flex gap-2">
        <Button
          variant="outline"
          class="whitespace-nowrap"
          on:click={refreshTransforms}
          disabled={refreshState === "refreshing"}
          aria-label="Refresh {title}s"
        >
          {#if refreshState === "refreshing"}
            <RotateCwIcon class="h-4 w-4 animate-spin" />
          {:else if refreshState === "done"}
            <CheckIcon class="h-4 w-4 text-green-500" />
          {:else}
            <RotateCwIcon class="h-4 w-4" />
          {/if}
        </Button>
        <Button
          variant="outline"
          class="whitespace-nowrap"
          on:click={() => window.open(createNewLink, "_blank")}
        >
          <Plus class="h-4 w-4 mr-2" />
          Create new {title.toLowerCase()}
        </Button>
      </div>
    </div>

    <!-- Only show the table if we have valid options -->
    {#if hasValidOptions}
      <div class="border rounded-lg overflow-hidden">
        <div class="max-h-[400px] overflow-y-auto">
          <Table>
            <TableBody>
              <!-- Only show "None" option if the none-option slot exists -->
              {#if hasNoneOption}
                <TableRow
                  on:click={() => handleTransformClick("none")}
                  class="cursor-pointer {selectedTransformId === 'none' ||
                  !selectedTransformId
                    ? 'bg-blue-50 hover:bg-blue-100'
                    : 'hover:bg-gray-100'}"
                >
                  <TableCell>
                    <div class="font-medium">None</div>
                    <div class="text-sm text-muted-foreground">
                      <slot name="none-option"></slot>
                    </div>
                  </TableCell>
                </TableRow>
              {/if}

              {#each filteredTransforms as transform}
                <TableRow
                  on:click={() => handleTransformClick(transform.id)}
                  class="cursor-pointer {transform.id === selectedTransformId
                    ? 'bg-blue-50 hover:bg-blue-100'
                    : 'hover:bg-gray-100'}"
                >
                  <TableCell>
                    <div class="flex items-center gap-2">
                      <span class="font-medium">{transform.name}</span>
                      {#if showTypeLabel}
                        <span
                          class="text-xs bg-gray-200 px-2 py-0.5 rounded-full"
                          >{transform[typeLabelKey]}</span
                        >
                      {/if}
                    </div>
                    <div class="text-sm text-muted-foreground">
                      {transform.description || "No description provided."}
                    </div>
                  </TableCell>
                </TableRow>
              {/each}
            </TableBody>
          </Table>
        </div>
      </div>
    {/if}
  </div>
</div>
