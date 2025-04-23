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
  export let transformTypes: string[] = ["function", "path"]; // Default for standard transforms
  export let showTypeLabel: boolean = true;
  export let typeLabelKey: string = "type"; // Use "type" or "sink_type" as the label
  
  // State tracking for refresh button
  export let refreshState: "idle" | "refreshing" | "done" = "idle";

  // Filter transforms based on the specified types
  $: filteredTransforms = transforms.filter(t => transformTypes.includes(t.type));

  function handleTransformClick(transformId: string) {
    onTransformChange(transformId);
  }
</script>

<div class="space-y-4">
  <div class="">
    <div class="flex justify-end gap-2 mb-4">
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
        on:click={() => window.open("/transforms/new", "_blank")}
      >
        <Plus class="h-4 w-4 mr-2" />
        Create new {title.toLowerCase()}
      </Button>
    </div>

    <div class="border rounded-lg overflow-hidden">
      <div class="max-h-[400px] overflow-y-auto">
        <Table>
          <TableBody>
            <TableRow
              on:click={() => handleTransformClick("none")}
              class="cursor-pointer {selectedTransformId === 'none' || !selectedTransformId
                ? 'bg-blue-50 hover:bg-blue-100'
                : 'hover:bg-gray-100'}"
            >
              <TableCell>
                <div class="font-medium">None</div>
                <div class="text-sm text-muted-foreground">
                  No {title.toLowerCase()} applied. {#if title.toLowerCase() === 'transform'}Messages will be sent as-is.{:else}All messages will be sent to the default destination.{/if}
                </div>
              </TableCell>
            </TableRow>
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
                      >{transform[typeLabelKey]}</span>
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
  </div>
</div>
