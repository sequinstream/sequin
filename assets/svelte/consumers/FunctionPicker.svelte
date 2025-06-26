<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Plus, RotateCwIcon, CheckIcon } from "lucide-svelte";
  import {
    Table,
    TableBody,
    TableCell,
    TableRow,
  } from "$lib/components/ui/table";
  import type { RoutedSinkType } from "./types";

  export let functions: Array<{
    id: string;
    name: string;
    type: string;
    description: string;
    sink_type?: string;
  }>;
  export let selectedFunctionId: string = "none";
  export let title: string = "Function";
  export let onFunctionChange: (functionId: string) => void;
  export let refreshFunctions: () => void;
  export let allowedFunctionTypes: string[] = ["function", "path"];
  export let showTypeLabel: boolean = true;
  export let typeLabelKey: string = "type";
  export let refreshState: "idle" | "refreshing" | "done" = "idle";
  export let newFunctionType: string =
    allowedFunctionTypes.length === 1 ? allowedFunctionTypes[0] : undefined;
  export let routedSinkType: RoutedSinkType | null = null;

  $: createNewLink = routedSinkType
    ? `/functions/new?type=${newFunctionType}&sink_type=${routedSinkType}`
    : `/functions/new?type=${newFunctionType}`;
  $: filteredFunctions = functions.filter(
    (t) =>
      allowedFunctionTypes.includes(t.type) &&
      (t.type !== "routing" || t.sink_type === routedSinkType),
  );
  $: hasNoneOption = $$slots["none-option"] !== undefined;
  $: hasValidOptions = filteredFunctions.length > 0 || hasNoneOption;

  function handleFunctionClick(functionId: string) {
    // Only allow selection of "none" if the none-option slot exists
    if (functionId === "none" && !hasNoneOption) return;
    onFunctionChange(functionId);
  }
</script>

<div class="space-y-4">
  {#if hasValidOptions}
    <div class="">
      <div class="flex justify-between mb-4">
        <!-- Show informational slot -->
        <div class="flex items-center">
          <slot></slot>
        </div>

        <!-- Buttons on the right side -->
        <div class="flex gap-2">
          <Button
            variant="outline"
            class="whitespace-nowrap"
            on:click={refreshFunctions}
            disabled={refreshState === "refreshing"}
            aria-label="Refresh functions"
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
            Create new function
          </Button>
        </div>
      </div>

      <!-- Table with options -->
      <div class="border rounded-lg overflow-hidden">
        <div class="max-h-[400px] overflow-y-auto">
          <Table>
            <TableBody>
              <!-- Only show "None" option if the none-option slot exists -->
              {#if hasNoneOption}
                <TableRow
                  on:click={() => handleFunctionClick("none")}
                  class="cursor-pointer {selectedFunctionId === 'none' ||
                  !selectedFunctionId
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

              {#each filteredFunctions as func}
                <TableRow
                  on:click={() => handleFunctionClick(func.id)}
                  class="cursor-pointer {func.id === selectedFunctionId
                    ? 'bg-blue-50 hover:bg-blue-100'
                    : 'hover:bg-gray-100'}"
                >
                  <TableCell>
                    <div class="flex items-center gap-2">
                      <span class="font-medium">{func.name}</span>
                      {#if showTypeLabel}
                        <span
                          class="text-xs bg-gray-200 px-2 py-0.5 rounded-full"
                          >{func[typeLabelKey]}</span
                        >
                      {/if}
                    </div>
                    <div class="text-sm text-muted-foreground">
                      {func.description || "No description provided."}
                    </div>
                  </TableCell>
                </TableRow>
              {/each}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  {:else}
    <!-- Beautiful empty state -->
    <div
      class="border rounded-lg p-8 flex flex-col items-center justify-center text-center space-y-4"
    >
      <h3 class="text-lg font-medium">No {title.toLowerCase()}s</h3>
      <p class="text-muted-foreground max-w-md">
        You don't have any {title.toLowerCase()}s yet. Create one to get
        started.
      </p>
      <div class="flex flex-col sm:flex-row gap-3 pt-2">
        <Button
          variant="outline"
          class="whitespace-nowrap"
          on:click={refreshFunctions}
          disabled={refreshState === "refreshing"}
          aria-label="Refresh functions"
        >
          {#if refreshState === "refreshing"}
            <RotateCwIcon class="h-4 w-4 animate-spin mr-2" />
            Refreshing...
          {:else if refreshState === "done"}
            <CheckIcon class="h-4 w-4 text-green-500 mr-2" />
            Refreshed
          {:else}
            <RotateCwIcon class="h-4 w-4 mr-2" />
            Refresh functions
          {/if}
        </Button>
        <Button
          variant="default"
          class="whitespace-nowrap"
          on:click={() => window.open(createNewLink, "_blank")}
        >
          <Plus class="h-4 w-4 mr-2" />
          Create new function
        </Button>
      </div>
    </div>
  {/if}
</div>
