<script lang="ts">
  import { Label } from "$lib/components/ui/label";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Checkbox } from "$lib/components/ui/checkbox";
  import { Button } from "$lib/components/ui/button";
  import { X } from "lucide-svelte";

  export let selectedTable: {
    columns: Array<{
      attnum: number;
      name: string;
      isPk?: boolean;
      type: string;
    }>;
  } | null = null;

  export let excludeColumnAttnums: number[] = [];
  export let includeColumnAttnums: number[] = [];

  $: availableColumns = selectedTable?.columns || [];
  $: primaryKeyColumns = availableColumns.filter((col) => col.isPk);

  let columnSelectionMode: "all" | "exclude" | "include" = "all";
  let modeInitialized = false;

  // Initialize mode from arrays - only run once
  $: if (!modeInitialized) {
    // Filter out primary keys from exclude list if needed
    if (selectedTable && excludeColumnAttnums.length > 0) {
      const pkAttnums = selectedTable.columns
        .filter((col) => col.isPk)
        .map((col) => col.attnum);
      const filteredExclude = excludeColumnAttnums.filter(
        (attnum) => !pkAttnums.includes(attnum),
      );
      if (filteredExclude.length !== excludeColumnAttnums.length) {
        excludeColumnAttnums = filteredExclude;
      }
    }

    // Set initial mode
    if (excludeColumnAttnums.length > 0) {
      columnSelectionMode = "exclude";
    } else if (includeColumnAttnums.length > 0) {
      columnSelectionMode = "include";
    } else {
      columnSelectionMode = "all";
    }
    modeInitialized = true;
  }

  // Track previous mode to detect user changes
  let previousMode: "all" | "exclude" | "include" | null = null;

  // Watch mode changes and update arrays when user selects a different mode
  $: if (previousMode !== null && columnSelectionMode !== previousMode) {
    if (columnSelectionMode === "all") {
      excludeColumnAttnums = [];
      includeColumnAttnums = [];
    } else if (columnSelectionMode === "exclude") {
      includeColumnAttnums = [];
      // Filter out any primary key columns
      const pkAttnums = primaryKeyColumns.map((col) => col.attnum);
      excludeColumnAttnums = excludeColumnAttnums.filter(
        (attnum) => !pkAttnums.includes(attnum),
      );
    } else if (columnSelectionMode === "include") {
      excludeColumnAttnums = [];
    }
    previousMode = columnSelectionMode;
  }

  // Initialize previousMode after first render
  $: if (previousMode === null) {
    previousMode = columnSelectionMode;
  }

  function toggleColumn(attnum: number) {
    // Prevent excluding primary key columns
    if (columnSelectionMode === "exclude") {
      const column = availableColumns.find((col) => col.attnum === attnum);
      if (column?.isPk) {
        return; // Don't allow excluding PK columns
      }

      if (excludeColumnAttnums.includes(attnum)) {
        excludeColumnAttnums = excludeColumnAttnums.filter((a) => a !== attnum);
      } else {
        excludeColumnAttnums = [...excludeColumnAttnums, attnum];
      }
    } else if (columnSelectionMode === "include") {
      if (includeColumnAttnums.includes(attnum)) {
        includeColumnAttnums = includeColumnAttnums.filter((a) => a !== attnum);
      } else {
        includeColumnAttnums = [...includeColumnAttnums, attnum];
      }
    }
  }

  function isColumnSelected(attnum: number): boolean {
    if (columnSelectionMode === "exclude") {
      return excludeColumnAttnums.includes(attnum);
    } else if (columnSelectionMode === "include") {
      return includeColumnAttnums.includes(attnum);
    }
    return false;
  }

  $: selectedColumns = availableColumns.filter((col) =>
    isColumnSelected(col.attnum),
  );

  $: hasExcludedPrimaryKeys =
    columnSelectionMode === "exclude" &&
    primaryKeyColumns.some((col) => excludeColumnAttnums.includes(col.attnum));
</script>

<div class="flex flex-col gap-4">
  <Label class="text-base font-medium">Column selection</Label>

  <div class="flex flex-col gap-4">
    <RadioGroup bind:value={columnSelectionMode}>
      <div class="flex flex-col gap-2">
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="all" id="column-selection-all" />
          <Label for="column-selection-all" class="cursor-pointer font-normal">
            Sync all columns
          </Label>
        </div>
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="exclude" id="column-selection-exclude" />
          <Label for="column-selection-exclude" class="cursor-pointer font-normal">
            Exclude specific columns
          </Label>
        </div>
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="include" id="column-selection-include" />
          <Label for="column-selection-include" class="cursor-pointer font-normal">
            Include only specific columns
          </Label>
        </div>
      </div>
    </RadioGroup>

    {#if columnSelectionMode !== "all" && availableColumns.length > 0}
      <div class="space-y-2">
        <Label class="text-sm text-muted-foreground">
          {#if columnSelectionMode === "exclude"}
            Select columns to exclude from sync:
          {:else}
            Select columns to include in sync:
          {/if}
        </Label>

        <div
          class="border rounded-lg p-4 max-h-[300px] overflow-y-auto space-y-2"
        >
          {#if hasExcludedPrimaryKeys}
            <div
              class="mb-2 p-3 rounded-md bg-yellow-50 border border-yellow-200"
            >
              <p class="text-sm text-yellow-800">
                <strong>Warning:</strong> Primary key columns cannot be excluded
                from sync. They are required for change tracking.
              </p>
            </div>
          {/if}
          {#each availableColumns as column}
            <div class="flex items-center space-x-2">
              <Checkbox
                id="column-{column.attnum}"
                checked={isColumnSelected(column.attnum)}
                disabled={columnSelectionMode === "exclude" && column.isPk}
                onCheckedChange={() => toggleColumn(column.attnum)}
              />
              <Label
                for="column-{column.attnum}"
                class="cursor-pointer flex-1 flex items-center gap-2 {columnSelectionMode ===
                'exclude' && column.isPk
                  ? 'opacity-50 cursor-not-allowed'
                  : ''}"
              >
                <span>{column.name}</span>
                {#if column.isPk}
                  <span
                    class="text-xs px-1.5 py-0.5 rounded bg-muted text-muted-foreground"
                  >
                    PK
                  </span>
                {/if}
                <span class="text-xs text-muted-foreground">
                  ({column.type})
                </span>
                {#if columnSelectionMode === "exclude" && column.isPk}
                  <span class="text-xs text-yellow-600">(required)</span>
                {/if}
              </Label>
            </div>
          {/each}
        </div>

        {#if selectedColumns.length > 0}
          <div class="flex flex-wrap gap-2 pt-2">
            {#each selectedColumns as column}
              <div
                class="flex items-center gap-1 px-2 py-1 rounded-md bg-muted text-sm"
              >
                <span>{column.name}</span>
                <button
                  type="button"
                  on:click={() => toggleColumn(column.attnum)}
                  class="hover:bg-muted-foreground/20 rounded p-0.5"
                >
                  <X class="h-3 w-3" />
                </button>
              </div>
            {/each}
          </div>
        {/if}
      </div>
    {/if}
  </div>
</div>
