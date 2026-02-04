<script lang="ts">
  import { onMount } from "svelte";
  import { Label } from "$lib/components/ui/label";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Checkbox } from "$lib/components/ui/checkbox";
  import { Button } from "$lib/components/ui/button";
  import { X } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import * as Alert from "$lib/components/ui/alert";

  export let selectedTable: {
    columns: Array<{
      attnum: number;
      name: string;
      "isPk?"?: boolean;
      isPk?: boolean;
      type: string;
    }>;
  } | null = null;

  export let excludeColumnAttnums: number[] = [];
  export let includeColumnAttnums: number[] = [];
  export let isEdit: boolean = false;

  $: availableColumns = selectedTable?.columns || [];
  $: primaryKeyColumns = availableColumns.filter(
    (col) => col.isPk || col["isPk?"],
  );

  let columnSelectionMode: "all" | "exclude" | "include" = "all";
  let isInitialized = false;

  // Initialize mode from arrays on mount
  onMount(() => {
    const pkAttnums = primaryKeyColumns.map((col) => col.attnum);

    // Ensure mutual exclusivity: if both arrays have values, prefer exclude
    if (excludeColumnAttnums.length > 0 && includeColumnAttnums.length > 0) {
      includeColumnAttnums = [];
    }

    // Filter out primary keys from exclude list if needed
    if (selectedTable && excludeColumnAttnums.length > 0) {
      const filteredExclude = excludeColumnAttnums.filter(
        (attnum) => !pkAttnums.includes(attnum),
      );
      if (filteredExclude.length !== excludeColumnAttnums.length) {
        excludeColumnAttnums = filteredExclude;
      }
    }

    // Ensure primary keys are always included when using include mode
    if (includeColumnAttnums.length > 0) {
      const missingPks = pkAttnums.filter(
        (attnum) => !includeColumnAttnums.includes(attnum),
      );
      if (missingPks.length > 0) {
        includeColumnAttnums = [...includeColumnAttnums, ...missingPks];
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
    isInitialized = true;
  });

  // Track previous mode to detect changes
  let previousMode: "all" | "exclude" | "include" | null = null;

  // Handle mode changes explicitly to ensure arrays are mutually exclusive
  function handleModeChange(newMode: "all" | "exclude" | "include") {
    if (!isInitialized) return;

    if (newMode === "all") {
      excludeColumnAttnums = [];
      includeColumnAttnums = [];
    } else if (newMode === "exclude") {
      // Clear include array and filter out PKs from exclude array
      includeColumnAttnums = [];
      const pkAttnums = primaryKeyColumns.map((col) => col.attnum);
      excludeColumnAttnums = excludeColumnAttnums.filter(
        (attnum) => !pkAttnums.includes(attnum),
      );
    } else if (newMode === "include") {
      // Clear exclude array and ensure PKs are always included
      excludeColumnAttnums = [];
      const pkAttnums = primaryKeyColumns.map((col) => col.attnum);
      const missingPks = pkAttnums.filter(
        (attnum) => !includeColumnAttnums.includes(attnum),
      );
      if (missingPks.length > 0) {
        includeColumnAttnums = [...includeColumnAttnums, ...missingPks];
      }
    }
    previousMode = newMode;
  }

  // Watch mode changes and call handler
  $: if (isInitialized) {
    if (previousMode === null) {
      previousMode = columnSelectionMode;
    } else if (previousMode !== columnSelectionMode) {
      handleModeChange(columnSelectionMode);
    }
  }

  // Ensure primary keys are always included and never excluded
  $: if (isInitialized && selectedTable) {
    const pkAttnums = primaryKeyColumns.map((col) => col.attnum);

    // Remove any PKs from exclude array
    const pkInExclude = excludeColumnAttnums.filter((attnum) =>
      pkAttnums.includes(attnum),
    );
    if (pkInExclude.length > 0) {
      excludeColumnAttnums = excludeColumnAttnums.filter(
        (attnum) => !pkAttnums.includes(attnum),
      );
    }

    // Ensure all PKs are in include array when in include mode
    if (columnSelectionMode === "include") {
      const missingPks = pkAttnums.filter(
        (attnum) => !includeColumnAttnums.includes(attnum),
      );
      if (missingPks.length > 0) {
        includeColumnAttnums = [...includeColumnAttnums, ...missingPks];
      }
    }
  }

  function toggleColumn(attnum: number) {
    const column = availableColumns.find((col) => col.attnum === attnum);
    const isPk = column?.isPk || column?.["isPk?"] || false;

    // Prevent excluding primary key columns
    if (columnSelectionMode === "exclude") {
      if (isPk) {
        return; // Don't allow excluding PK columns
      }

      if (excludeColumnAttnums.includes(attnum)) {
        excludeColumnAttnums = excludeColumnAttnums.filter((a) => a !== attnum);
      } else {
        excludeColumnAttnums = [...excludeColumnAttnums, attnum];
      }
    } else if (columnSelectionMode === "include") {
      // Prevent unchecking primary key columns - they must always be included
      if (isPk) {
        return; // Don't allow unchecking PK columns
      }

      if (includeColumnAttnums.includes(attnum)) {
        includeColumnAttnums = includeColumnAttnums.filter((a) => a !== attnum);
      } else {
        includeColumnAttnums = [...includeColumnAttnums, attnum];
      }
    }
  }

  function isColumnSelected(attnum: number): boolean {
    const column = availableColumns.find((col) => col.attnum === attnum);
    const isPk = column?.isPk || column?.["isPk?"] || false;

    if (columnSelectionMode === "exclude") {
      // In exclude mode: checked = column is in exclude list
      // PKs are never in exclude list, so they show as unchecked (but disabled)
      if (isPk) {
        return false; // PKs are never excluded, so unchecked
      }
      return excludeColumnAttnums.includes(attnum);
    } else if (columnSelectionMode === "include") {
      // In include mode: checked = column is in include list
      // PKs are always included, so they show as checked (but disabled)
      if (isPk) {
        return true; // PKs are always included, so checked
      }
      return includeColumnAttnums.includes(attnum);
    }
    return false;
  }

  // Show selected columns in tags - for exclude mode, show excluded columns
  // For include mode, show included columns (including PKs which are always included)
  $: selectedColumns = availableColumns.filter((col) => {
    if (columnSelectionMode === "exclude") {
      // In exclude mode, show columns that are excluded (PKs are never excluded, so they won't show)
      return excludeColumnAttnums.includes(col.attnum);
    } else if (columnSelectionMode === "include") {
      // In include mode, show all included columns
      // PKs are always included (either explicitly in array or implicitly required)
      const isPk = col.isPk || col["isPk?"] || false;
      if (isPk) {
        return true; // Always show PKs in include mode
      }
      return includeColumnAttnums.includes(col.attnum);
    }
    return false;
  });
</script>

<div class="flex flex-col gap-4">
  <Label class="text-base font-medium">Column selection</Label>

  {#if isEdit && (columnSelectionMode !== "all" || excludeColumnAttnums.length > 0 || includeColumnAttnums.length > 0)}
    <Alert.Root variant="info">
      <Alert.Description>
        Column selection changes will apply to new changes going forward, but
        will not affect existing events already captured in the destination
        table.
      </Alert.Description>
    </Alert.Root>
  {/if}

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
          <Label
            for="column-selection-exclude"
            class="cursor-pointer font-normal"
          >
            Exclude specific columns
          </Label>
        </div>
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="include" id="column-selection-include" />
          <Label
            for="column-selection-include"
            class="cursor-pointer font-normal"
          >
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
          {#each availableColumns as column}
            {@const isPk = column.isPk || column["isPk?"] || false}
            {@const isChecked = isColumnSelected(column.attnum)}
            <div class="flex items-center space-x-2">
              {#if isPk}
                <Tooltip.Root openDelay={200}>
                  <Tooltip.Trigger asChild let:builder>
                    <div
                      builders={[builder]}
                      class="flex items-center space-x-2 flex-1"
                    >
                      <Checkbox
                        id="column-pk-{column.attnum}"
                        checked={isChecked}
                        disabled={true}
                        data-disabled="true"
                      />
                      <span
                        class="flex-1 flex items-center gap-2 opacity-50 cursor-not-allowed select-none"
                      >
                        <span>{column.name}</span>
                        <span
                          class="text-xs px-1.5 py-0.5 rounded bg-muted text-muted-foreground"
                        >
                          PK
                        </span>
                        <span class="text-xs text-muted-foreground">
                          ({column.type})
                        </span>
                      </span>
                    </div>
                  </Tooltip.Trigger>
                  <Tooltip.Content class="max-w-xs">
                    <p class="text-sm">
                      {#if columnSelectionMode === "exclude"}
                        Primary key columns cannot be excluded from sync. They
                        are always included and required for change tracking.
                      {:else}
                        Primary key columns are always included in sync. They
                        are required for change tracking and cannot be
                        unchecked.
                      {/if}
                    </p>
                  </Tooltip.Content>
                </Tooltip.Root>
              {:else}
                <Checkbox
                  id="column-{column.attnum}"
                  checked={isChecked}
                  disabled={false}
                  onCheckedChange={() => toggleColumn(column.attnum)}
                />
                <Label
                  for="column-{column.attnum}"
                  class="cursor-pointer flex-1 flex items-center gap-2"
                >
                  <span>{column.name}</span>
                  <span class="text-xs text-muted-foreground">
                    ({column.type})
                  </span>
                </Label>
              {/if}
            </div>
          {/each}
        </div>

        {#if selectedColumns.length > 0}
          <div class="flex flex-wrap gap-2 pt-2">
            {#each selectedColumns as column}
              {@const isPk = column.isPk || column["isPk?"] || false}
              {#if isPk}
                <Tooltip.Root openDelay={200}>
                  <Tooltip.Trigger asChild let:builder>
                    <div
                      builders={[builder]}
                      class="flex items-center gap-1 px-2 py-1 rounded-md bg-muted text-sm opacity-75"
                    >
                      <span>{column.name}</span>
                      <span
                        class="text-xs px-1 py-0.5 rounded bg-muted-foreground/20 text-muted-foreground"
                      >
                        PK
                      </span>
                    </div>
                  </Tooltip.Trigger>
                  <Tooltip.Content class="max-w-xs">
                    <p class="text-sm">
                      {#if columnSelectionMode === "exclude"}
                        Primary key columns cannot be excluded from sync. They
                        are always included and required for change tracking.
                      {:else}
                        Primary key columns are always included in sync. They
                        are required for change tracking and cannot be
                        unchecked.
                      {/if}
                    </p>
                  </Tooltip.Content>
                </Tooltip.Root>
              {:else}
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
              {/if}
            {/each}
          </div>
        {/if}
      </div>
    {/if}
  </div>
</div>
