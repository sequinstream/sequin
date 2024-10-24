<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Switch } from "$lib/components/ui/switch";
  import { Checkbox } from "$lib/components/ui/checkbox";
  import { onMount } from "svelte";

  export let errors: any;
  export let isEditMode: boolean;
  export let selectedTable: any;
  export let groupColumnAttnums: number[] | null;
  export let onGroupColumnAttnumsChange: (attnums: number[]) => void;

  let useCustomGrouping = false;
  let customGroupColumnAttnums: number[] = [];
  let primaryKeyAttnums: number[] = [];
  let isUsingPrimaryKeys = false;
  let previousTableOid: number | null = null;
  let groupColumnError: string | null = null;

  $: isUsingPrimaryKeys = arraysEqual(groupColumnAttnums, primaryKeyAttnums);
  $: groupColumnError = errors.sequence_filter?.group_column_attnums?.[0];

  $: {
    if (selectedTable && selectedTable.oid !== previousTableOid) {
      previousTableOid = selectedTable.oid;

      // Update primary key attnums
      primaryKeyAttnums = selectedTable.columns
        .filter((column) => column["isPk?"])
        .map((column) => column.attnum);

      // Reset custom group column attnums to match primary keys
      customGroupColumnAttnums = [...primaryKeyAttnums];

      // Automatically use custom grouping if no primary keys are available
      useCustomGrouping = primaryKeyAttnums.length === 0;

      // Set group column attnums in the form
      setGroupColumnAttnums();
    }
  }

  function setGroupColumnAttnums() {
    const attnums = useCustomGrouping
      ? customGroupColumnAttnums
      : primaryKeyAttnums;

    if (!isEditMode && !arraysEqual(groupColumnAttnums, attnums)) {
      onGroupColumnAttnumsChange(attnums);
      groupColumnAttnums = attnums;
    }
  }

  function updatePrimaryKeyAttnums() {}

  function toggleGroupingMode(checked: boolean) {
    useCustomGrouping = checked;

    setGroupColumnAttnums();
  }

  function toggleColumnGrouping(attnum: number) {
    const index = customGroupColumnAttnums.indexOf(attnum);

    if (index === -1) {
      customGroupColumnAttnums = [...customGroupColumnAttnums, attnum];
    } else {
      customGroupColumnAttnums = customGroupColumnAttnums.filter(
        (num) => num !== attnum
      );
    }

    setGroupColumnAttnums();
  }

  function getGroupingColumnNames() {
    const attnums = useCustomGrouping
      ? customGroupColumnAttnums
      : primaryKeyAttnums;
    return selectedTable.columns
      .filter((column) => attnums.includes(column.attnum))
      .map((column) => column.name)
      .join(", ");
  }

  function arraysEqual(a, b) {
    if (a === b) return true;
    if (a == null || b == null) return false;
    if (a.length !== b.length) return false;

    for (let i = 0; i < a.length; ++i) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }
</script>

<Card>
  <CardHeader>
    <div class="flex items-center justify-between">
      <CardTitle>Message Grouping</CardTitle>
      {#if !isEditMode}
        <div class="flex items-center space-x-2">
          <Switch
            id="use-custom-grouping"
            checked={selectedTable && useCustomGrouping}
            onCheckedChange={toggleGroupingMode}
            disabled={!selectedTable || primaryKeyAttnums.length === 0}
          />
          <span class="text-sm">Custom</span>
        </div>
      {:else}
        <div class="flex items-center space-x-2">
          <Switch
            id="use-custom-grouping"
            checked={!isUsingPrimaryKeys}
            disabled={true}
          />
          <span class="text-sm">Custom</span>
        </div>
      {/if}
    </div>
  </CardHeader>
  <CardContent class="space-y-4">
    {#if selectedTable && primaryKeyAttnums.length === 0 && !isEditMode}
      <p class="text-sm text-info">
        No primary keys available. Custom grouping is required.
      </p>
    {/if}
    {#if isEditMode}
      {#if isUsingPrimaryKeys}
        <p class="text-sm text-muted-foreground">
          Using primary keys for grouping.
        </p>
      {:else}
        <div class="space-y-2 max-h-96 overflow-y-auto">
          {#each selectedTable.columns as column}
            <div class="flex items-center space-x-2">
              <Checkbox
                id={`column-${column.attnum}`}
                checked={groupColumnAttnums.includes(column.attnum)}
                disabled={true}
              />
              <label
                for={`column-${column.attnum}`}
                class="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
              >
                {column.name}
              </label>
            </div>
          {/each}
        </div>
      {/if}
    {:else if !useCustomGrouping}
      <p class="text-sm text-muted-foreground">
        By default, Sequin uses primary keys to group messages. This ensures
        that records are processed serially for each individual record.
      </p>
      {#if groupColumnError}
        <p class="text-destructive text-sm">{groupColumnError}</p>
      {/if}
    {:else}
      <p class="text-sm text-muted-foreground mb-4">
        Select the columns to use for custom grouping.
      </p>
      <div class="space-y-2 max-h-96 overflow-y-auto">
        {#each selectedTable.columns as column}
          <div class="flex items-center space-x-2">
            <Checkbox
              id={`column-${column.attnum}`}
              checked={customGroupColumnAttnums.includes(column.attnum)}
              onCheckedChange={() => toggleColumnGrouping(column.attnum)}
            />
            <label
              for={`column-${column.attnum}`}
              class="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
            >
              {column.name}
            </label>
          </div>
        {/each}
      </div>
      {#if groupColumnError}
        <p class="text-destructive text-sm">{groupColumnError}</p>
      {/if}
    {/if}
  </CardContent>
</Card>
