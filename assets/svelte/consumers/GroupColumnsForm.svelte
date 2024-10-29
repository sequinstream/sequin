<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Switch } from "$lib/components/ui/switch";
  import { Checkbox } from "$lib/components/ui/checkbox";

  export let errors: any;
  export let isEditMode: boolean;
  export let selectedTable: any;
  export let groupColumnAttnums: number[] | null;
  export let usingDefaultGrouping: boolean;
  export let defaultGroupColumnAttnums: number[];
  export let onGroupColumnAttnumsChange: (attnums: number[]) => void;

  let useCustomGrouping =
    usingDefaultGrouping || defaultGroupColumnAttnums?.length === 0;
  let groupColumnError: string | null = null;

  $: groupColumnError = errors.sequence_filter?.group_column_attnums?.[0];

  function toggleGroupingMode(checked: boolean) {
    useCustomGrouping = checked;
  }

  function toggleColumnGrouping(attnum: number) {
    const index = groupColumnAttnums.indexOf(attnum);

    if (index === -1) {
      groupColumnAttnums = [...groupColumnAttnums, attnum];
    } else {
      groupColumnAttnums = groupColumnAttnums.filter((num) => num !== attnum);
    }

    onGroupColumnAttnumsChange(groupColumnAttnums);
  }
</script>

<Card>
  <CardHeader>
    <div class="flex items-center justify-between">
      <CardTitle>Message grouping</CardTitle>
      {#if !isEditMode}
        <div class="flex items-center space-x-2">
          <Switch
            id="use-custom-grouping"
            checked={selectedTable && useCustomGrouping}
            onCheckedChange={toggleGroupingMode}
            disabled={!selectedTable || defaultGroupColumnAttnums.length === 0}
          />
          <span class="text-sm">Custom</span>
        </div>
      {:else}
        <div class="flex items-center space-x-2">
          <Switch
            id="use-custom-grouping"
            checked={!usingDefaultGrouping}
            disabled={true}
          />
          <span class="text-sm">Custom</span>
        </div>
      {/if}
    </div>
  </CardHeader>
  <CardContent class="space-y-4">
    {#if selectedTable && selectedTable.isEventTable && !useCustomGrouping}
      <p class="text-sm text-muted-foreground">
        By default, Sequin uses the <code>source_database_id</code>,
        <code>source_table_oid</code>, and <code>record_pk</code> columns to group
        messages from event tables. This ensures that each event for a given record
        is processed serially and in the right order.
      </p>
    {:else if selectedTable && !useCustomGrouping}
      <p class="text-sm text-muted-foreground">
        By default, Sequin uses primary keys to group messages. This ensures
        that records are processed serially for each individual record.
      </p>
    {:else if selectedTable && useCustomGrouping && defaultGroupColumnAttnums.length === 0}
      <p class="text-sm text-info">
        No primary keys available. Custom grouping is required.
      </p>
    {/if}

    {#if useCustomGrouping}
      <p class="text-sm text-muted-foreground mb-4">
        Select the columns to use for custom grouping.
      </p>
      <div class="space-y-2 max-h-96 overflow-y-auto">
        {#each selectedTable.columns as column}
          <div class="flex items-center space-x-2">
            <Checkbox
              id={`column-${column.attnum}`}
              checked={groupColumnAttnums.includes(column.attnum)}
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
