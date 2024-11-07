<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Switch } from "$lib/components/ui/switch";
  import ColumnList from "./ColumnList.svelte";
  export let errors: any;
  export let isEditMode: boolean;
  export let selectedTable: any;
  export let groupColumnAttnums: number[] = [];

  let defaultGroupColumns = selectedTable?.default_group_columns || [];
  let groupColumnError: string | null = null;
  let useCustomGrouping = false;
  let useCustomGroupingChanged = false;

  $: groupColumnError = errors.sequence_filter?.group_column_attnums?.[0];
  $: defaultGroupColumns = selectedTable?.default_group_columns || [];

  $: {
    if (
      !isEditMode &&
      groupColumnAttnums.length === 0 &&
      defaultGroupColumns.length > 0
    ) {
      groupColumnAttnums = defaultGroupColumns;
      if (!useCustomGroupingChanged) {
        useCustomGrouping = false;
        useCustomGroupingChanged = true;
      }
    }

    if (
      isEditMode &&
      groupColumnAttnums.sort().join() !== defaultGroupColumns.sort().join()
    ) {
      useCustomGrouping = true;
    }
  }

  function toggleGroupingMode(checked: boolean) {
    useCustomGrouping = checked;
    useCustomGroupingChanged = true;
  }

  function toggleColumnGrouping(attnum: number) {
    const index = groupColumnAttnums.indexOf(attnum);

    if (index === -1) {
      groupColumnAttnums = [...groupColumnAttnums, attnum];
    } else {
      groupColumnAttnums = groupColumnAttnums.filter((num) => num !== attnum);
    }
  }
</script>

<!-- Edit Mode Card -->
{#if isEditMode}
  <Card>
    <CardHeader>
      <div class="flex items-center justify-between">
        <CardTitle>Message grouping</CardTitle>
        <div class="flex items-center space-x-2">
          <Switch
            id="use-custom-grouping"
            checked={useCustomGrouping}
            disabled={true}
          />
          <span class="text-sm">Custom</span>
        </div>
      </div>
    </CardHeader>
    <CardContent>
      {#if !useCustomGrouping && !selectedTable.is_event_table}
        <p class="text-sm text-muted-foreground">
          Using primary keys for grouping.
        </p>
      {:else}
        <ColumnList
          columns={selectedTable.columns}
          selectedAttnums={groupColumnAttnums}
          readonly={true}
        />
      {/if}
    </CardContent>
  </Card>

  <!-- Create Mode - No PKs Available -->
{:else if selectedTable && defaultGroupColumns.length === 0}
  <Card>
    <CardHeader>
      <CardTitle>Message grouping</CardTitle>
    </CardHeader>
    <CardContent>
      <p class="text-sm text-info mb-4">
        No primary keys available. Custom grouping is required.
      </p>
      <ColumnList
        columns={selectedTable.columns}
        selectedAttnums={groupColumnAttnums}
        onToggle={toggleColumnGrouping}
      />
      {#if groupColumnError}
        <p class="text-destructive text-sm mt-2">{groupColumnError}</p>
      {/if}
    </CardContent>
  </Card>

  <!-- Create Mode - Normal -->
{:else if selectedTable}
  <Card>
    <CardHeader>
      <div class="flex items-center justify-between">
        <CardTitle>Message grouping</CardTitle>
        <div class="flex items-center space-x-2">
          <Switch
            id="use-custom-grouping"
            checked={useCustomGrouping}
            onCheckedChange={toggleGroupingMode}
            disabled={!selectedTable}
          />
          <span class="text-sm">Custom</span>
        </div>
      </div>
    </CardHeader>
    <CardContent>
      {#if !useCustomGrouping && !selectedTable.is_event_table}
        <p class="text-sm text-muted-foreground">
          By default, Sequin uses primary keys to group messages. This ensures
          that records are processed serially for each individual record.
        </p>
      {:else if !useCustomGrouping && selectedTable.is_event_table}
        <p class="text-sm text-muted-foreground">
          By default, Sequin uses these columns to group event table messages:
          <code>source_database_id</code>, <code>source_table_oid</code>, and
          <code>record_pk</code>. This ensures that records are processed
          serially.
        </p>
      {:else}
        <p class="text-sm text-muted-foreground mb-4">
          Select the columns to use for custom grouping.
        </p>
        <ColumnList
          columns={selectedTable.columns}
          selectedAttnums={groupColumnAttnums}
          onToggle={toggleColumnGrouping}
        />
      {/if}
      {#if groupColumnError}
        <p class="text-destructive text-sm mt-2">{groupColumnError}</p>
      {/if}
    </CardContent>
  </Card>
{/if}
