<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import ChevronDown from "lucide-svelte/icons/chevron-down";
  import ColumnList from "./ColumnList.svelte";
  export let errors: any;
  export let isEditMode: boolean;
  export let selectedTable: any;
  export let groupColumnAttnums: number[] = [];
  export let infoText: string | null = null;

  let defaultGroupColumns = selectedTable?.default_group_columns || [];
  let groupColumnError: string | null = null;
  let useCustomGrouping = false;
  let isExpanded = false;

  $: groupColumnError = errors.sequence_filter?.group_column_attnums?.[0];
  $: defaultGroupColumns = selectedTable?.default_group_columns || [];

  $: {
    if (
      !isEditMode &&
      groupColumnAttnums.length === 0 &&
      defaultGroupColumns.length > 0
    ) {
      // Set initial group columns if not set
      groupColumnAttnums = defaultGroupColumns;
    }

    if (
      groupColumnAttnums.sort().join() !== defaultGroupColumns.sort().join()
    ) {
      useCustomGrouping = true;
    } else {
      useCustomGrouping = false;
    }
  }

  function toggleColumnGrouping(attnum: number) {
    const index = groupColumnAttnums.indexOf(attnum);

    if (index === -1) {
      groupColumnAttnums = [...groupColumnAttnums, attnum];
    } else {
      groupColumnAttnums = groupColumnAttnums.filter((num) => num !== attnum);
    }
  }

  $: summaryText = useCustomGrouping
    ? `Using these columns for grouping: ${groupColumnAttnums
        .map(
          (attnum) =>
            selectedTable?.columns.find((col) => col.attnum === attnum)?.name,
        )
        .join(", ")}`
    : selectedTable?.is_event_table
      ? "Using source_database_id, source_table_oid, and record_pk for grouping, which is the default"
      : "Using primary keys for grouping, which is the default";
</script>

<!-- Edit Mode Card -->
{#if isEditMode}
  <Card>
    <CardHeader>
      <div class="flex items-center justify-between">
        <CardTitle>Message grouping</CardTitle>
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
        <button
          type="button"
          class="flex items-center space-x-2 text-sm hover:text-primary transition-colors"
          on:click={() => (isExpanded = !isExpanded)}
          disabled={!selectedTable}
        >
          <div
            class="transition-transform duration-200"
            class:rotate-180={isExpanded}
          >
            <ChevronDown class="h-4 w-4" />
          </div>
        </button>
      </div>
    </CardHeader>
    <CardContent>
      <div class="space-y-4">
        {#if !isExpanded}
          <p class="text-sm text-muted-foreground">
            {summaryText}
          </p>
        {:else}
          <p class="text-sm text-muted-foreground">
            {selectedTable.is_event_table
              ? "By default, Sequin uses these columns to group event table messages: source_database_id, source_table_oid, and record_pk. This ensures that records are processed serially."
              : "By default, Sequin uses primary keys to group messages. This ensures that records are processed serially for each individual record."}
          </p>
          <p class="text-sm text-muted-foreground">
            Alternatively, select custom columns to use for grouping.
          </p>

          <ColumnList
            columns={selectedTable.columns}
            selectedAttnums={groupColumnAttnums}
            onToggle={toggleColumnGrouping}
            readonly={isEditMode}
          />

          {#if infoText}
            <p class="text-sm text-muted-foreground">
              {infoText}
            </p>
          {/if}

          {#if groupColumnError}
            <p class="text-destructive text-sm">{groupColumnError}</p>
          {/if}
        {/if}
      </div>
    </CardContent>
  </Card>
{/if}
