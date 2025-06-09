<script lang="ts">
  import { CardTitle, ExpandableCard } from "$lib/components/ui/card";
  import type { Table } from "$lib/databases/types";
  import ColumnList from "./ColumnList.svelte";
  export let errors: any;
  export let isEditMode: boolean;
  export let selectedTable: Table;
  export let selectedSchema: string | null;
  export let groupColumnAttnums: number[] = [];
  export let infoText: string | null = null;

  let defaultGroupColumns = selectedTable?.defaultGroupColumns || [];
  let groupColumnError: string | null = null;
  let useCustomGrouping = false;

  $: groupColumnError = errors.sequence_filter?.group_column_attnums?.[0];
  $: defaultGroupColumns = selectedTable?.defaultGroupColumns || [];

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
    : selectedTable?.isEventTable
      ? "Using source_database_id, source_table_oid, and record_pk for grouping, which is the default"
      : defaultGroupColumns.length === 0
        ? "No primary keys available. Custom grouping is required."
        : "Using primary keys for grouping, which is the default";
</script>

<!-- Edit Mode Card -->
{#if isEditMode}
  <ExpandableCard
    disabled={true}
    titleTooltip="Message grouping can’t be changed for an active sink."
  >
    <svelte:fragment slot="title">
      <CardTitle>Message grouping</CardTitle>
    </svelte:fragment>

    <svelte:fragment slot="summary">
      {#if !useCustomGrouping && !selectedTable?.isEventTable}
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
    </svelte:fragment>
  </ExpandableCard>

  <!-- Create Mode - No PKs Available -->
{:else if selectedTable && defaultGroupColumns.length === 0}
  <ExpandableCard
    expanded={!isEditMode}
    disabled={defaultGroupColumns.length === 0}
  >
    <svelte:fragment slot="title">
      <CardTitle>Message grouping</CardTitle>
    </svelte:fragment>

    <svelte:fragment slot="summary">
      <p class="text-sm text-muted-foreground">
        {summaryText}
      </p>
    </svelte:fragment>

    <svelte:fragment slot="content">
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
    </svelte:fragment>
  </ExpandableCard>

  <!-- Create Mode - Normal -->
{:else}
  <ExpandableCard disabled={!selectedTable}>
    <svelte:fragment slot="title">
      <CardTitle>Message grouping</CardTitle>
    </svelte:fragment>

    <svelte:fragment slot="summary">
      {#if selectedSchema}
        <p class="text-sm text-muted-foreground">
          When syncing all tables in a schema, Sequin groups messages by the
          table and primary keys of the table. This ensures that messages are
          processed serially for each row in each table.
        </p>
        <p class="text-sm text-muted-foreground">
          Messages in different tables are processed in parallel.
        </p>
      {:else if selectedTable}
        <p class="text-sm text-muted-foreground">
          {summaryText}
        </p>
      {:else}
        <p class="text-sm text-muted-foreground">
          Please select a table or schema.
        </p>
      {/if}
    </svelte:fragment>

    <svelte:fragment slot="content">
      <p class="text-sm text-muted-foreground">
        {selectedTable.isEventTable
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
    </svelte:fragment>
  </ExpandableCard>
{/if}
