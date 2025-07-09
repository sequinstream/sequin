<script lang="ts">
  import { CheckIcon, TableIcon } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import {
    Table as TableComponent,
    TableBody,
    TableCell,
    TableRow,
  } from "$lib/components/ui/table";
  import type { Table } from "$lib/databases/types";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { slide } from "svelte/transition";
  import { Checkbox } from "$lib/components/ui/checkbox";
  import Datetime from "./Datetime.svelte";

  export let form: {
    selectedTables?: Array<{
      oid: number;
      type: "full" | "partial";
      sortColumnAttnum?: number | null;
      initialMinCursor?: string | Date | number | null;
    }>;
  };
  export let tables_included_in_source: Table[];

  // Initialize selectedTables if not present
  if (!form.selectedTables) {
    form.selectedTables = [];
  }

  const defaultSortColumnNames = [
    // Updated/Modified columns
    "updated_at",
    "UpdatedAt",
    "updatedAt",
    "last_modified",
    "LastModified",
    "lastModified",
    "last_modified_at",
    "LastModifiedAt",
    "lastModifiedAt",
    "last_updated",
    "lastUpdated",
    "last_updated_at",
    "lastUpdatedAt",
    "modified_at",
    "ModifiedAt",
    "modifiedAt",
    "modified_date",
    "modifiedDate",
    "modified_on",
    "modifiedOn",
    "update_time",
    "updateTime",
    "modification_date",
    "modificationDate",
    "dateModified",
    "dateUpdated",

    // Created/Inserted columns
    "created_at",
    "CreatedAt",
    "createdAt",
    "inserted_at",
    "InsertedAt",
    "insertedAt",
    "creation_date",
    "CreationDate",
    "creationDate",
    "creation_time",
    "create_time",
    "createTime",
    "created_date",
    "created_on",
    "DateCreated",
    "dateCreated",
    "insert_date",
    "insert_time",
    "insertTime",
    "timestamp",
  ];

  // Handle table selection for multi-table mode
  function toggleTableSelection(tableOid: number) {
    if (!form.selectedTables) form.selectedTables = [];

    const index = form.selectedTables.findIndex((t) => t.oid === tableOid);
    if (index > -1) {
      form.selectedTables = form.selectedTables.filter(
        (t) => t.oid !== tableOid,
      );
    } else {
      form.selectedTables = [
        ...form.selectedTables,
        {
          oid: tableOid,
          type: "full",
          sortColumnAttnum: null,
          initialMinCursor: null,
        },
      ];
    }
  }

  function toggleSelectAll() {
    if (!tables_included_in_source || !form.selectedTables) return;

    if (form.selectedTables.length === tables_included_in_source.length) {
      form.selectedTables = [];
    } else {
      form.selectedTables = tables_included_in_source.map((t) => ({
        oid: t.oid,
        type: "full",
        sortColumnAttnum: null,
        initialMinCursor: null,
      }));
    }
  }

  function handleSortColumnInput(e: Event, tableOid: number) {
    const target = e.target as HTMLSelectElement;
    updateTableOptions(
      tableOid,
      target.value ? parseInt(target.value, 10) : null,
      null, // Clear the initialMinCursor when sort column changes
    );
  }

  function handleBackfillMode(tableOid: number, mode: "full" | "partial") {
    const index = form.selectedTables?.findIndex((t) => t.oid === tableOid);
    if (index > -1) {
      const table = tables_included_in_source.find((t) => t.oid === tableOid);
      const defaultColumn = table?.columns.find((col) =>
        defaultSortColumnNames.includes(col.name),
      );
      form.selectedTables = [
        ...form.selectedTables.slice(0, index),
        {
          ...form.selectedTables[index],
          type: mode,
          ...(mode === "full"
            ? {
                sortColumnAttnum: null,
                initialMinCursor: null,
              }
            : {
                sortColumnAttnum: defaultColumn?.attnum ?? null,
                initialMinCursor: null,
              }),
        },
        ...form.selectedTables.slice(index + 1),
      ];
    }
  }

  function updateTableOptions(
    tableOid: number,
    sortColumnAttnum: number | null,
    initialMinCursor: string | Date | number | null,
  ) {
    const index = form.selectedTables.findIndex((t) => t.oid === tableOid);
    if (index > -1) {
      form.selectedTables = [
        ...form.selectedTables.slice(0, index),
        {
          ...form.selectedTables[index],
          sortColumnAttnum,
          initialMinCursor,
        },
        ...form.selectedTables.slice(index + 1),
      ];
    }
  }

  $: allTablesSelected =
    form.selectedTables && tables_included_in_source
      ? form.selectedTables.length === tables_included_in_source.length
      : false;

  $: console.log(form.selectedTables);

  $: selectedTableOids = form.selectedTables?.map((t) => t.oid) ?? [];

  function handleStartPositionInput(e: Event, tableOid: number) {
    const target = e.target as HTMLInputElement;
    const selectedTable = form.selectedTables?.find((t) => t.oid === tableOid);
    updateTableOptions(
      tableOid,
      selectedTable?.sortColumnAttnum ?? null,
      target.value || null,
    );
  }

  function handleDateChange(date: Date, tableOid: number) {
    const selectedTable = form.selectedTables?.find((t) => t.oid === tableOid);
    updateTableOptions(
      tableOid,
      selectedTable?.sortColumnAttnum ?? null,
      date.toISOString(),
    );
  }
</script>

<div class="space-y-4">
  <div class="space-y-4">
    <div>
      <p class="text-sm text-muted-foreground mt-1">
        Select one or more tables to backfill. Sequin will backfill rows in the
        selected tables while concurrently capturing changes.
      </p>
    </div>

    <div class="border rounded-lg overflow-hidden">
      <div class="flex items-center justify-between p-4 border-b bg-gray-50">
        <h3 class="font-medium">Tables to backfill</h3>
        <Button variant="outline" size="sm" on:click={toggleSelectAll}>
          {allTablesSelected ? "Deselect all" : "Select all"}
        </Button>
      </div>

      <div class="overflow-y-auto" style="max-height: calc(75vh - 300px)">
        {#if tables_included_in_source.length === 0}
          <div class="p-8 text-center text-gray-500">
            <p>No tables in source to backfill.</p>
          </div>
        {:else}
          <TableComponent>
            <TableBody>
              {#each tables_included_in_source as table (table.oid)}
                <TableRow
                  class={selectedTableOids.includes(table.oid)
                    ? "bg-blue-50 hover:bg-blue-100"
                    : "hover:bg-gray-100"}
                >
                  <TableCell class="w-12 p-0">
                    <div class="h-full flex items-center justify-center">
                      <Checkbox
                        checked={selectedTableOids.includes(table.oid)}
                        onCheckedChange={() => toggleTableSelection(table.oid)}
                      />
                    </div>
                  </TableCell>
                  <TableCell class="p-4">
                    <div class="h-8 flex items-center justify-between">
                      <div class="flex items-center space-x-2">
                        <span
                          class={selectedTableOids.includes(table.oid)
                            ? "font-medium"
                            : ""}>{table.schema}.{table.name}</span
                        >
                      </div>

                      {#if form.selectedTables?.find((t) => t.oid === table.oid)}
                        {@const selectedTable = form.selectedTables?.find(
                          (t) => t.oid === table.oid,
                        )}
                        <div class="flex">
                          <button
                            class="text-xs px-2 py-1 transition-all border {selectedTable?.type ===
                            'full'
                              ? 'bg-primary text-primary-foreground border-primary'
                              : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} rounded-l-md border-r-0"
                            on:click={() =>
                              handleBackfillMode(table.oid, "full")}
                          >
                            Full
                          </button>
                          <button
                            class="text-xs px-2 py-1 transition-all border {selectedTable?.type ===
                            'partial'
                              ? 'bg-primary text-primary-foreground border-primary'
                              : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} rounded-r-md"
                            on:click={() =>
                              handleBackfillMode(table.oid, "partial")}
                          >
                            Partial
                          </button>
                        </div>
                      {/if}
                    </div>

                    {#if form.selectedTables?.find((t) => t.oid === table.oid)?.type === "partial"}
                      <div
                        transition:slide={{ duration: 200 }}
                        class="mt-4 space-y-4 w-full"
                      >
                        <div class="space-y-4">
                          <div class="space-y-2">
                            <Label for="sortColumn-{table.oid}" class="text-xs"
                              >Sort Column</Label
                            >
                            <select
                              id="sortColumn-{table.oid}"
                              class="h-8 py-0 text-sm w-full rounded-md border border-input bg-background px-3"
                              value={form.selectedTables?.find(
                                (t) => t.oid === table.oid,
                              )?.sortColumnAttnum ?? ""}
                              on:change={(e) =>
                                handleSortColumnInput(e, table.oid)}
                            >
                              <option value="">Select a column</option>
                              {#each table.columns as column}
                                <option value={column.attnum}>
                                  {column.name}
                                </option>
                              {/each}
                            </select>
                          </div>
                          <div class="space-y-2">
                            <Label
                              for="startPosition-{table.oid}"
                              class="text-xs">Start Position</Label
                            >
                            {#if table.columns
                              .find((c) => c.attnum === form.selectedTables?.find((t) => t.oid === table.oid)?.sortColumnAttnum)
                              ?.type.startsWith("timestamp")}
                              <Datetime
                                id="startPosition-{table.oid}"
                                class="text-sm"
                                value={new Date(
                                  form.selectedTables?.find(
                                    (t) => t.oid === table.oid,
                                  )?.initialMinCursor ?? new Date(),
                                )}
                                onChange={(date) =>
                                  handleDateChange(date, table.oid)}
                              />
                            {:else if ["integer", "bigint", "smallint", "serial"].includes(table.columns.find((c) => c.attnum === form.selectedTables?.find((t) => t.oid === table.oid)?.sortColumnAttnum)?.type)}
                              <Input
                                type="number"
                                id="startPosition-{table.oid}"
                                class="h-8 text-sm"
                                placeholder="e.g. 1000"
                                value={form.selectedTables?.find(
                                  (t) => t.oid === table.oid,
                                )?.initialMinCursor ?? ""}
                                on:input={(e) =>
                                  handleStartPositionInput(e, table.oid)}
                              />
                            {:else}
                              <Input
                                type="text"
                                id="startPosition-{table.oid}"
                                class="h-8 text-sm"
                                placeholder="Enter start position"
                                value={form.selectedTables?.find(
                                  (t) => t.oid === table.oid,
                                )?.initialMinCursor ?? ""}
                                on:input={(e) =>
                                  handleStartPositionInput(e, table.oid)}
                              />
                            {/if}
                          </div>
                        </div>
                      </div>
                    {/if}
                  </TableCell>
                </TableRow>
              {/each}
            </TableBody>
          </TableComponent>
        {/if}
      </div>
    </div>
  </div>
</div>
