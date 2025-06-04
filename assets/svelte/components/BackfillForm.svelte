<script lang="ts">
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Label } from "$lib/components/ui/label";
  import { Input } from "$lib/components/ui/input";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Checkbox } from "$lib/components/ui/checkbox";
  import { ExternalLinkIcon } from "lucide-svelte";
  import type { Table } from "$lib/databases/types";
  import Datetime from "./Datetime.svelte";
  import { conditionalClass } from "$lib/utils";

  export let form: {
    startPosition: "beginning" | "specific" | "none";
    sortColumnAttnum: number | null;
    initialSortColumnValue: any | null;
    selectedTableOids?: number[]; // For multi-table mode
  };
  export let formErrors: Record<string, string>;
  export let table: Table | null;
  export let tablesInSchema: Table[] | null;
  export let showNoBackfill: boolean = true;

  // Determine if we're in multi-table mode
  $: isMultiTableMode = !table && tablesInSchema && tablesInSchema.length > 0;

  // Initialize selectedTableOids if not present
  $: if (isMultiTableMode && !form.selectedTableOids) {
    form.selectedTableOids = [];
  }

  $: {
    if (!isMultiTableMode && table && !form.sortColumnAttnum) {
      const defaultName = defaultSortColumnNames.find((col) =>
        table.columns.find((c) => c.name.toLowerCase() === col.toLowerCase()),
      );
      if (defaultName) {
        const column = table.columns.find(
          (col) => col.name.toLowerCase() === defaultName.toLowerCase(),
        );
        form.sortColumnAttnum = column.attnum;
      }
    }
  }

  // Default sort column names to look for
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

  $: selectedColumnName =
    !isMultiTableMode && table
      ? table.columns.find((column) => column.attnum === form.sortColumnAttnum)
          ?.name || "Select a sort column"
      : "";

  $: sortColumn =
    !isMultiTableMode && table
      ? table.columns.find((column) => column.attnum === form.sortColumnAttnum)
      : null;

  // Handle table selection for multi-table mode
  function toggleTableSelection(tableOid: number) {
    if (!form.selectedTableOids) form.selectedTableOids = [];

    const index = form.selectedTableOids.indexOf(tableOid);
    if (index > -1) {
      form.selectedTableOids = form.selectedTableOids.filter(
        (id) => id !== tableOid,
      );
    } else {
      form.selectedTableOids = [...form.selectedTableOids, tableOid];
    }
  }

  function toggleSelectAll() {
    if (!tablesInSchema || !form.selectedTableOids) return;

    if (form.selectedTableOids.length === tablesInSchema.length) {
      form.selectedTableOids = [];
    } else {
      form.selectedTableOids = tablesInSchema.map((t) => t.oid);
    }
  }

  $: allTablesSelected =
    isMultiTableMode && form.selectedTableOids && tablesInSchema
      ? form.selectedTableOids.length === tablesInSchema.length
      : false;
</script>

<div class="space-y-4">
  {#if isMultiTableMode}
    <!-- Multi-table mode -->
    <div class="space-y-4">
      <div>
        <p class="text-sm text-muted-foreground mt-1">
          Select one or more tables to backfill. All selected tables will be
          backfilled from the beginning.
        </p>
      </div>

      <div class="space-y-2">
        <div class="flex items-center space-x-2 border-b pb-2">
          <Checkbox
            checked={allTablesSelected}
            onCheckedChange={toggleSelectAll}
            id="select-all"
          />
          <Label for="select-all" class="font-medium">
            Select all {tablesInSchema.length} tables
          </Label>
        </div>

        <div class="max-h-60 overflow-y-auto space-y-2">
          {#each tablesInSchema as table (table.oid)}
            <div class="flex items-center space-x-2">
              <Checkbox
                checked={form.selectedTableOids?.includes(table.oid) || false}
                onCheckedChange={() => toggleTableSelection(table.oid)}
                id={`table-${table.oid}`}
              />
              <Label for={`table-${table.oid}`} class="cursor-pointer">
                {table.name}
              </Label>
            </div>
          {/each}
        </div>
      </div>
    </div>
  {:else}
    <!-- Single table mode - existing functionality -->
    <div class="space-y-4">
      <p class="text-sm text-muted-foreground">
        Select where you want to start the backfill from.
      </p>
    </div>

    <RadioGroup bind:value={form.startPosition}>
      {#if showNoBackfill}
        <div class="flex items-center space-x-2">
          <RadioGroupItem value="none" id="none" />
          <Label for="none">No backfill</Label>
        </div>
      {/if}
      <div class="flex items-center space-x-2">
        <RadioGroupItem value="beginning" id="beginning" />
        <Label for="beginning">Backfill all rows</Label>
      </div>
      <div class="flex items-center space-x-2">
        <RadioGroupItem value="specific" id="specific" />
        <Label for="specific">Backfill from a specific point</Label>
      </div>
    </RadioGroup>

    {#if form.startPosition === "none"}
      <p class="text-sm text-muted-foreground">
        No initial backfill will be performed. You can run backfills at any time
        in the future.
      </p>
    {:else if form.startPosition === "beginning"}
      <p class="text-sm text-muted-foreground">
        Sequin will backfill all rows in the table.
      </p>
    {:else if form.startPosition === "specific"}
      <div class="space-y-4">
        <!-- Integrated SortColumnSelector functionality -->
        <div class="space-y-2">
          <Label for="sort_column_attnum">Sort column</Label>
          <p class="text-sm text-muted-foreground mt-1 mb-2">
            Select a sort column for the backfill. A good example of a sort
            column is <code>updated_at</code>.
            <a
              href="https://sequinstream.com/docs/reference/backfills#backfill-ordering"
              target="_blank"
              rel="noopener noreferrer"
              class="inline-flex items-center text-link hover:underline"
            >
              Learn more
              <ExternalLinkIcon class="w-3 h-3 ml-1" />
            </a>
          </p>
          <Select
            selected={{
              value: form.sortColumnAttnum,
              label: selectedColumnName,
            }}
            onSelectedChange={(event) => {
              form.sortColumnAttnum = event.value;
            }}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select a sort column" />
            </SelectTrigger>
            <SelectContent>
              {#each table.columns as column}
                <SelectItem value={column.attnum}>{column.name}</SelectItem>
              {/each}
            </SelectContent>
          </Select>
        </div>
        {#if sortColumn}
          <div class="grid grid-cols-[auto_1fr] gap-4 content-center mt-4">
            <div
              class="flex items-center space-x-2 text-sm font-mono {conditionalClass(
                sortColumn.type.startsWith('timestamp'),
                'pt-8',
              )}"
            >
              <span class="bg-secondary-2xSubtle px-2 py-1 rounded"
                >{sortColumn.name}</span
              >
              <span class="bg-secondary-2xSubtle px-2 py-1 rounded">&gt;=</span>
            </div>

            {#if sortColumn.type.startsWith("timestamp")}
              <Datetime bind:value={form.initialSortColumnValue} />
              {#if formErrors?.initialSortColumnValue}
                <p class="text-sm text-red-500 mt-2">
                  {formErrors.initialSortColumnValue}
                </p>
              {/if}
            {:else if ["integer", "bigint", "smallint", "serial"].includes(sortColumn.type)}
              <Input type="number" bind:value={form.initialSortColumnValue} />
            {:else}
              <Input type="text" bind:value={form.initialSortColumnValue} />
            {/if}
          </div>
        {/if}
      </div>
    {/if}
  {/if}
</div>
