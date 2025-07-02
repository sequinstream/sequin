<script lang="ts">
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Input } from "$lib/components/ui/input";
  import {
    Table,
    TableBody,
    TableCell,
    TableRow,
  } from "$lib/components/ui/table";
  import {
    TableIcon,
    SearchIcon,
    RotateCwIcon,
    CheckIcon,
    DatabaseIcon,
    XIcon,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { onMount } from "svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import type { Source } from "../consumers/types";

  type Table = {
    oid: number;
    schema: string;
    name: string;
  };

  export let isEditMode: boolean;
  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<Table>;
    schemas: Array<string>;
  }>;

  export let selectedDatabaseId: string | undefined;
  export let source: Source;
  export let errors: Record<string, string>;
  export let pushEvent: (
    event: string,
    detail: any,
    callback: () => void,
  ) => void;
  export let onDatabaseSelect: (
    database: {
      id: string;
      name: string;
      tables: Array<Table>;
      schemas: Array<string>;
    } | null,
  ) => void;

  let selectedDatabase;
  let searchSchemaQuery = "";
  let searchTableQuery = "";
  let filteredTables: Array<Table> = [];
  let filteredSchemas: Array<string> = [];

  // Selection mode states
  type SelectionMode = "all" | "include" | "exclude";
  let schemaMode: SelectionMode = "all";
  let tableMode: SelectionMode = "all";

  // Selected items
  let selectedSchemas: Set<string> = new Set();
  let selectedTableOids: Set<number> = new Set();
  let filteredSelectedTableOids: Set<number> = new Set();

  // Add state for storing last schema selections
  let lastSchemaMode: SelectionMode = "all";
  let lastSelectedSchemas: Set<string> = new Set();

  if (isEditMode) {
    if (source.include_schemas) {
      schemaMode = "include";
      selectedSchemas = new Set(source.include_schemas);
    } else if (source.exclude_schemas) {
      schemaMode = "exclude";
      selectedSchemas = new Set(source.exclude_schemas);
    }

    if (source.include_table_oids) {
      tableMode = "include";
      selectedTableOids = new Set(source.include_table_oids);
    } else if (source.exclude_table_oids) {
      tableMode = "exclude";
      selectedTableOids = new Set(source.exclude_table_oids);
    }
  }

  function isTableIncludedBySchemaMode(table: Table): boolean {
    if (schemaMode === "all") return true;
    return schemaMode === "include"
      ? selectedSchemas.has(table.schema)
      : !selectedSchemas.has(table.schema);
  }

  // Update selected database when ID changes
  $: selectedDatabase = selectedDatabaseId
    ? databases.find((db) => db.id === selectedDatabaseId)
    : undefined;

  // Filter schemas based on search
  $: filteredSchemas =
    selectedDatabase?.schemas.filter((schema: string) =>
      schema.toLowerCase().includes((searchSchemaQuery || "").toLowerCase()),
    ) || [];

  // Filter tables based on search and schemas
  $: filteredTables =
    selectedDatabase?.tables.filter((table: Table) => {
      const includedInSearch = `${table.schema}.${table.name}`
        .toLowerCase()
        .includes((searchTableQuery || "").toLowerCase());
      return includedInSearch && isTableIncludedBySchemaMode(table);
    }) || [];

  $: filteredSelectedTableOids = new Set(
    Array.from(selectedTableOids).filter((oid) => {
      const table = selectedDatabase?.tables.find((t) => t.oid === oid);
      return table && isTableIncludedBySchemaMode(table);
    }),
  );

  let summaryText = "";

  $: {
    summaryText = "";
    if (tableMode === "all") {
      if (schemaMode === "all") {
        summaryText = "All current and future tables in your publication.";
      } else if (schemaMode === "include") {
        if (selectedSchemas.size === 0) {
          summaryText = "No schemas included; select one or more.";
        } else if (selectedSchemas.size === 1) {
          summaryText = `All current and future tables in schema "${Array.from(selectedSchemas)[0]}".`;
        } else {
          summaryText = `All current and future tables in ${selectedSchemas.size} schemas.`;
        }
      } else if (schemaMode === "exclude") {
        if (selectedSchemas.size === 0) {
          summaryText = "No schemas excluded; select one or more.";
        } else if (selectedSchemas.size === 1) {
          summaryText = `All current and future tables other than those in schema "${Array.from(selectedSchemas)[0]}".`;
        } else {
          summaryText = `All current and future tables other than those in ${selectedSchemas.size} schemas.`;
        }
      }
    } else if (tableMode === "include") {
      if (filteredSelectedTableOids.size === 0) {
        summaryText = "No tables included; select one or more.";
      } else if (filteredSelectedTableOids.size === 1) {
        const table = selectedDatabase?.tables.find(
          (t) => t.oid === Array.from(filteredSelectedTableOids)[0],
        );
        summaryText = `Including table "${table?.schema}.${table?.name}".`;
      } else {
        summaryText = `Including ${filteredSelectedTableOids.size} tables.`;
      }
    } else if (tableMode === "exclude") {
      if (filteredSelectedTableOids.size === 0) {
        summaryText = "No tables excluded; select one or more.";
      } else if (filteredSelectedTableOids.size === 1) {
        const table = selectedDatabase?.tables.find(
          (t) => t.oid === Array.from(filteredSelectedTableOids)[0],
        );
        if (schemaMode === "all") {
          summaryText = `All current and future tables other than "${table?.schema}.${table?.name}".`;
        } else if (schemaMode === "include") {
          summaryText = `All current and future tables in selected schemas other than "${table?.schema}.${table?.name}".`;
        } else if (schemaMode === "exclude") {
          summaryText = `All current and future tables except those in excluded schemas and except "${table?.schema}.${table?.name}".`;
        }
      } else {
        if (schemaMode === "all") {
          summaryText = `All current and future tables other than ${filteredSelectedTableOids.size} tables.`;
        } else if (schemaMode === "include") {
          summaryText = `All current and future tables in selected schemas other than ${filteredSelectedTableOids.size} tables.`;
        } else if (schemaMode === "exclude") {
          summaryText = `All current and future tables except those in excluded schemas and except ${filteredSelectedTableOids.size} tables.`;
        }
      }
    }
  }

  function updateSource() {
    // Update schema-related properties
    source.include_schemas =
      schemaMode === "include" ? Array.from(selectedSchemas) : null;
    source.exclude_schemas =
      schemaMode === "exclude" ? Array.from(selectedSchemas) : null;

    // Update table-related properties
    if (tableMode === "include") {
      const includedTables = Array.from(selectedTableOids).filter((oid) => {
        const table = selectedDatabase?.tables.find((t) => t.oid === oid);
        return table && isTableIncludedBySchemaMode(table);
      });
      source.include_table_oids = includedTables;
      source.exclude_table_oids = null;
    } else if (tableMode === "exclude") {
      const excludedTables = Array.from(selectedTableOids).filter((oid) => {
        const table = selectedDatabase?.tables.find((t) => t.oid === oid);
        return table && isTableIncludedBySchemaMode(table);
      });
      source.include_table_oids = null;
      source.exclude_table_oids = excludedTables;
    } else {
      source.include_table_oids = null;
      source.exclude_table_oids = null;
    }
  }

  function toggleSchema(schema: string) {
    if (selectedSchemas.has(schema)) {
      selectedSchemas.delete(schema);
    } else {
      selectedSchemas.add(schema);
    }
    selectedSchemas = selectedSchemas; // Trigger reactivity
    updateSource();
  }

  function toggleTable(table: Table) {
    if (selectedTableOids.has(table.oid)) {
      selectedTableOids.delete(table.oid);
    } else {
      selectedTableOids.add(table.oid);
    }
    selectedTableOids = selectedTableOids; // Trigger reactivity
    updateSource();
  }

  function handleSchemaMode(mode: SelectionMode) {
    schemaMode = mode;
    updateSource();
  }

  function handleTableMode(mode: SelectionMode) {
    if (mode === "include" && tableMode !== "include") {
      // Save current schema state before switching to include mode
      lastSchemaMode = schemaMode;
      lastSelectedSchemas = new Set(selectedSchemas);

      // Reset schema selection for include mode
      schemaMode = "all";
      selectedSchemas.clear();
      selectedSchemas = selectedSchemas;
    } else if (mode !== "include" && tableMode === "include") {
      // Restore previous schema state when leaving include mode
      schemaMode = lastSchemaMode;
      selectedSchemas = new Set(lastSelectedSchemas);
    }

    tableMode = mode;
    updateSource();
  }

  function handleDbChange(event: any) {
    selectedDatabaseId = event.value;
    const database = databases.find((db) => db.id === event.value);
    onDatabaseSelect(database || null);
    // Reset selections when database changes
    selectedSchemas.clear();
    selectedTableOids.clear();
    selectedSchemas = selectedSchemas;
    selectedTableOids = selectedTableOids;
    updateSource();
  }

  let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";

  function refreshDatabases() {
    databaseRefreshState = "refreshing";
    pushEvent("refresh_databases", {}, () => {
      databaseRefreshState = "done";
      setTimeout(() => {
        databaseRefreshState = "idle";
      }, 2000);
    });
  }

  // Add computed property for schema disabled state
  $: isSchemaSelectionDisabled = tableMode === "include";

  onMount(() => {
    if (databases.length === 1 && !selectedDatabaseId) {
      handleDbChange({ value: databases[0].id });
    }
  });
</script>

<div class="w-full mx-auto space-y-4">
  <div class="flex items-center space-x-2">
    <h2 class="font-medium">Database</h2>
  </div>
  {#if isEditMode}
    <div class="flex items-center space-x-2">
      <DatabaseIcon class="h-5 w-5 text-gray-400" />
      <h2 class="text-lg font-medium">
        {selectedDatabase?.name}
      </h2>
    </div>
  {:else}
    <div class="flex items-center space-x-2">
      <Select
        onSelectedChange={handleDbChange}
        selected={{ value: selectedDatabaseId, label: selectedDatabase?.name }}
      >
        <SelectTrigger>
          <SelectValue placeholder="Select a database" />
        </SelectTrigger>
        <SelectContent>
          {#each databases as db}
            <SelectItem value={db.id}>{db.name}</SelectItem>
          {/each}
        </SelectContent>
      </Select>
      <Tooltip.Root>
        <Tooltip.Trigger>
          <Button
            variant="outline"
            size="sm"
            on:click={refreshDatabases}
            disabled={databaseRefreshState === "refreshing"}
            class="p-2"
            aria-label="Refresh Databases"
          >
            {#if databaseRefreshState === "refreshing"}
              <RotateCwIcon class="h-5 w-5 animate-spin" />
            {:else if databaseRefreshState === "done"}
              <CheckIcon class="h-5 w-5 text-green-500" />
            {:else}
              <RotateCwIcon class="h-5 w-5" />
            {/if}
          </Button>
        </Tooltip.Trigger>
        <Tooltip.Content>
          <p class="text-xs">Refresh databases</p>
        </Tooltip.Content>
      </Tooltip.Root>
    </div>
  {/if}

  {#if selectedDatabaseId}
    <div class="space-y-4">
      <!-- Schema Selection Mode -->
      <div class="space-y-2">
        <div class="flex items-center justify-between">
          <h3 class="font-medium">Schemas</h3>
          <div class="inline-flex rounded-md shadow-sm" role="group">
            <button
              class="text-sm px-4 py-2 transition-all border {isSchemaSelectionDisabled
                ? 'bg-background text-muted-foreground border-input'
                : schemaMode === 'all'
                  ? 'bg-primary text-primary-foreground border-primary'
                  : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} rounded-l-md border-r-0"
              on:click={(e) => {
                e.preventDefault();
                if (!isSchemaSelectionDisabled) handleSchemaMode("all");
              }}
              disabled={isSchemaSelectionDisabled}
              class:opacity-50={isSchemaSelectionDisabled}
            >
              All
            </button>
            <button
              class="text-sm px-4 py-2 transition-all border {isSchemaSelectionDisabled
                ? 'bg-background text-muted-foreground border-input'
                : schemaMode === 'include'
                  ? 'bg-primary text-primary-foreground border-primary'
                  : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} border-r-0"
              on:click={(e) => {
                e.preventDefault();
                if (!isSchemaSelectionDisabled) handleSchemaMode("include");
              }}
              disabled={isSchemaSelectionDisabled}
              class:opacity-50={isSchemaSelectionDisabled}
            >
              Include
            </button>
            <button
              class="text-sm px-4 py-2 transition-all border {isSchemaSelectionDisabled
                ? 'bg-background text-muted-foreground border-input'
                : schemaMode === 'exclude'
                  ? 'bg-primary text-primary-foreground border-primary'
                  : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} rounded-r-md"
              on:click={(e) => {
                e.preventDefault();
                if (!isSchemaSelectionDisabled) handleSchemaMode("exclude");
              }}
              disabled={isSchemaSelectionDisabled}
              class:opacity-50={isSchemaSelectionDisabled}
            >
              Exclude
            </button>
          </div>
        </div>
      </div>

      {#if schemaMode !== "all" && !isSchemaSelectionDisabled}
        <div class="border rounded-lg overflow-hidden">
          <div class="p-2 border-b bg-gray-50">
            <div class="relative">
              <SearchIcon
                class="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4"
              />
              <Input
                type="search"
                placeholder="Search schemas..."
                class="pl-8"
                bind:value={searchSchemaQuery}
              />
            </div>
          </div>
          <div class="max-h-[200px] overflow-y-auto">
            {#if filteredSchemas.length > 0}
              <Table>
                <TableBody>
                  {#each filteredSchemas as schema}
                    <TableRow
                      on:click={() => toggleSchema(schema)}
                      class="cursor-pointer hover:bg-gray-100 {selectedSchemas.has(
                        schema,
                      )
                        ? 'bg-blue-50 hover:bg-blue-100'
                        : ''}"
                    >
                      <TableCell class="flex items-center space-x-2">
                        {#if selectedSchemas.has(schema) && schemaMode === "include"}
                          <CheckIcon class="h-4 w-4 text-green-500" />
                        {:else if selectedSchemas.has(schema) && schemaMode === "exclude"}
                          <XIcon class="h-4 w-4 text-red-500" />
                        {:else}
                          <DatabaseIcon class="h-4 w-4 text-gray-400" />
                        {/if}
                        <span>{schema}</span>
                      </TableCell>
                    </TableRow>
                  {/each}
                </TableBody>
              </Table>
            {:else}
              <div class="p-8 text-center text-gray-500">
                <p>
                  No schemas found. Try modifying your database or schema
                  search.
                </p>
              </div>
            {/if}
          </div>
        </div>
        {#if errors?.include_schemas}
          <p class="text-destructive text-sm">
            {errors.include_schemas}
          </p>
        {/if}
        {#if errors?.exclude_schemas}
          <p class="text-destructive text-sm">
            {errors.exclude_schemas}
          </p>
        {/if}
      {/if}

      <!-- Table Selection Mode -->
      <div class="space-y-2">
        <div class="flex items-center justify-between">
          <h3 class="font-medium">Tables</h3>
          <div class="inline-flex rounded-md shadow-sm" role="group">
            <button
              class="text-sm px-4 py-2 transition-all border {tableMode ===
              'all'
                ? 'bg-primary text-primary-foreground border-primary'
                : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} rounded-l-md border-r-0"
              on:click={(e) => {
                e.preventDefault();
                handleTableMode("all");
              }}
            >
              All
            </button>
            <button
              class="text-sm px-4 py-2 transition-all border {tableMode ===
              'include'
                ? 'bg-primary text-primary-foreground border-primary'
                : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} border-r-0"
              on:click={(e) => {
                e.preventDefault();
                handleTableMode("include");
              }}
            >
              Include
            </button>
            <button
              class="text-sm px-4 py-2 transition-all border {tableMode ===
              'exclude'
                ? 'bg-primary text-primary-foreground border-primary'
                : 'bg-background text-muted-foreground border-input hover:bg-accent hover:text-accent-foreground'} rounded-r-md"
              on:click={(e) => {
                e.preventDefault();
                handleTableMode("exclude");
              }}
            >
              Exclude
            </button>
          </div>
        </div>
      </div>

      {#if tableMode !== "all"}
        <div class="border rounded-lg overflow-hidden">
          <div class="p-2 border-b bg-gray-50">
            <div class="relative">
              <SearchIcon
                class="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4"
              />
              <Input
                type="search"
                placeholder="Search tables..."
                class="pl-8"
                bind:value={searchTableQuery}
              />
            </div>
          </div>
          <div class="max-h-[200px] overflow-y-auto">
            {#if filteredTables.length > 0}
              <Table>
                <TableBody>
                  {#each filteredTables as table}
                    <TableRow
                      on:click={() => toggleTable(table)}
                      class="cursor-pointer hover:bg-gray-100 {selectedTableOids.has(
                        table.oid,
                      )
                        ? 'bg-blue-50 hover:bg-blue-100'
                        : ''}"
                    >
                      <TableCell class="flex items-center space-x-2">
                        {#if selectedTableOids.has(table.oid) && tableMode === "include"}
                          <CheckIcon class="h-4 w-4 text-green-500" />
                        {:else if selectedTableOids.has(table.oid) && tableMode === "exclude"}
                          <XIcon class="h-4 w-4 text-red-500" />
                        {:else}
                          <TableIcon class="h-4 w-4 text-gray-400" />
                        {/if}
                        <span>{table.schema}.{table.name}</span>
                      </TableCell>
                    </TableRow>
                  {/each}
                </TableBody>
              </Table>
            {:else}
              <div class="p-8 text-center text-gray-500">
                <p>
                  No tables found. Try modifying your database, table search, or
                  schema selection.
                </p>
              </div>
            {/if}
          </div>
        </div>
        {#if errors?.include_table_oids}
          <p class="text-destructive text-sm">
            {errors.include_table_oids}
          </p>
        {/if}
        {#if errors?.exclude_table_oids}
          <p class="text-destructive text-sm">
            {errors.exclude_table_oids}
          </p>
        {/if}
      {/if}
    </div>
  {/if}
  <div class="space-y-2">
    <h2 class="font-medium">Summary</h2>
    <p class="text-sm text-muted-foreground">{summaryText}</p>
  </div>
</div>
