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
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import {
    TableIcon,
    SearchIcon,
    RefreshCwIcon,
    CheckIcon,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";

  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<{
      oid: number;
      schema: string;
      name: string;
    }>;
  }>;
  export let onSelect: (event: {
    databaseId: string;
    tableOid: number;
  }) => void;
  export let pushEvent: (
    event: string,
    detail: any,
    callback: () => void
  ) => void;
  export let selectedDatabaseId: string | undefined;
  export let selectedTableOid: number | null;

  let selectedDatabase;

  let filteredTables: Array<{ oid: number; schema: string; name: string }> = [];
  let searchQuery = "";

  $: {
    if (selectedDatabaseId) {
      selectedDatabase = databases.find((db) => db.id === selectedDatabaseId);
    }
    filteredTables =
      selectedDatabase?.tables.filter((table) =>
        `${table.schema}.${table.name}`
          .toLowerCase()
          .includes(searchQuery.toLowerCase())
      ) || [];
  }

  let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";
  let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  $: console.log(databaseRefreshState, tableRefreshState);

  function handleDbChange(event: any) {
    selectedDatabaseId = event.value;
  }

  function refreshDatabases() {
    console.log("refreshDBs");
    console.log("refreshDatabases");
    databaseRefreshState = "refreshing";
    pushEvent("refresh_databases", {}, () => {
      databaseRefreshState = "done";
      setTimeout(() => {
        databaseRefreshState = "idle";
      }, 2000);
    });
  }

  function refreshTables() {
    if (selectedDatabaseId) {
      tableRefreshState = "refreshing";
      pushEvent("refresh_tables", { database_id: selectedDatabaseId }, () => {
        tableRefreshState = "done";
        setTimeout(() => {
          tableRefreshState = "idle";
        }, 2000);
      });
    }
  }

  function handleTableSelect(table: {
    oid: number;
    schema: string;
    name: string;
  }) {
    onSelect({
      databaseId: selectedDatabaseId,
      tableOid: table.oid,
    });
  }
</script>

<div class="w-full mx-auto space-y-4">
  <div class="flex items-center space-x-2">
    <Select onSelectedChange={handleDbChange}>
      <SelectTrigger>
        <SelectValue placeholder="Select a database" />
      </SelectTrigger>
      <SelectContent>
        {#each databases as db}
          <SelectItem value={db.id}>{db.name}</SelectItem>
        {/each}
      </SelectContent>
    </Select>
    <Button
      variant="outline"
      size="sm"
      on:click={refreshDatabases}
      disabled={databaseRefreshState === "refreshing"}
    >
      {#if databaseRefreshState === "refreshing"}
        <RefreshCwIcon class="h-4 w-4 mr-2 animate-spin" />
      {:else if databaseRefreshState === "done"}
        <CheckIcon class="h-4 w-4 mr-2 text-green-500" />
      {:else}
        <RefreshCwIcon class="h-4 w-4 mr-2" />
      {/if}
      Refresh
    </Button>
  </div>

  {#if selectedDatabaseId}
    <div class="border rounded-lg p-4 space-y-4">
      <div class="flex items-center justify-between">
        <h2 class="text-lg font-semibold">
          {selectedDatabase.name}
        </h2>
        <div class="flex items-center space-x-2">
          <div class="relative">
            <SearchIcon
              class="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4"
            />
            <Input
              type="search"
              placeholder="Search tables..."
              class="pl-8"
              bind:value={searchQuery}
            />
          </div>
          <Button
            variant="outline"
            size="sm"
            on:click={refreshTables}
            disabled={tableRefreshState === "refreshing"}
          >
            {#if tableRefreshState === "refreshing"}
              <RefreshCwIcon class="h-4 w-4 mr-2 animate-spin" />
            {:else if tableRefreshState === "done"}
              <CheckIcon class="h-4 w-4 mr-2 text-green-500" />
            {:else}
              <RefreshCwIcon class="h-4 w-4 mr-2" />
            {/if}
            Refresh
          </Button>
        </div>
      </div>

      <div class="border rounded-lg overflow-hidden">
        <div class="max-h-[400px] overflow-y-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Table Name</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {#each filteredTables as table}
                <TableRow
                  on:click={() => handleTableSelect(table)}
                  class="cursor-pointer {table.oid === selectedTableOid
                    ? 'bg-blue-50 hover:bg-blue-100'
                    : 'hover:bg-gray-100'}"
                >
                  <TableCell class="flex items-center space-x-2">
                    <TableIcon class="h-4 w-4 text-gray-400" />
                    <span>{table.schema}.{table.name}</span>
                  </TableCell>
                </TableRow>
              {/each}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  {/if}
</div>
