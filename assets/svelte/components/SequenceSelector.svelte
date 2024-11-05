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
    DatabaseIcon,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { onMount } from "svelte";

  export let databases: Array<{
    id: string;
    name: string;
    sequences: Array<{
      id: string;
      table_oid: number;
      table_schema: string;
      table_name: string;
      sort_column_name: string;
    }>;
  }>;
  export let onSelect: (event: {
    databaseId: string;
    sequenceId: string;
  }) => void;
  export let pushEvent: (
    event: string,
    detail: any,
    callback: () => void,
  ) => void;
  export let selectedDatabaseId: string | undefined;
  export let selectedSequenceId: string | null;

  let selectedDatabase;
  let autoRefreshedDatabaseSequences = [];

  let filteredSequences: Array<{
    id: string;
    table_oid: number;
    table_schema: string;
    table_name: string;
    sort_column_name: string;
  }> = [];
  let searchQuery = "";

  $: {
    if (selectedDatabaseId) {
      selectedDatabase = databases.find((db) => db.id === selectedDatabaseId);
    }
    filteredSequences =
      selectedDatabase?.sequences.filter((sequence) =>
        `${sequence.table_schema}.${sequence.table_name}`
          .toLowerCase()
          .includes((searchQuery || "").toLowerCase()),
      ) || [];
  }

  $: {
    if (
      selectedDatabase &&
      selectedDatabase.sequences.length === 0 &&
      !autoRefreshedDatabaseSequences.includes(selectedDatabaseId)
    ) {
      autoRefreshedDatabaseSequences.push(selectedDatabaseId);
      refreshSequences();
    }
  }

  let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";
  let sequenceRefreshState: "idle" | "refreshing" | "done" = "idle";

  function handleDbChange(event: any) {
    selectedDatabaseId = event.value;
    onSelect({ databaseId: event.value, sequenceId: null });
  }

  function refreshDatabases() {
    databaseRefreshState = "refreshing";
    pushEvent("refresh_databases", {}, () => {
      databaseRefreshState = "done";
      setTimeout(() => {
        databaseRefreshState = "idle";
      }, 2000);
    });
  }

  function refreshSequences() {
    if (selectedDatabaseId) {
      sequenceRefreshState = "refreshing";
      pushEvent(
        "refresh_sequences",
        { database_id: selectedDatabaseId },
        () => {
          sequenceRefreshState = "done";
          setTimeout(() => {
            sequenceRefreshState = "idle";
          }, 2000);
        },
      );
    }
  }

  function handleSequenceSelect(sequence: {
    id: string;
    table_oid: number;
    table_schema: string;
    table_name: string;
    sort_column_name: string;
  }) {
    onSelect({
      databaseId: selectedDatabaseId,
      sequenceId: sequence.id,
    });
  }

  onMount(() => {
    if (databases.length === 1 && !selectedDatabaseId) {
      handleDbChange({ value: databases[0].id });
    }
  });
</script>

<div class="w-full mx-auto space-y-4">
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
        <div class="flex items-center space-x-2">
          <DatabaseIcon class="h-5 w-5 text-gray-400" />
          <h2 class="text-lg font-semibold">
            {selectedDatabase.name}
          </h2>
        </div>
        <div class="flex items-center space-x-2">
          <div class="relative">
            <SearchIcon
              class="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4"
            />
            <Input
              type="search"
              placeholder="Search streams..."
              class="pl-8"
              bind:value={searchQuery}
            />
          </div>
          <Button
            variant="outline"
            size="sm"
            on:click={refreshSequences}
            disabled={sequenceRefreshState === "refreshing"}
          >
            {#if sequenceRefreshState === "refreshing"}
              <RefreshCwIcon class="h-4 w-4 mr-2 animate-spin" />
            {:else if sequenceRefreshState === "done"}
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
                <TableHead>Streams</TableHead>
                <TableHead>Sort Column</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {#if filteredSequences.length === 0}
                <TableRow>
                  <TableCell colspan={2} class="text-center py-8">
                    <p class="text-gray-500">
                      No streams found in the database.
                    </p>
                  </TableCell>
                </TableRow>
              {:else}
                {#each filteredSequences as sequence}
                  <TableRow
                    on:click={() => handleSequenceSelect(sequence)}
                    class="cursor-pointer {sequence.id === selectedSequenceId
                      ? 'bg-blue-50 hover:bg-blue-100'
                      : 'hover:bg-gray-100'}"
                  >
                    <TableCell class="flex items-center space-x-2">
                      <TableIcon class="h-4 w-4 text-gray-400" />
                      <span>{sequence.table_schema}.{sequence.table_name}</span>
                    </TableCell>
                    <TableCell>{sequence.sort_column_name}</TableCell>
                  </TableRow>
                {/each}
              {/if}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  {/if}
</div>
