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
    Logs,
    Info,
    Plus,
  } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { onMount } from "svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { Badge } from "$lib/components/ui/badge";
  import * as Dialog from "$lib/components/ui/dialog";
  import CodeWithCopy from "./CodeWithCopy.svelte";
  import { Label } from "$lib/components/ui/label";
  import * as Tabs from "$lib/components/ui/tabs";

  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<{
      oid: number;
      schema: string;
      name: string;
      isEventTable?: boolean;
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
  export let onlyEventTables: boolean = false;
  export let excludeEventTables: boolean = false;

  let selectedDatabase;
  let autoRefreshedDatabaseTables = [];

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
          .includes((searchQuery || "").toLowerCase())
      ) || [];

    if (onlyEventTables) {
      filteredTables = filteredTables.filter((table) => table.isEventTable);
    } else if (excludeEventTables) {
      filteredTables = filteredTables.filter((table) => !table.isEventTable);
    }
  }

  $: {
    if (
      selectedDatabase &&
      selectedDatabase.tables.length === 0 &&
      !autoRefreshedDatabaseTables.includes(selectedDatabaseId)
    ) {
      autoRefreshedDatabaseTables.push(selectedDatabaseId);
      refreshTables();
    }
  }

  let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";
  let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  function handleDbChange(event: any) {
    selectedDatabaseId = event.value;
    onSelect({ databaseId: event.value, tableOid: null });
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

  onMount(() => {
    if (databases.length === 1 && !selectedDatabaseId) {
      handleDbChange({ value: databases[0].id });
    }
  });

  let createEventTableDialogOpen = false;
  let destinationTableName = "sequin_events";
  let retentionDays = 30;

  function openCreateEventTableDialog() {
    createEventTableDialogOpen = true;
  }

  $: createTableDDL = `
create table ${destinationTableName} (
  id serial primary key,
  seq bigint not null,
  source_database_id uuid not null,
  source_table_oid bigint not null,
  source_table_schema text not null,
  source_table_name text not null,
  record_pk text not null,
  record jsonb not null,
  changes jsonb,
  action text not null,
  committed_at timestamp with time zone not null,
  inserted_at timestamp with time zone not null default now()
);

create unique index on ${destinationTableName} (source_database_id, seq, record_pk);
create index on ${destinationTableName} (seq);
create index on ${destinationTableName} (source_table_oid);
create index on ${destinationTableName} (committed_at);

-- important comment to identify this table as a sequin events table
comment on table ${destinationTableName} is '$sequin-events$';
`;

  let retentionPolicyPgCronDDL = `
-- create required extension
create extension if not exists pg_cron;

-- setup cron job to run every 10 minutes and delete old data
select cron.schedule('retention_policy_10min', '*/10 * * * *', $$
  delete from ${destinationTableName}
  where committed_at < now() - interval '${retentionDays} days';
$$);
`;

  $: retentionPolicyPgPartmanDDL = `
-- create required extensions
create extension if not exists pg_partman;
create extension if not exists pg_cron;

-- set up pg_partman for time-based partitioning
select partman.create_parent(
  p_parent_table => '${destinationTableName}',
  p_control => 'committed_at',
  p_type => 'native',
  p_interval => 'daily',
  p_premake => 30
);

-- set up retention policy
select partman.add_to_part_config(
  p_parent_table => '${destinationTableName}',
  p_retention => '${retentionDays} days',
  p_retention_keep_table => false
);

-- setup cron job to run maintenance for pg_partman every hour
-- this is necessary to clean up old partitions (i.e. drop stale data)
select cron.schedule('partman_maintenance', '0 * * * *', $$
  select partman.run_maintenance(p_analyze := false);
$$);
`;
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
                <TableHead>
                  {#if onlyEventTables}
                    <div class="flex items-center justify-between gap-2">
                      Event tables
                      <Tooltip.Root>
                        <Tooltip.Trigger>
                          <Badge variant="default">
                            <Info class="mr-1 h-4 w-4" />
                            Filtered
                          </Badge>
                        </Tooltip.Trigger>
                        <Tooltip.Content>
                          <p class="text-xs">
                            Filtered down to source tables that look like Sequin
                            events tables.
                          </p>
                        </Tooltip.Content>
                      </Tooltip.Root>
                    </div>
                  {:else if excludeEventTables}
                    <div class="flex items-center justify-between gap-2">
                      Non-event tables
                      <Tooltip.Root>
                        <Tooltip.Trigger>
                          <Badge variant="default">
                            <Info class="mr-1 h-4 w-4" />
                            Filtered
                          </Badge>
                        </Tooltip.Trigger>
                        <Tooltip.Content>
                          <p class="text-xs">
                            Filtered to exclude tables that look like Sequin
                            events tables.
                          </p>
                        </Tooltip.Content>
                      </Tooltip.Root>
                    </div>
                  {:else}
                    Tables
                  {/if}
                </TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {#if filteredTables.length === 0 && onlyEventTables}
                <TableRow>
                  <TableCell colspan="2" class="text-center py-8">
                    <p class="text-gray-500 mb-4">
                      No event tables found in the destination.
                    </p>
                    <Button on:click={openCreateEventTableDialog}>
                      <Plus class="mr-2 h-4 w-4" /> Create event table
                    </Button>
                  </TableCell>
                </TableRow>
              {:else}
                {#each filteredTables as table}
                  <TableRow
                    on:click={() => handleTableSelect(table)}
                    class="cursor-pointer {table.oid === selectedTableOid
                      ? 'bg-blue-50 hover:bg-blue-100'
                      : 'hover:bg-gray-100'}"
                  >
                    <TableCell class="flex items-center space-x-2">
                      {#if onlyEventTables}
                        <Logs class="h-4 w-4 text-gray-400" />
                      {:else}
                        <TableIcon class="h-4 w-4 text-gray-400" />
                      {/if}
                      <span>{table.schema}.{table.name}</span>
                    </TableCell>
                  </TableRow>
                {/each}
                {#if onlyEventTables}
                  <TableRow
                    on:click={openCreateEventTableDialog}
                    class="cursor-pointer bg-gray-50 hover:bg-gray-100"
                  >
                    <TableCell class="flex items-center space-x-2">
                      <Plus class="h-4 w-4 text-gray-400" />
                      <span class="text-gray-600">Create new event table</span>
                    </TableCell>
                  </TableRow>
                {/if}
              {/if}
            </TableBody>
          </Table>
        </div>
      </div>
    </div>
  {/if}
</div>

<Dialog.Root
  bind:open={createEventTableDialogOpen}
  id="create-event-table-dialog"
>
  <Dialog.Content class="w-4/5 max-w-[80%] flex flex-col">
    <Dialog.Header>
      <Dialog.Title>Create Event Table</Dialog.Title>
      <Dialog.Description>
        Set up your event table with the correct schema and retention policy.
      </Dialog.Description>
    </Dialog.Header>
    <div class="flex flex-col gap-4 flex-grow overflow-y-auto">
      <div class="flex flex-col gap-2">
        <Label for="destinationTableName">Desired table name</Label>
        <Input id="destinationTableName" bind:value={destinationTableName} />
      </div>

      <div class="flex flex-col gap-4">
        <Label for="retentionDays">Desired retention period (days)</Label>
        <Input type="number" id="retentionDays" bind:value={retentionDays} />
      </div>

      {#if destinationTableName}
        <div class="flex flex-col gap-4">
          <p><strong>Create your event table</strong></p>
          <p>
            Create an event table with the following DDL, including indexes:
          </p>
          <CodeWithCopy
            language="sql"
            code={createTableDDL}
            copyIconPosition="top"
          />
        </div>
        <div class="flex flex-col gap-4">
          <p><strong>Create your retention policy</strong></p>
          <p>
            We recommend either the <code>pg_cron</code> or
            <code>pg_partman</code>
            extensions for enforcing a retention policy. <code>pg_cron</code> is
            fine if your change volume will rarely exceed 10 writes/sec or 1M
            writes/day. For higher volumes, we recommend
            <code>pg_partman</code>:
          </p>
          <Tabs.Root value="pg_cron" class="w-full">
            <Tabs.List>
              <Tabs.Trigger value="pg_cron">pg_cron</Tabs.Trigger>
              <Tabs.Trigger value="pg_partman">pg_partman</Tabs.Trigger>
            </Tabs.List>
            <Tabs.Content value="pg_cron">
              <CodeWithCopy
                language="sql"
                code={retentionPolicyPgCronDDL}
                copyIconPosition="top"
              />
            </Tabs.Content>
            <Tabs.Content value="pg_partman">
              <CodeWithCopy
                language="sql"
                code={retentionPolicyPgPartmanDDL}
                copyIconPosition="top"
              />
            </Tabs.Content>
          </Tabs.Root>
        </div>
      {/if}
    </div>
    <Dialog.Footer class="py-4 sticky bottom-0 bg-background">
      <Button
        variant="outline"
        on:click={() => (createEventTableDialogOpen = false)}>Cancel</Button
      >
      <Button
        on:click={() => {
          createEventTableDialogOpen = false;
          refreshTables();
        }}>Done</Button
      >
    </Dialog.Footer>
  </Dialog.Content>
</Dialog.Root>
