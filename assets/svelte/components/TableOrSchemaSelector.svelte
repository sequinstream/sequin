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
    RotateCwIcon,
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
  import * as RadioGroup from "$lib/components/ui/radio-group";

  type Table = {
    oid: number;
    schema: string;
    name: string;
    isEventTable?: boolean;
  };

  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<Table>;
    schemas: Array<string>;
  }>;
  export let onSelect: (event: {
    databaseId: string;
    tableOid: number;
    schema: string;
  }) => void;
  export let pushEvent: (
    event: string,
    detail: any,
    callback: () => void,
  ) => void;
  export let selectedDatabaseId: string | undefined;
  export let selectedTableOid: number | null;
  export let selectedSchema: string | null;
  export let onlyEventTables: boolean = false;
  export let excludeEventTables: boolean = false;

  let selectedDatabase;
  let autoRefreshedDatabaseTables = [];
  let selectionMode: "table" | "schema" = "schema";

  let filteredTables: Array<Table> = [];
  let filteredSchemas: Array<string> = [];
  let searchQuery = "";

  let lastSelectedTableOid: number | null = null;
  let lastSelectedSchema: string | null = null;

  $: {
    if (selectedDatabaseId) {
      selectedDatabase = databases.find((db) => db.id === selectedDatabaseId);
    }
    filteredTables =
      selectedDatabase?.tables.filter((table) =>
        `${table.schema}.${table.name}`
          .toLowerCase()
          .includes((searchQuery || "").toLowerCase()),
      ) || [];

    if (onlyEventTables) {
      filteredTables = filteredTables.filter((table) => table.isEventTable);
    } else if (excludeEventTables) {
      filteredTables = filteredTables.filter((table) => !table.isEventTable);
    }

    filteredSchemas =
      selectedDatabase?.schemas.filter((schema) =>
        schema.toLowerCase().includes((searchQuery || "").toLowerCase()),
      ) || [];
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

  $: {
    if (selectionMode === "schema") {
      onSelect({
        databaseId: selectedDatabaseId,
        tableOid: null,
        schema: lastSelectedSchema,
      });
    } else if (selectionMode === "table") {
      onSelect({
        databaseId: selectedDatabaseId,
        tableOid: lastSelectedTableOid,
        schema: null,
      });
    }
  }

  let databaseRefreshState: "idle" | "refreshing" | "done" = "idle";
  let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  function handleDbChange(event: any) {
    selectedDatabaseId = event.value;
    onSelect({ databaseId: event.value, tableOid: null, schema: null });
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

  onMount(() => {
    if (databases.length === 1 && !selectedDatabaseId) {
      handleDbChange({ value: databases[0].id });
    }

    if (selectedDatabaseId) {
      selectedDatabase = databases.find((db) => db.id === selectedDatabaseId);
    }
    if (selectedDatabase && selectedDatabase.tables.length === 1) {
      onSelect({
        databaseId: selectedDatabaseId,
        tableOid: selectedDatabase.tables[0].oid,
        schema: null,
      });
    }
  });

  let createEventTableDialogOpen = false;
  let destinationTableName = "sequin_events";
  let destinationSchemaName = "public";
  let retentionDays = 30;
  let retentionPolicy = "none";
  let createTableDDL = "";

  function openCreateEventTableDialog() {
    createEventTableDialogOpen = true;
  }

  $: {
    createTableDDL =
      retentionPolicy === "pg_partman"
        ? `create table ${destinationSchemaName}.${destinationTableName} (
  id bigserial,
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
  inserted_at timestamp with time zone not null default now(),
  primary key (id, committed_at)
) partition by range (committed_at);`
        : `create table ${destinationSchemaName}.${destinationTableName} (
  id bigserial primary key,
  seq bigint not null,
  source_database_id uuid not null,
  source_table_oid bigint not null,
  source_table_schema text not null,
  source_table_name text not null,
  record_pk text not null,
  record jsonb not null,
  changes jsonb,
  action text not null,
  transaction_annotations jsonb,
  committed_at timestamp with time zone not null,
  inserted_at timestamp with time zone not null default now()
);`;

    createTableDDL =
      createTableDDL +
      `\n\ncreate unique index on ${destinationSchemaName}.${destinationTableName} (source_database_id, committed_at, seq, record_pk);
create index on ${destinationSchemaName}.${destinationTableName} (seq);
create index on ${destinationSchemaName}.${destinationTableName} (source_table_oid);
create index on ${destinationSchemaName}.${destinationTableName} (committed_at);

-- important comment to identify this table as a sequin events table
comment on table ${destinationSchemaName}.${destinationTableName} is '$sequin-events$';
`;
  }

  let retentionPolicyPgCronDDL = `
-- create required extension
-- you need to run this in the database that pg_cron is installed in
-- (usually in \`postgres\`)
create extension if not exists pg_cron;

-- setup cron job to run every 10 minutes and delete old data
select cron.schedule('retention_policy_10min', '*/10 * * * *', $$
  delete from ${destinationSchemaName}.${destinationTableName}
  where committed_at < now() - interval '${retentionDays} days';
$$);
`;

  $: retentionPolicyPgPartmanDDL = `
-- You'll need to setup pg_partman in your database, including adding this to postgresql.conf:
-- shared_preload_libraries = 'pg_partman_bgw'     # (change requires restart)

-- create required extensions
create schema partman;
create extension pg_partman schema partman;

-- set up pg_partman for time-based partitioning
select partman.create_parent(
  p_parent_table => '${destinationSchemaName}.${destinationTableName}',
  p_template_table => '${destinationSchemaName}.${destinationTableName}',
  p_control => 'committed_at',
  p_interval => '1 day',
  p_automatic_maintenance => 'on',
  p_default_table := false
);

-- set up retention policy
update partman.part_config
set
  retention = '${retentionDays} days',
  retention_keep_table = false,
  infinite_time_partitions = true
where parent_table = '${destinationSchemaName}.${destinationTableName}';
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
    <a href="/databases/new" target="_blank">
      <Button variant="outline" size="sm">
        <Plus class="h-4 w-4 mr-2" />
        Connect new database
      </Button>
    </a>
  </div>

  {#if selectedDatabaseId}
    <div class="flex flex-col space-y-2 text-sm text-gray-600">
      <p>
        Select either a <b>schema</b> to receive messages from <i>all tables</i>
        in that schema, or a <b>table</b> to receive messages from
        <i>only that table</i>.
      </p>
    </div>
    <div class="border rounded-lg p-4 space-y-4">
      <div class="flex items-center justify-between">
        <div class="flex items-center space-x-2">
          <DatabaseIcon class="h-5 w-5 text-gray-400" />
          <h2 class="text-lg font-semibold">
            {selectedDatabase.name}
          </h2>
        </div>
        <div class="flex items-center space-x-2">
          <div class="inline-flex rounded-md shadow-sm" role="group">
            <Button
              variant={selectionMode === "schema" ? "default" : "outline"}
              class="rounded-r-none border-r-0"
              on:click={() => {
                selectionMode = "schema";
              }}
            >
              Schemas
            </Button>
            <Button
              variant={selectionMode === "table" ? "default" : "outline"}
              class="rounded-l-none"
              on:click={() => {
                selectionMode = "table";
              }}
            >
              Tables
            </Button>
          </div>
          <div class="relative">
            <SearchIcon
              class="absolute left-2 top-1/2 transform -translate-y-1/2 text-gray-400 h-4 w-4"
            />
            <Input
              type="search"
              placeholder="Search {selectionMode === 'table'
                ? 'tables'
                : 'schemas'}..."
              class="pl-8"
              bind:value={searchQuery}
            />
          </div>
          <Tooltip.Root>
            <Tooltip.Trigger>
              <Button
                variant="outline"
                size="sm"
                on:click={refreshTables}
                disabled={tableRefreshState === "refreshing"}
                class="p-2"
                aria-label="Refresh Tables"
              >
                {#if tableRefreshState === "refreshing"}
                  <RotateCwIcon class="h-5 w-5 animate-spin" />
                {:else if tableRefreshState === "done"}
                  <CheckIcon class="h-5 w-5 text-green-500" />
                {:else}
                  <RotateCwIcon class="h-5 w-5" />
                {/if}
              </Button>
            </Tooltip.Trigger>
            <Tooltip.Content>
              <p class="text-xs">Refresh tables</p>
            </Tooltip.Content>
          </Tooltip.Root>
        </div>
      </div>

      <div class="border rounded-lg overflow-hidden">
        <div class="max-h-[400px] overflow-y-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>
                  {#if selectionMode === "schema"}
                    Schemas
                  {:else if onlyEventTables}
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
              {#if selectionMode === "schema"}
                {#each filteredSchemas as schema}
                  <TableRow
                    on:click={() => {
                      lastSelectedSchema = schema;
                    }}
                    class="cursor-pointer hover:bg-gray-100 {selectedSchema ===
                    schema
                      ? 'bg-blue-50 hover:bg-blue-100'
                      : ''}"
                  >
                    <TableCell class="flex items-center space-x-2">
                      {#if selectedSchema === schema}
                        <CheckIcon class="h-4 w-4 text-green-500" />
                      {:else}
                        <DatabaseIcon class="h-4 w-4 text-gray-400" />
                      {/if}
                      <span>{schema}</span>
                    </TableCell>
                  </TableRow>
                {/each}
              {:else if filteredTables.length === 0 && onlyEventTables}
                <TableRow>
                  <TableCell colspan={2} class="text-center py-8">
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
                    on:click={() => {
                      lastSelectedTableOid = table.oid;
                    }}
                    class="cursor-pointer {table.oid === selectedTableOid
                      ? 'bg-blue-50 hover:bg-blue-100'
                      : 'hover:bg-gray-100'}"
                  >
                    <TableCell class="flex items-center space-x-2">
                      {#if table.oid === selectedTableOid}
                        <CheckIcon class="h-4 w-4 text-green-500" />
                      {:else if onlyEventTables}
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

<Dialog.Root bind:open={createEventTableDialogOpen}>
  <Dialog.Content class="w-4/5 max-w-[80%] flex flex-col space-y-4">
    <Dialog.Header>
      <Dialog.Title>Create event table</Dialog.Title>
      <Dialog.Description>
        Create an event table to setup Change Retention. For more information,<a
          href="https://sequinstream.com/docs/capture-changes/wal-pipelines"
          target="_blank"
          class="text-blue-500 underline">see the docs ↗</a
        >.
      </Dialog.Description>
    </Dialog.Header>
    <div class="flex flex-col gap-4 flex-grow overflow-y-auto mt-4">
      <div class="flex flex-col gap-2">
        <Label for="destinationSchemaName">Schema name</Label>
        <Input id="destinationSchemaName" bind:value={destinationSchemaName} />
      </div>

      <div class="flex flex-col gap-2">
        <Label for="destinationTableName">Table name</Label>
        <Input id="destinationTableName" bind:value={destinationTableName} />
      </div>

      <div class="flex flex-col gap-4">
        <Label>Retention Policy</Label>
        <p class="text-sm text-gray-500">
          If you want to enforce a retention policy, you can use <code
            >pg_cron</code
          >
          or
          <code>pg_partman</code> to do so. We've benched a <code>pg_cron</code>
          solution on a 16 CPU/64GB machine at 100M writes/day (≈1K writes/sec) and
          it's worked just fine. But for very high volumes, you might consider using
          <code>pg_partman</code>. You can change this in the future.
        </p>
        <RadioGroup.Root
          bind:value={retentionPolicy}
          class="flex flex-col space-y-1 cursor-pointer"
        >
          <div class="flex items-center space-x-2">
            <RadioGroup.Item value="none" id="none" />
            <Label for="none">None</Label>
          </div>
          <div class="flex items-center space-x-2">
            <RadioGroup.Item value="pg_cron" id="pg_cron" />
            <Label for="pg_cron">pg_cron</Label>
          </div>
          <div class="flex items-center space-x-2">
            <RadioGroup.Item value="pg_partman" id="pg_partman" />
            <Label for="pg_partman">pg_partman (&gt;10M writes/day)</Label>
          </div>
        </RadioGroup.Root>
      </div>

      {#if retentionPolicy !== "none"}
        <div class="flex flex-col gap-4">
          <Label for="retentionDays">Retention period (days)</Label>
          <Input type="number" id="retentionDays" bind:value={retentionDays} />
        </div>
      {/if}

      {#if destinationTableName && destinationSchemaName}
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
        {#if retentionPolicy !== "none"}
          <div class="flex flex-col gap-4">
            <p><strong>Retention Policy</strong></p>
            <CodeWithCopy
              language="sql"
              code={retentionPolicy === "pg_cron"
                ? retentionPolicyPgCronDDL
                : retentionPolicyPgPartmanDDL}
              copyIconPosition="top"
            />
          </div>
        {/if}
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
