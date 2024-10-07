<script lang="ts">
  import { onMount } from "svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import * as RadioGroup from "$lib/components/ui/radio-group";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Checkbox } from "$lib/components/ui/checkbox";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import TableSelector from "../components/TableSelector.svelte";
  import CodeWithCopy from "../components/CodeWithCopy.svelte";
  import SortAndFilterCard from "../components/SortAndFilterCard.svelte";
  import FullPageModal from "../components/FullPageModal.svelte";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";

  export let walProjection: any;
  export let databases: any[];
  export let errors: any = {};
  export let parent: string;
  export let isEdit: boolean;
  export let live;

  const pushEvent = (event, payload = {}, cb = (event: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  let form = {
    name: walProjection.name || "",
    status: walProjection.status || "active",
    sourceDatabaseId: walProjection.sourceDatabaseId || "",
    destinationDatabaseId: walProjection.destinationDatabaseId || "",
    sourceTableOid: walProjection.sourceTables?.[0]?.oid || "",
    destinationTableOid: walProjection.destinationOid || "",
    sourceTableActions: walProjection.sourceTables?.[0]?.actions || [
      "insert",
      "update",
      "delete",
    ],
    sourceTableFilters: walProjection.sourceTables?.[0]?.columnFilters || [],
  };

  let selectedSourceDatabase: any;
  let selectedDestinationDatabase: any;
  let selectedSourceTable: any;
  let selectedDestinationTable: any;
  let destinationTableName = "my_events";
  let retentionDays = 30;

  $: {
    if (form.sourceDatabaseId) {
      selectedSourceDatabase = databases.find(
        (db) => db.id === form.sourceDatabaseId
      );
    }
    if (form.destinationDatabaseId) {
      selectedDestinationDatabase = databases.find(
        (db) => db.id === form.destinationDatabaseId
      );
    }
  }

  function handleSubmit() {
    pushEvent("save", { wal_projection: form });
  }

  $: pushEvent("validate", { wal_projection: form });

  $: createTableDDL = `
create table ${destinationTableName ? destinationTableName : "my_events"} (
  id serial primary key,
  seq bigint not null,
  source_database_id uuid not null,
  source_table_oid bigint not null,
  record_pk text not null,
  record jsonb not null,
  changes jsonb,
  action text not null,
  committed_at timestamp with time zone not null,
  inserted_at timestamp with time zone not null default now()
);

create unique index on ${destinationTableName} (seq, source_database_id);
create index on ${destinationTableName} (seq);
create index on ${destinationTableName} (source_table_oid);
create index on ${destinationTableName} (committed_at);
`;

  $: retentionPolicyDDL = `
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

  let dialogOpen = true;
  let showConfirmDialog = false;

  function handleClose() {
    pushEvent("form_closed");
  }
</script>

<FullPageModal
  title="WAL Projection"
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form
    on:submit|preventDefault={handleSubmit}
    class="space-y-6 max-w-3xl mx-auto mt-6"
  >
    <Card>
      <CardHeader></CardHeader>
      <CardContent>
        <p class="mb-4 text-secondary-foreground text-sm" class:hidden={isEdit}>
          With a WAL Projection, you can capture every insert, update, or delete
          that happens to one or more tables into another table in your
          database. Then, you can stream these events with Sequin.
        </p>

        <div class="space-y-4">
          <Label>Source table</Label>
          <TableSelector
            {pushEvent}
            {databases}
            onSelect={({ databaseId, tableOid }) => {
              form.sourceDatabaseId = databaseId;
              form.sourceTableOid = tableOid;
            }}
            selectedDatabaseId={form.sourceDatabaseId}
            selectedTableOid={form.sourceTableOid}
          />

          {#if selectedSourceTable}
            <SortAndFilterCard
              showCardTitle={false}
              showTableInfo
              messageKind="event"
              selectedTable={selectedSourceTable}
              bind:form
              {errors}
              isEditMode={false}
              onFilterChange={(filters) => (form.sourceTableFilters = filters)}
            />
          {/if}
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Destination configuration</CardTitle>
      </CardHeader>
      <CardContent>
        <div class="space-y-4">
          <Accordion class="space-y-2" value="instructions" type="single">
            <AccordionItem value="instructions">
              <AccordionTrigger class="text-sm font-medium">
                Don't have an events table yet? See instructions
              </AccordionTrigger>
              <AccordionContent>
                <div class="space-y-4">
                  <div class="space-y-2">
                    <Label for="destinationTableName">Desired table name</Label>
                    <Input
                      id="destinationTableName"
                      bind:value={destinationTableName}
                    />
                  </div>

                  <div class="space-y-2">
                    <Label for="retentionDays"
                      >Desired retention period (days)</Label
                    >
                    <Input
                      type="number"
                      id="retentionDays"
                      bind:value={retentionDays}
                    />
                  </div>

                  {#if destinationTableName}
                    <div class="mt-4">
                      <p class="mb-2">
                        <strong>Create your event table</strong>
                      </p>
                      <p class="mb-2">
                        Create an event table with the following DDL, including
                        indexes:
                      </p>
                      <CodeWithCopy
                        language="sql"
                        code={createTableDDL}
                        copyIconPosition="top"
                      />
                    </div>
                    <div class="mt-4">
                      <p class="mb-2">
                        <strong>Create your retention policy</strong>
                      </p>
                      <p class="mb-2">
                        We recommend using the pg_partman extension to enforce a
                        retention policy. Here's what it looks like to enforce a
                        retention of {retentionDays} days:
                      </p>
                      <CodeWithCopy
                        language="sql"
                        code={retentionPolicyDDL}
                        copyIconPosition="top"
                      />
                    </div>
                  {/if}
                </div>
              </AccordionContent>
            </AccordionItem>
          </Accordion>

          <Label>Destination database</Label>
          <Select bind:value={form.destinationDatabaseId}>
            <SelectTrigger>
              <SelectValue placeholder="Select a database" />
            </SelectTrigger>
            <SelectContent>
              {#each databases as database}
                <SelectItem value={database.id}>{database.name}</SelectItem>
              {/each}
            </SelectContent>
          </Select>

          <Label>Destination Table</Label>
          <TableSelector
            {databases}
            onSelect={({ databaseId, tableOid }) => {
              form.destinationDatabaseId = databaseId;
              form.destinationTableOid = tableOid;
            }}
            selectedDatabaseId={form.destinationDatabaseId}
            selectedTableOid={form.destinationTableOid}
          />
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardContent>
        <div class="space-y-4 my-4">
          <div class="space-y-2">
            <Label for="name">WAL Projection name</Label>
            <Input id="name" bind:value={form.name} />
            {#if errors.name}
              <p class="text-red-500 text-sm">{errors.name}</p>
            {/if}
          </div>
          <div class="flex justify-end">
            <Button
              type="submit"
              disabled={!form.sourceTableOid || !form.destinationTableOid}
            >
              {walProjection.id ? "Update" : "Create"} WAL Projection
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
