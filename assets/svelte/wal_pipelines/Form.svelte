<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import {
    Select,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import TableSelector from "../components/TableSelector.svelte";
  import FilterForm from "../components/FilterForm.svelte";
  import ColumnSelectionForm from "../components/ColumnSelectionForm.svelte"; // 🍆
  import FullPageForm from "../components/FullPageForm.svelte";
  import { cn } from "$lib/utils";

  export let walPipeline: any;
  export let databases: any[];
  export let errors: any = {};
  export let parent: string;
  export let isEdit: boolean;
  export let live;
  export let submitError: string;
  const pushEvent = (event, payload = {}, cb = (event: any) => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  let form = {
    name: walPipeline.name || "",
    status: walPipeline.status || "active",
    postgresDatabaseId: walPipeline.postgresDatabaseId || "",
    destinationDatabaseId: walPipeline.destinationDatabaseId || "",
    tableOid: walPipeline.tableOid || "",
    destinationTableOid: walPipeline.destinationTableOid || "",
    sourceTableActions: walPipeline.sourceTableActions || [
      "insert",
      "update",
      "delete",
    ],
    sourceTableFilters: walPipeline.sourceTableFilters || [],
    excludeColumnAttnums: walPipeline.excludeColumnAttnums || [], // 🍆
    includeColumnAttnums: walPipeline.includeColumnAttnums || [], // 🍆
    sortColumnAttnum: walPipeline.sortColumnAttnum || "",
  };

  let selectedSourceDatabase: any;
  let selectedDestinationDatabase: any;
  let selectedSourceTable: any;
  let selectedDestinationTable: any;

  $: {
    if (form.postgresDatabaseId) {
      selectedSourceDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId,
      );
    }
    if (form.destinationDatabaseId) {
      selectedDestinationDatabase = databases.find(
        (db) => db.id === form.destinationDatabaseId,
      );
    }
    if (form.tableOid) {
      selectedSourceTable = selectedSourceDatabase.tables.find(
        (table) => table.oid === form.tableOid,
      );
    }
    if (form.destinationTableOid) {
      selectedDestinationTable = selectedDestinationDatabase.tables.find(
        (table) => table.oid === form.destinationTableOid,
      );
    }
  }

  function handleSubmit() {
    pushEvent("save", { wal_pipeline: form });
  }

  $: pushEvent("validate", { wal_pipeline: form });

  let dialogOpen = true;
  let showConfirmDialog = false;

  function handleClose() {
    pushEvent("form_closed");
  }
</script>

<FullPageForm
  title={isEdit ? "Edit Change Retention" : "Setup Change Retention"}
  on:close={handleClose}
>
  <form
    on:submit|preventDefault={handleSubmit}
    class="space-y-6 max-w-3xl mx-auto"
  >
    <Card>
      <CardHeader>
        <CardTitle>Source configuration</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-4 text-secondary-foreground text-sm" class:hidden={isEdit}>
          With Change Retention, you can capture every insert, update, or delete
          from one or more tables into another table in your database. Then, you
          can backfill and replay changes to a sink.
        </p>

        <div class="space-y-4">
          <Label>Source table</Label>
          {#if isEdit}
            <Select
              disabled
              selected={{
                value: form.postgresDatabaseId,
                label: selectedSourceDatabase?.name || "Selected database",
              }}
            >
              <SelectTrigger
                class={cn(
                  "w-full",
                  "bg-muted text-muted-foreground opacity-100",
                )}
              >
                <SelectValue placeholder="Selected database" />
              </SelectTrigger>
            </Select>
            <Select
              disabled
              selected={{
                value: form.tableOid,
                label: selectedSourceTable?.name || "Selected table",
              }}
            >
              <SelectTrigger
                class={cn(
                  "w-full",
                  "bg-muted text-muted-foreground opacity-100",
                )}
              >
                <SelectValue placeholder="Selected table" />
              </SelectTrigger>
            </Select>
          {:else}
            <TableSelector
              {pushEvent}
              {databases}
              onSelect={({ databaseId, tableOid }) => {
                form.postgresDatabaseId = databaseId;
                form.tableOid = tableOid;
              }}
              selectedDatabaseId={form.postgresDatabaseId}
              selectedTableOid={form.tableOid}
              excludeEventTables
            />
          {/if}

          {#if selectedSourceTable}
            <FilterForm
              selectedTable={selectedSourceTable}
              bind:form
              onFilterChange={(filters) => (form.sourceTableFilters = filters)}
            />
            <ColumnSelectionForm // 🍆
              {selectedSourceTable}
              bind:excludeColumnAttnums={form.excludeColumnAttnums}
              bind:includeColumnAttnums={form.includeColumnAttnums}
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
          <Label>Destination table</Label>
          {#if isEdit}
            <Select
              disabled
              selected={{
                value: form.destinationDatabaseId,
                label: selectedDestinationDatabase?.name || "Selected database",
              }}
            >
              <SelectTrigger
                class={cn(
                  "w-full",
                  "bg-muted text-muted-foreground opacity-100",
                )}
              >
                <SelectValue placeholder="Selected database" />
              </SelectTrigger>
            </Select>
            <Select
              disabled
              selected={{
                value: form.destinationTableOid,
                label: selectedDestinationTable?.name || "Selected table",
              }}
            >
              <SelectTrigger
                class={cn(
                  "w-full",
                  "bg-muted text-muted-foreground opacity-100",
                )}
              >
                <SelectValue placeholder="Selected table" />
              </SelectTrigger>
            </Select>
          {:else}
            <TableSelector
              {pushEvent}
              {databases}
              onSelect={({ databaseId, tableOid }) => {
                form.destinationDatabaseId = databaseId;
                form.destinationTableOid = tableOid;
              }}
              selectedDatabaseId={form.destinationDatabaseId}
              selectedTableOid={form.destinationTableOid}
              onlyEventTables
            />
          {/if}
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardContent>
        <div class="space-y-4 my-4">
          <div class="space-y-2">
            <Label for="name">Change Retention name</Label>
            <Input id="name" bind:value={form.name} />
            {#if errors.name}
              <p class="text-red-500 text-sm">{errors.name}</p>
            {/if}
          </div>
          {#if submitError}
            <p class="text-destructive text-sm">{submitError}</p>
          {/if}
          <div class="flex justify-end">
            <Button
              type="submit"
              disabled={!form.tableOid || !form.destinationTableOid}
            >
              {walPipeline.id ? "Update" : "Setup"} Change Retention
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  </form>
</FullPageForm>
