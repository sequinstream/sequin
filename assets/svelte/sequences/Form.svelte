<script lang="ts">
  import { onMount } from "svelte";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";

  export let databases: Array<{
    id: string;
    name: string;
    tables: Array<{
      oid: number;
      schema: string;
      name: string;
      columns: Array<{
        attnum: number;
        name: string;
        type: string;
      }>;
    }>;
  }>;
  export let errors: Record<string, any> = {};
  export let submitError: string | null = null;
  export let parent: string;
  export let live;

  let form = {
    postgres_database_id: "",
    table_oid: null,
    sort_column_attnum: null,
  };
  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;

  let selectedDatabase = null;
  let selectedTable = null;

  function updateSelectedDatabase() {
    if (form.postgres_database_id) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgres_database_id
      );
    } else {
      selectedDatabase = null;
    }

    console.log("selectedDatabase", selectedDatabase);

    // Reset table and sort column when database changes
    form.table_oid = null;
    form.sort_column_attnum = null;
  }

  function updateSelectedTable() {
    if (selectedDatabase && form.table_oid) {
      selectedTable = selectedDatabase.tables.find(
        (table) => table.oid === form.table_oid
      );
    } else {
      selectedTable = null;
    }

    // Reset sort column when table changes
    form.sort_column_attnum = null;
  }

  $: updateSelectedDatabase();
  $: updateSelectedTable();

  function pushEvent(
    event: string,
    payload = {},
    callback: (reply?: any) => void = () => {}
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    validating = true;
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        validating = false;
      }
    });
  }

  $: pushEvent("form_updated", { form });

  function handleClose() {
    pushEvent("form_closed");
  }
</script>

<FullPageModal
  title="Create Sequence"
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-4xl mx-auto mt-6">
    <Card>
      <CardHeader>
        <CardTitle>Sequence Details</CardTitle>
      </CardHeader>
      <CardContent class="space-y-6">
        <div class="space-y-2">
          <Label for="postgres_database_id">Database</Label>
          <Select
            selected={{
              value: form.postgres_database_id,
              label:
                databases.find((db) => db.id === form.postgres_database_id)
                  ?.name || "Select a database",
            }}
            onSelectedChange={(event) => {
              form.postgres_database_id = event.value;
              updateSelectedDatabase();
            }}
          >
            <SelectTrigger>
              <SelectValue placeholder="Select a database" />
            </SelectTrigger>
            <SelectContent>
              {#each databases as database}
                <SelectItem value={database.id}>{database.name}</SelectItem>
              {/each}
            </SelectContent>
          </Select>
          {#if errors.postgres_database_id}
            <p class="text-destructive text-sm">
              {errors.postgres_database_id}
            </p>
          {/if}
        </div>

        {#if selectedDatabase}
          <div class="space-y-2">
            <Label for="table_oid">Table</Label>
            <Select
              selected={{
                value: form.table_oid,
                label: selectedDatabase.tables.find(
                  (table) => table.oid === form.table_oid
                )
                  ? `${selectedDatabase.tables.find((table) => table.oid === form.table_oid).schema}.${selectedDatabase.tables.find((table) => table.oid === form.table_oid).name}`
                  : "Select a table",
              }}
              onSelectedChange={(event) => {
                form.table_oid = event.value;
                updateSelectedTable();
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a table" />
              </SelectTrigger>
              <SelectContent>
                {#each selectedDatabase.tables as table}
                  <SelectItem value={table.oid}
                    >{table.schema}.{table.name}</SelectItem
                  >
                {/each}
              </SelectContent>
            </Select>
            {#if errors.table_oid}
              <p class="text-destructive text-sm">{errors.table_oid}</p>
            {/if}
          </div>
        {/if}

        {#if selectedTable}
          <div class="space-y-2">
            <Label for="sort_column_attnum">Sort Column</Label>
            <Select
              selected={{
                value: form.sort_column_attnum,
                label:
                  selectedTable.columns.find(
                    (column) => column.attnum === form.sort_column_attnum
                  )?.name || "Select a sort column",
              }}
              onSelectedChange={(event) => {
                form.sort_column_attnum = event.value;
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a sort column" />
              </SelectTrigger>
              <SelectContent>
                {#each selectedTable.columns as column}
                  <SelectItem value={column.attnum}>{column.name}</SelectItem>
                {/each}
              </SelectContent>
            </Select>
            {#if errors.sort_column_attnum}
              <p class="text-destructive text-sm">
                {errors.sort_column_attnum}
              </p>
            {/if}
          </div>
        {/if}

        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {:else if Object.keys(errors).length > 0}
          <p class="text-destructive text-sm">Validation errors, see above</p>
        {/if}

        <div class="flex justify-end items-center">
          <Button
            type="submit"
            loading={validating}
            variant="default"
            disabled={!form.postgres_database_id ||
              !form.table_oid ||
              !form.sort_column_attnum}
          >
            <span slot="loading">Validating...</span>
            Create Sequence
          </Button>
        </div>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
