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
  import { Info } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { cn } from "$lib/utils";

  export let walPipeline: any;
  export let databases: any[];
  export let errors: any = {};
  export let parent: string;
  export let isEdit: boolean;
  export let live;

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
  };

  let selectedSourceDatabase: any;
  let selectedDestinationDatabase: any;
  let selectedSourceTable: any;
  let selectedDestinationTable: any;

  $: {
    if (form.postgresDatabaseId) {
      selectedSourceDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId
      );
    }
    if (form.destinationDatabaseId) {
      selectedDestinationDatabase = databases.find(
        (db) => db.id === form.destinationDatabaseId
      );
    }
    if (form.tableOid) {
      selectedSourceTable = selectedSourceDatabase.tables.find(
        (table) => table.oid === form.tableOid
      );
    }
    if (form.destinationTableOid) {
      selectedDestinationTable = selectedDestinationDatabase.tables.find(
        (table) => table.oid === form.destinationTableOid
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

<FullPageModal
  title={isEdit ? "Edit WAL Pipeline" : "Create WAL Pipeline"}
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form
    on:submit|preventDefault={handleSubmit}
    class="space-y-6 max-w-3xl mx-auto mt-6"
  >
    <Card>
      <CardHeader>
        <CardTitle>Source configuration</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="mb-4 text-secondary-foreground text-sm" class:hidden={isEdit}>
          With a WAL Pipeline, you can capture every insert, update, or delete
          that happens to one or more tables into another table in your
          database. Then, you can stream these events with Sequin.
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
                  "bg-muted text-muted-foreground opacity-100"
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
                  "bg-muted text-muted-foreground opacity-100"
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
            />
          {/if}

          {#if selectedSourceTable}
            <SortAndFilterCard
              showCardTitle={false}
              showTableInfo
              messageKind="event"
              selectedTable={selectedSourceTable}
              bind:form
              {errors}
              isEditMode={isEdit}
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
                  "bg-muted text-muted-foreground opacity-100"
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
                  "bg-muted text-muted-foreground opacity-100"
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
            <Label for="name">WAL Pipeline name</Label>
            <Input id="name" bind:value={form.name} />
            {#if errors.name}
              <p class="text-red-500 text-sm">{errors.name}</p>
            {/if}
          </div>
          <div class="flex justify-end">
            <Button
              type="submit"
              disabled={!form.tableOid || !form.destinationTableOid}
            >
              {walPipeline.id ? "Update" : "Create"} WAL Pipeline
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
