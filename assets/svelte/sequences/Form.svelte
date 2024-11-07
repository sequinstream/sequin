<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Label } from "$lib/components/ui/label";
  import { ExternalLinkIcon } from "lucide-svelte";
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
  import TableSelector from "../components/TableSelector.svelte";
  import { Input } from "$lib/components/ui/input";

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
    name: "",
  };
  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;

  let selectedDatabase = null;
  let selectedTable = null;

  function pushEvent(
    event: string,
    payload = {},
    callback: (reply?: any) => void = () => {},
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

  function handleTableSelect(event) {
    form.postgres_database_id = event.databaseId;

    if (event.databaseId) {
      selectedDatabase = databases.find((db) => db.id === event.databaseId);
    } else {
      selectedDatabase = null;
    }

    form.table_oid = event.tableOid;

    if (selectedDatabase && form.table_oid) {
      selectedTable = selectedDatabase.tables.find(
        (table) => table.oid === form.table_oid,
      );
    } else {
      selectedTable = null;
    }

    if (selectedTable && selectedTable.fixed_sort_column_attnum) {
      form.sort_column_attnum = selectedTable.fixed_sort_column_attnum;
    } else {
      // Reset sort column when table changes
      form.sort_column_attnum = null;
    }

    if (selectedTable) {
      form.name = `${selectedDatabase.name}.${selectedTable.schema}.${selectedTable.name}`;
    } else {
      form.name = "";
    }
  }

  function pushEventToParent(event, payload, callback) {
    pushEvent(event, payload, callback);
  }
</script>

<FullPageModal
  title="Create Stream"
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-4xl mx-auto mt-6">
    <Card>
      <CardHeader>
        <CardTitle>Stream details</CardTitle>
      </CardHeader>
      <CardContent class="space-y-6">
        <TableSelector
          {databases}
          onSelect={handleTableSelect}
          pushEvent={pushEventToParent}
          selectedDatabaseId={form.postgres_database_id}
          selectedTableOid={form.table_oid}
        />

        {#if errors.postgres_database_id}
          <p class="text-destructive text-sm">
            {errors.postgres_database_id}
          </p>
        {/if}

        {#if errors.table_oid}
          <p class="text-destructive text-sm">{errors.table_oid}</p>
        {/if}

        {#if selectedTable}
          <div class="space-y-2">
            <Label for="sort_column_attnum">Sort and start</Label>
            <p class="text-sm text-muted-foreground mt-1 mb-2">
              Select the sort column for the table. Your system should update
              the sort column whenever a row is updated. A good example of a
              sort column is <code>updated_at</code>.
              <a
                href="https://sequinstream.com/docs/how-sequin-works#creating-a-table-stream"
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
                value: form.sort_column_attnum,
                label:
                  selectedTable.columns.find(
                    (column) => column.attnum === form.sort_column_attnum,
                  )?.name || "Select a sort column",
              }}
              onSelectedChange={(event) => {
                form.sort_column_attnum = event.value;
              }}
              disabled={selectedTable?.fixed_sort_column_attnum}
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

        {#if selectedTable && form.sort_column_attnum}
          <div class="space-y-2">
            <Label for="name">Stream name</Label>
            <div class="flex flex-col gap-1">
              <Input
                id="name"
                bind:value={form.name}
                placeholder="Enter a unique name for your stream"
                data-1p-ignore
                data-lpignore="true"
                data-form-type="other"
              />
              <p class="text-xs font-light">
                Must contain only alphanumeric characters, underscores, hyphens,
                or dots.
              </p>
            </div>
            {#if errors.name}
              <p class="text-destructive text-sm">{errors.name}</p>
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
            Create Stream
          </Button>
        </div>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
