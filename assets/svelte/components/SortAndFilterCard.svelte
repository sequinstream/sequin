<script lang="ts">
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Switch } from "$lib/components/ui/switch";
  import { ExternalLinkIcon } from "lucide-svelte";
  import { getColorFromName } from "$lib/utils";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { PlusCircle } from "lucide-svelte";

  export let messageKind: string;
  export let selectedTable: any;
  export let form: any;
  export let errors: any;
  export let isEditMode: boolean;
  export let onFilterChange: (newFilters: any) => void;
  export let showTableInfo = false; // New prop to control table info display

  $: actions = form.sourceTableActions || [];

  const switches = [
    { id: "insert", label: "Insert" },
    { id: "update", label: "Update" },
    { id: "delete", label: "Delete" },
  ];

  // TableFilters component logic
  type Filter = {
    columnAttnum: number | null;
    operator: string | null;
    value: string;
    valueType: string | null;
  };

  const operators = [
    "=",
    "!=",
    ">",
    "<",
    ">=",
    "<=",
    "IN",
    "NOT IN",
    "IS NULL",
    "IS NOT NULL",
  ];

  function addFilter() {
    const newFilter: Filter = {
      columnAttnum: null,
      operator: null,
      value: "",
      valueType: null,
    };
    form.sourceTableFilters = [...form.sourceTableFilters, newFilter];
    onFilterChange(form.sourceTableFilters);
  }

  function removeFilter(index: number) {
    form.sourceTableFilters = form.sourceTableFilters.filter(
      (_, i) => i !== index
    );
    onFilterChange(form.sourceTableFilters);
  }

  function updateFilter(index: number, key: keyof Filter, value: any) {
    form.sourceTableFilters = form.sourceTableFilters.map((filter, i) => {
      if (i === index) {
        const updatedFilter = { ...filter, [key]: value };

        if (key === "operator" && ["IS NULL", "IS NOT NULL"].includes(value)) {
          updatedFilter.value = "";
        }

        if (key === "columnAttnum") {
          const selectedColumn = selectedTable?.columns.find(
            (col) => col.attnum === value
          );
          updatedFilter.valueType = selectedColumn
            ? selectedColumn.filterType
            : "";
        }

        return updatedFilter;
      }
      return filter;
    });
    onFilterChange(form.sourceTableFilters);
  }

  $: filterErrorMessages = (
    errors.source_tables?.[0]?.column_filters || []
  ).reduce(
    (acc, error, index) => {
      if (error) {
        if (error.columnAttnum) acc[index] = error.columnAttnum[0];
        else if (error.operator) acc[index] = error.operator[0];
        else if (error.value && error.value.value)
          acc[index] = error.value.value[0];
      }
      return acc;
    },
    {} as Record<number, string>
  );
</script>

<Card>
  <CardHeader>
    {#if messageKind === "record"}
      <CardTitle>Sort and filter</CardTitle>
    {:else}
      <CardTitle>Filters</CardTitle>
    {/if}
  </CardHeader>
  <CardContent>
    {#if showTableInfo && selectedTable}
      <div class="mb-6">
        <div class="grid grid-cols-[auto_1fr] gap-4 mb-2 items-center">
          <icon
            class="hero-table-cells w-6 h-6 rounded {getColorFromName(
              `${selectedTable.schema}.${selectedTable.name}`
            )}"
          ></icon>
          <span class="font-medium"
            >{selectedTable.schema}.{selectedTable.name}</span
          >
        </div>
      </div>
    {/if}

    {#if messageKind === "record"}
      <div class="space-y-2 mb-6">
        <Label for="sortColumn">Sort Column</Label>
        <Select
          selected={{
            value: form.sortColumnAttnum,
            label:
              selectedTable?.columns.find(
                (c) => c.attnum === form.sortColumnAttnum
              )?.name ||
              (selectedTable
                ? "Select a column"
                : "Please select a table first"),
          }}
          onSelectedChange={(event) => {
            form.sortColumnAttnum = event.value;
          }}
          disabled={isEditMode || !selectedTable}
        >
          <SelectTrigger
            class="w-full {isEditMode || !selectedTable
              ? 'bg-muted text-muted-foreground opacity-100'
              : ''}"
          >
            <SelectValue placeholder="Select a column" />
          </SelectTrigger>
          <SelectContent>
            {#each selectedTable?.columns || [] as column}
              <SelectItem value={column.attnum}>{column.name}</SelectItem>
            {/each}
          </SelectContent>
        </Select>
        <p class="text-sm text-muted-foreground">
          Select the sort column for the table. Your system should update the
          sort column whenever a row is updated. A good example of a sort column
          is <code>updated_at</code>.
          <a
            href="https://sequinstream.com/docs/core-concepts#sorting-row-consumers-only"
            target="_blank"
            rel="noopener noreferrer"
            class="inline-flex items-center text-link hover:underline"
          >
            Learn more
            <ExternalLinkIcon class="w-3 h-3 ml-1" />
          </a>
        </p>
        {#if errors.sort_column_attnum}
          <p class="text-destructive text-sm">{errors.sort_column_attnum}</p>
        {/if}
      </div>
    {/if}

    {#if messageKind === "event"}
      <div class="space-y-2 mb-6">
        <Label>Operations to capture</Label>
        <div class="flex items-center space-x-4">
          {#each switches as { id, label }}
            <div class="flex items-center space-x-2">
              <Label for={id} class="cursor-pointer">{label}</Label>
              <Switch
                {id}
                disabled={!form.postgresDatabaseId && !form.tableOid}
                checked={actions.includes(id)}
                onCheckedChange={(checked) => {
                  const newActions = checked
                    ? [...actions, id]
                    : actions.filter((a) => a !== id);
                  form.sourceTableActions = newActions;
                }}
              />
            </div>
          {/each}
        </div>
        {#if errors.source_tables?.[1]?.actions}
          <p class="text-destructive text-sm">
            {errors.source_tables[1].actions}
          </p>
        {/if}
      </div>
    {/if}

    <div class="my-6">
      {#each form.sourceTableFilters as filter, index}
        <div class="grid grid-cols-[1fr_1fr_1fr_15px] gap-4 mb-2">
          <Select
            selected={{
              value: filter.columnAttnum,
              label:
                selectedTable?.columns.find(
                  (col) => col.attnum === filter.columnAttnum
                )?.name || "Column",
            }}
            onSelectedChange={(e) =>
              updateFilter(index, "columnAttnum", e.value)}
            disabled={!form.postgresDatabaseId && !form.tableOid}
          >
            <SelectTrigger class="border-carbon-100">
              <SelectValue placeholder="Column" />
            </SelectTrigger>
            <SelectContent>
              {#each selectedTable?.columns || [] as column}
                <SelectItem value={column.attnum}>{column.name}</SelectItem>
              {/each}
            </SelectContent>
          </Select>
          <Select
            selected={{
              value: filter.operator,
              label: filter.operator || "Operator",
            }}
            onSelectedChange={(e) => updateFilter(index, "operator", e.value)}
            disabled={!form.postgresDatabaseId && !form.tableOid}
          >
            <SelectTrigger class="border-carbon-100">
              <SelectValue placeholder="Operator" />
            </SelectTrigger>
            <SelectContent>
              {#each operators as operator}
                <SelectItem value={operator}>{operator}</SelectItem>
              {/each}
            </SelectContent>
          </Select>
          <Input
            type="text"
            placeholder="Value"
            value={filter.value}
            on:input={(e) =>
              updateFilter(index, "value", e.currentTarget.value)}
            disabled={(!form.postgresDatabaseId && !form.tableOid) ||
              ["IS NULL", "IS NOT NULL"].includes(filter.operator)}
          />
          <button
            on:click={() => removeFilter(index)}
            class="text-carbon-400 hover:text-carbon-600 justify-self-end"
            disabled={!form.postgresDatabaseId && !form.tableOid}
          >
            <icon class="hero-x-mark w-4 h-4" />
          </button>
        </div>
        {#if filterErrorMessages[index]}
          <p class="text-destructive text-sm mt-1 mb-2">
            {filterErrorMessages[index]}
          </p>
        {/if}
      {/each}
      <div class="grid grid-cols-1 gap-4 max-w-fit">
        <Button
          variant="outline"
          size="sm"
          on:click={addFilter}
          class="mt-2"
          disabled={!form.postgresDatabaseId && !form.tableOid}
        >
          <PlusCircle class="w-4 h-4 mr-2" />
          Add filter
        </Button>
      </div>
    </div>
  </CardContent>
</Card>
