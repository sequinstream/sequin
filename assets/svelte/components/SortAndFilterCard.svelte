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
  import Datetime from "./Datetime.svelte";
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";

  export let messageKind: string;
  export let selectedTable: any;
  export let form: any;
  export let errors: any;
  export let isEditMode: boolean;
  export let onFilterChange: (newFilters: any) => void;
  export let showTableInfo = false; // New prop to control table info display
  export let showCardTitle = true;
  export let showStartPositionForm = true;

  $: actions = form.sourceTableActions || [];

  $: sortColumn = selectedTable?.columns?.find(
    (col) => col.attnum === form.sortColumnAttnum
  );

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
          updatedFilter.operator = "="; // Set default operator to "="
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

  let startPosition = "beginning";

  $: {
    if (startPosition === "beginning") {
      form.recordConsumerState = {
        producer: "table_and_wal",
        initialMinSortCol: null,
      };
    } else if (startPosition === "end") {
      form.recordConsumerState = {
        producer: "wal",
        initialMinSortCol: null,
      };
    } else if (startPosition === "specific") {
      form.recordConsumerState.producer = "table_and_wal";
    }
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>
      {#if messageKind === "record" && showCardTitle}
        Records to process
      {:else if showCardTitle}
        Changes to process
      {/if}
    </CardTitle>
  </CardHeader>
  <CardContent class="space-y-6">
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
      <div class="space-y-6">
        <div>
          <h4 class="text-lg font-semibold mb-2">Sort and start</h4>

          <div class="space-y-4">
            <div>
              <Label for="sortColumn" class="text-base font-medium"
                >Sort column</Label
              >
              <p class="text-sm text-muted-foreground mt-1 mb-2">
                Select the sort column for the table. Your system should update
                the sort column whenever a row is updated. A good example of a
                sort column is <code>updated_at</code>.
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
              {#if errors.source_tables?.[0]?.sort_column_attnum}
                <p class="text-destructive text-sm mt-1">
                  {errors.source_tables[0].sort_column_attnum[0]}
                </p>
              {/if}
            </div>
          </div>
        </div>

        {#if showStartPositionForm && sortColumn}
          <div>
            <Label for="startPosition" class="text-base font-medium">
              Where should the consumer start?
            </Label>
            <p class="text-sm text-muted-foreground mt-1 mb-2">
              Indicate where in the table you want the consumer to start.
            </p>
            <RadioGroup bind:value={startPosition}>
              <div class="flex items-center space-x-2">
                <RadioGroupItem value="beginning" id="beginning" />
                <Label for="beginning">At the beginning of the table</Label>
              </div>
              <div class="flex items-center space-x-2">
                <RadioGroupItem value="end" id="end" />
                <Label for="end">At the end of the table (now forward)</Label>
              </div>
              <div class="flex items-center space-x-2">
                <RadioGroupItem value="specific" id="specific" />
                <Label for="specific">At a specific position...</Label>
              </div>
            </RadioGroup>

            {#if startPosition === "specific"}
              <div class="grid grid-cols-[auto_1fr] gap-4 mt-4 content-center">
                <div
                  class="flex content-center items-center space-x-2 text-sm font-mono"
                  class:mt-8={sortColumn?.type.startsWith("timestamp")}
                >
                  <span class="bg-secondary-2xSubtle px-2 py-1 rounded"
                    >{sortColumn.name}</span
                  >
                  <span class="bg-secondary-2xSubtle px-2 py-1 rounded"
                    >&gt;=</span
                  >
                </div>

                {#if sortColumn?.type.startsWith("timestamp")}
                  <Datetime
                    bind:value={form.recordConsumerState.initialMinSortCol}
                  />
                {:else if ["integer", "bigint", "smallint", "serial"].includes(sortColumn?.type)}
                  <Input
                    type="number"
                    bind:value={form.recordConsumerState.initialMinSortCol}
                  />
                {:else}
                  <Input
                    type="text"
                    bind:value={form.recordConsumerState.initialMinSortCol}
                  />
                {/if}
              </div>
            {/if}
          </div>
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

    <div>
      <h4 class="text-lg font-semibold mb-4">Filters</h4>
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
              value: filter.operator || "=",
              label: filter.operator || "=",
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
