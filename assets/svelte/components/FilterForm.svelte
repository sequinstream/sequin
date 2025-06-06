<script lang="ts">
  import { Label } from "$lib/components/ui/label";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { Switch } from "$lib/components/ui/switch";
  import { HelpCircle } from "lucide-svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Plus } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import FunctionPicker from "$lib/consumers/FunctionPicker.svelte";

  export let messageKind: string;
  export let selectedTable: any;
  export let form: any;
  export let errors: any;
  export let onFilterChange: (newFilters: any) => void;
  export let showTitle: boolean = true;
  export let functions: Array<{
    id: string;
    name: string;
    type: string;
    description: string;
  }> = [];
  export let refreshFunctions: () => void = () => {};
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";

  $: actions = form.sourceTableActions || [];
  const switches = [
    { id: "insert", label: "Insert" },
    { id: "update", label: "Update" },
    { id: "delete", label: "Delete" },
  ];

  const fieldTypes = [
    { value: "string", label: "Text" },
    { value: "number", label: "Number" },
    { value: "boolean", label: "Boolean" },
    { value: "datetime", label: "Datetime" },
    { value: "list", label: "List" },
    { value: "cistring", label: "Case-insensitive text" },
  ];

  // TableFilters component logic
  type Filter = {
    columnAttnum: number | null;
    isJsonb: boolean | null;
    operator: string | null;
    value: string;
    valueType: string | null;
    jsonbPath: string | null;
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
      isJsonb: null,
      operator: "=",
      value: "",
      valueType: null,
      jsonbPath: null,
    };
    form.sourceTableFilters = [...form.sourceTableFilters, newFilter];
    onFilterChange(form.sourceTableFilters);
  }

  function removeFilter(index: number) {
    form.sourceTableFilters = form.sourceTableFilters.filter(
      (_, i) => i !== index,
    );
    onFilterChange(form.sourceTableFilters);
  }

  function updateFilter(index: number, key: keyof Filter, value: any) {
    form.sourceTableFilters = form.sourceTableFilters.map((filter, i) => {
      if (i === index) {
        const updatedFilter = { ...filter, [key]: value };

        // Side effects
        // Clear value when operator is IS NULL or IS NOT NULL
        if (key === "operator" && ["IS NULL", "IS NOT NULL"].includes(value)) {
          updatedFilter.value = "";
        }

        // Update column type and value type when column changes
        if (key === "columnAttnum") {
          const selectedColumn = selectedTable?.columns.find(
            (col) => col.attnum === value,
          );
          if (selectedColumn) {
            // The `columnType` of the filter always maps to the column's filter type
            updatedFilter.isJsonb = selectedColumn.filterType === "jsonb";

            // But the valueType may be different than the column's filter type if JSONB.
            // That's because JSONB columns embed many value types.
            // So, we'll prompt the user to select the value type.
            if (!updatedFilter.isJsonb) {
              updatedFilter.valueType = selectedColumn.filterType;
            } else {
              updatedFilter.valueType = "string";
            }
          }

          // Clear the value if the column type changes
          if (filter.valueType !== updatedFilter.valueType) {
            updatedFilter.value = "";
          }
        }

        return updatedFilter;
      }
      return filter;
    });
    onFilterChange(form.sourceTableFilters);
  }

  function handleFilterFunctionChange(functionId: string) {
    form.filterId = functionId;
  }

  const getFieldTypeLabel = (value: string) => {
    return fieldTypes.find((type) => type.value === value)?.label || value;
  };

  $: filterErrorMessages = (
    errors.sequence_filter?.column_filters || []
  ).reduce(
    (acc, error, index) => {
      if (error) {
        if (error.columnAttnum) acc[index] = error.columnAttnum[0];
        else if (error.operator) acc[index] = error.operator[0];
        else if (error.value && error.value.value)
          acc[index] = error.value.value[0];
        else if (error.value) acc[index] = error.value[0];
      }
      return acc;
    },
    {} as Record<number, string>,
  );
</script>

<div class="flex flex-col gap-8">
  {#if showTitle}
    <Label class="text-base font-medium">Filters</Label>
  {/if}

  {#if messageKind === "event"}
    <div class="flex flex-col gap-4">
      <div class="text-base font-medium">Operations to capture</div>
      <div class="flex items-center gap-4">
        {#each switches as { id, label }}
          <div class="flex items-center gap-2">
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

  <div class="flex flex-col gap-4">
    <div class="flex justify-between mb-4">
      <div class="text-base font-medium">Column filters</div>
      <Button
        variant="outline"
        class="whitespace-nowrap"
        on:click={addFilter}
        disabled={(!form.postgresDatabaseId && !form.tableOid) ||
          !selectedTable}
      >
        <Plus class="h-4 w-4 mr-2" />
        Add column filter
      </Button>
    </div>

    {#if !selectedTable}
      <!-- Empty state when no table is selected -->
      <div class="border rounded-lg">
        <div class="p-4 text-sm">
          <div class="font-medium">No table selected</div>
          <div class="text-sm text-muted-foreground">
            Please select a table to start adding column filters.
          </div>
        </div>
      </div>
    {:else if form.sourceTableFilters.length === 0}
      <!-- Empty state when table selected but no filters added -->
      <div class="border rounded-lg">
        <div class="p-4 text-sm">
          <div class="font-medium">No column filters</div>
          <div class="text-sm text-muted-foreground">
            You haven't added any column filters yet.
          </div>
        </div>
      </div>
    {:else}
      {#each form.sourceTableFilters as filter, index}
        <div class="bg-blue-50 border-bg-blue-100 rounded-lg p-4">
          <div class="grid grid-cols-[2fr_1fr_2fr_auto] gap-4 items-start">
            <!-- Column 1: Column selection (and JSONB fields if applicable) -->
            <div class="flex flex-col gap-2">
              <Label for={`column-${index}`}
                >Column
                <!-- Unused help circle, helps align inputs -->
                <HelpCircle
                  class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help invisible"
                />
              </Label>
              <Select
                selected={{
                  value: filter.columnAttnum,
                  label:
                    selectedTable?.columns.find(
                      (col) => col.attnum === filter.columnAttnum,
                    )?.name || "Column",
                }}
                onSelectedChange={(e) =>
                  updateFilter(index, "columnAttnum", e.value)}
                disabled={!form.postgresDatabaseId && !form.tableOid}
              >
                <SelectTrigger class="border-carbon-100 bg-surface-base">
                  <SelectValue placeholder="Column" />
                </SelectTrigger>
                <SelectContent class="max-h-64 overflow-y-auto">
                  {#each selectedTable?.columns || [] as column}
                    <SelectItem value={column.attnum}>{column.name}</SelectItem>
                  {/each}
                </SelectContent>
              </Select>
            </div>

            {#if filter.isJsonb}
              <div class="flex flex-col gap-2 col-start-1">
                <Label for={`field-path-${index}`} class="flex items-center">
                  Field path
                  <Tooltip.Root openDelay={200}>
                    <Tooltip.Trigger>
                      <HelpCircle
                        class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help"
                      />
                    </Tooltip.Trigger>
                    <Tooltip.Content class="max-w-xs">
                      <p class="text-xs text-gray-500">
                        Specify the path to the JSONB field you want to filter
                        on. Use dot notation for nested fields (e.g.,
                        "address.city").
                      </p>
                    </Tooltip.Content>
                  </Tooltip.Root>
                </Label>
                <Input
                  id={`field-path-${index}`}
                  type="text"
                  placeholder="path.to.value"
                  value={filter.jsonbPath}
                  on:input={(e) =>
                    updateFilter(index, "jsonbPath", e.currentTarget.value)}
                  disabled={!form.postgresDatabaseId && !form.tableOid}
                  class="bg-surface-base border-carbon-100"
                />
              </div>

              <div class="flex flex-col gap-2 col-start-1">
                <Label for={`field-type-${index}`} class="flex items-center">
                  Field type
                  <Tooltip.Root openDelay={200}>
                    <Tooltip.Trigger>
                      <HelpCircle
                        class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help"
                      />
                    </Tooltip.Trigger>
                    <Tooltip.Content class="max-w-xs">
                      <p class="text-xs text-gray-500">
                        Select the data type of the JSONB field you're filtering
                        on. This helps ensure proper comparison and filtering.
                      </p>
                    </Tooltip.Content>
                  </Tooltip.Root>
                </Label>
                <Select
                  selected={{
                    value: filter.valueType,
                    label: getFieldTypeLabel(filter.valueType) || "Field type",
                  }}
                  onSelectedChange={(e) =>
                    updateFilter(index, "valueType", e.value)}
                  disabled={!form.postgresDatabaseId && !form.tableOid}
                >
                  <SelectTrigger class="border-carbon-100 bg-surface-base">
                    <SelectValue placeholder="Field type" />
                  </SelectTrigger>
                  <SelectContent>
                    {#each fieldTypes as { value, label }}
                      <SelectItem {value}>{label}</SelectItem>
                    {/each}
                  </SelectContent>
                </Select>
              </div>
            {/if}

            <!-- Column 2: Operator -->
            <div
              class="flex flex-col gap-2 {filter.isJsonb
                ? 'row-start-2 col-start-2'
                : ''}"
            >
              <Label for={`operator-${index}`}
                >Operator
                <!-- Unused help circle, helps align inputs -->
                <HelpCircle
                  class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help invisible"
                />
              </Label>
              <Select
                selected={{
                  value: filter.operator,
                  label: filter.operator || "Operator",
                }}
                onSelectedChange={(e) =>
                  updateFilter(index, "operator", e.value)}
                disabled={!form.postgresDatabaseId && !form.tableOid}
              >
                <SelectTrigger class="border-carbon-100 bg-surface-base">
                  <SelectValue placeholder="Operator" />
                </SelectTrigger>
                <SelectContent>
                  {#each operators as operator}
                    <SelectItem value={operator}>{operator}</SelectItem>
                  {/each}
                </SelectContent>
              </Select>
            </div>

            <!-- Column 3: Comparand -->
            {#if !["IS NULL", "IS NOT NULL"].includes(filter.operator)}
              <div
                class="flex flex-col gap-2 {filter.isJsonb
                  ? 'row-start-2 col-start-3'
                  : ''}"
              >
                <Label for={`value-${index}`}
                  >Comparison value
                  <!-- Unused help circle, helps align inputs -->
                  <HelpCircle
                    class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help invisible"
                  />
                </Label>
                <Input
                  id={`value-${index}`}
                  type="text"
                  placeholder="Value"
                  value={filter.value}
                  on:input={(e) => {
                    e.stopPropagation();
                    updateFilter(index, "value", e.currentTarget.value);
                  }}
                  on:keydown={(e) => {
                    if (e.key === "Enter") {
                      e.preventDefault();
                      e.stopPropagation();
                    }
                  }}
                  disabled={!form.postgresDatabaseId && !form.tableOid}
                  class="bg-surface-base border-carbon-100"
                />
              </div>
            {/if}

            <!-- Column 4: Remove button -->
            <button
              type="button"
              on:click={(e) => {
                e.stopPropagation();
                removeFilter(index);
              }}
              class="text-carbon-400 hover:text-carbon-600 justify-self-end p-2 transition-colors hover:scale-110 self-start mt-6 {filter.isJsonb
                ? 'row-start-2 col-start-4'
                : ''}"
              disabled={!form.postgresDatabaseId && !form.tableOid}
              aria-label="Remove filter"
            >
              <icon class="hero-x-mark w-4 h-4" />
            </button>
          </div>

          {#if filterErrorMessages[index]}
            <p class="text-destructive text-sm mt-2">
              {filterErrorMessages[index]}
            </p>
          {/if}
        </div>
      {/each}
    {/if}
  </div>

  <!-- Filter Function -->
  <div class="flex flex-col gap-4">
    <FunctionPicker
      {functions}
      selectedFunctionId={form.filterId || "none"}
      title="Filter"
      onFunctionChange={handleFilterFunctionChange}
      {refreshFunctions}
      functionTypes={["filter"]}
      createNewQueryParams="?type=filter"
      bind:refreshState={functionRefreshState}
    >
      <svelte:fragment>
        <!-- This is the header for the function picker -->
        <span class="text-base font-medium">Filter functions</span>
      </svelte:fragment>

      <svelte:fragment slot="none-option">No function filter.</svelte:fragment>
    </FunctionPicker>
  </div>
</div>
