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
  export let showTitle: boolean = true;
  // export let isEditMode: boolean;
  export let onFilterChange: (newFilters: any) => void;

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
      operator: "=",
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

<div class="flex flex-col gap-6">
  {#if showTitle}
    <Label class="text-base font-medium">
      {#if messageKind === "event"}
        Filters
      {:else}
        Column filters
      {/if}
    </Label>
  {/if}

  {#if messageKind === "event"}
    <div class="flex flex-col gap-4">
      <Label>Operations to capture</Label>
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
    {#if messageKind === "event"}
      <Label>Column filters</Label>
    {/if}
    {#each form.sourceTableFilters as filter, index}
      <div class="flex flex-col gap-2">
        <div class="grid grid-cols-[1fr_1fr_1fr_auto] gap-4">
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
            <SelectContent class="max-h-64 overflow-y-auto">
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
          <p class="text-destructive text-sm">
            {filterErrorMessages[index]}
          </p>
        {/if}
      </div>
    {/each}
    <div class="flex justify-start">
      <Button
        variant="outline"
        size="sm"
        on:click={addFilter}
        disabled={!form.postgresDatabaseId && !form.tableOid}
      >
        <PlusCircle class="w-4 h-4 mr-2" />
        Add filter
      </Button>
    </div>
  </div>
</div>
