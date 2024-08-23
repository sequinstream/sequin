<script lang="ts">
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { PlusCircle } from "lucide-svelte";

  type Filter = {
    columnAttnum: number | null;
    operator: string | null;
    value: string;
    valueType: string | null;
  };

  export let filters: Array<Filter>;
  export let columns: Array<{
    attnum: number;
    name: string;
    filterType: string;
  }>;
  export let onFilterChange: (filters: Array<Filter>) => void;
  export let disabled: boolean = false;
  export let errors: Array<{
    operator?: string[];
    columnAttnum?: string[];
    value?: { value?: string[] };
  } | null> = [];

  $: console.log(errors);

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
    filters = [...filters, newFilter];
    onFilterChange(filters);
  }

  function removeFilter(index: number) {
    filters = filters.filter((_, i) => i !== index);
    onFilterChange(filters);
  }

  function updateFilter(
    index: number,
    key: keyof {
      columnAttnum: number | null;
      operator: string | null;
      value: string | null;
      valueType: string | null;
    },
    value: any
  ) {
    filters = filters.map((filter, i) => {
      if (i === index) {
        const updatedFilter = { ...filter, [key]: value };

        // Clear and disable value field for IS NULL and IS NOT NULL operators
        if (key === "operator" && ["IS NULL", "IS NOT NULL"].includes(value)) {
          updatedFilter.value = "";
        }

        // Set valueType when columnAttnum is updated
        if (key === "columnAttnum") {
          const selectedColumn = columns.find((col) => col.attnum === value);
          updatedFilter.valueType = selectedColumn
            ? selectedColumn.filterType
            : "";
        }

        return updatedFilter;
      }
      return filter;
    });
    onFilterChange(filters);
  }

  $: errorMessages = errors.reduce(
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

<div class="mb-6">
  {#each filters as filter, index}
    <div class="grid grid-cols-[1fr_1fr_1fr_15px] gap-4 mb-2">
      <Select
        selected={{
          value: filter.columnAttnum,
          label:
            columns.find((col) => col.attnum === filter.columnAttnum)?.name ||
            "Column",
        }}
        onSelectedChange={(e) => updateFilter(index, "columnAttnum", e.value)}
        {disabled}
      >
        <SelectTrigger class="border-carbon-100">
          <SelectValue placeholder="Column" />
        </SelectTrigger>
        <SelectContent>
          {#each columns as column}
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
        {disabled}
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
        on:input={(e) => updateFilter(index, "value", e.currentTarget.value)}
        disabled={disabled ||
          ["IS NULL", "IS NOT NULL"].includes(filter.operator)}
      />
      <button
        on:click={() => removeFilter(index)}
        class="text-carbon-400 hover:text-carbon-600 justify-self-end"
        {disabled}
      >
        <icon class="hero-x-mark w-4 h-4" />
      </button>
    </div>
    {#if errorMessages[index]}
      <p class="text-destructive text-sm mt-1 mb-2">
        {errorMessages[index]}
      </p>
    {/if}
  {/each}
  <div class="grid grid-cols-1 gap-4 max-w-fit">
    <Button
      variant="outline"
      size="sm"
      on:click={addFilter}
      class="mt-2"
      {disabled}
    >
      <PlusCircle class="w-4 h-4 mr-2" />
      Add filter
    </Button>
  </div>
</div>
