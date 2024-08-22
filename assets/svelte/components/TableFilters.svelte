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
    column: number | null;
    operator: string | null;
    value: string;
  };

  export let filters: Array<Filter>;
  export let columns: Array<{ attnum: number; name: string }>;
  export let onFilterChange: (filters: Array<Filter>) => void;
  export let disabled: boolean = false;

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
    filters = [...filters, { column: null, operator: null, value: "" }];
    onFilterChange(filters);
  }

  function removeFilter(index: number) {
    filters = filters.filter((_, i) => i !== index);
    onFilterChange(filters);
  }

  function updateFilter(
    index: number,
    key: keyof {
      column: number | null;
      operator: string | null;
      value: string;
    },
    value: any
  ) {
    filters = filters.map((filter, i) =>
      i === index
        ? {
            ...filter,
            [key]: value,
          }
        : filter
    );
    onFilterChange(filters);
  }
</script>

<div class="mb-6">
  <h3 class="text-lg font-semibold mb-2">Filters</h3>
  {#each filters as filter, index}
    <div class="grid grid-cols-[1fr_1fr_1fr_15px] gap-4 mb-2">
      <Select
        selected={{
          value: filter.column,
          label:
            columns.find((col) => col.attnum === filter.column)?.name ||
            "Column",
        }}
        onSelectedChange={(e) => updateFilter(index, "column", e.value)}
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
        {disabled}
      />
      <button
        on:click={() => removeFilter(index)}
        class="text-carbon-400 hover:text-carbon-600 justify-self-end"
        {disabled}
      >
        <icon class="hero-x-mark w-4 h-4" />
      </button>
    </div>
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
