<script lang="ts">
  import { Checkbox } from "$lib/components/ui/checkbox";

  export let columns: any[];
  export let selectedAttnums: number[];
  export let onToggle: ((attnum: number) => void) | undefined = undefined;
  export let readonly: boolean = false;
</script>

<div class="space-y-2 max-h-96 overflow-y-auto">
  {#each columns as column}
    <div class="flex items-center space-x-2">
      <Checkbox
        id={`column-${column.attnum}`}
        checked={selectedAttnums.includes(column.attnum)}
        onCheckedChange={onToggle ? () => onToggle(column.attnum) : undefined}
        disabled={readonly}
      />
      <label
        for={`column-${column.attnum}`}
        class="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70"
      >
        {column.name}
      </label>
    </div>
  {/each}
</div>
