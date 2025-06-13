<script lang="ts">
  import { RadioGroup, RadioGroupItem } from "$lib/components/ui/radio-group";
  import { Label } from "$lib/components/ui/label";
  import { Input } from "$lib/components/ui/input";
  import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
  } from "$lib/components/ui/select";
  import { ExternalLinkIcon, CheckIcon, TableIcon } from "lucide-svelte";
  import Datetime from "./Datetime.svelte";
  import { conditionalClass } from "$lib/utils";
  import { Button } from "$lib/components/ui/button";
  import {
    Table as TableComponent,
    TableBody,
    TableCell,
    TableRow,
  } from "$lib/components/ui/table";
  import type { Consumer } from "$lib/consumers/types";

  export let form: {
    selectedTableOids?: number[];
  };
  export let consumer: Consumer;

  $: console.log(consumer);

  // Handle table selection for multi-table mode
  function toggleTableSelection(tableOid: number) {
    if (!form.selectedTableOids) form.selectedTableOids = [];

    const index = form.selectedTableOids.indexOf(tableOid);
    if (index > -1) {
      form.selectedTableOids = form.selectedTableOids.filter(
        (id) => id !== tableOid,
      );
    } else {
      form.selectedTableOids = [...form.selectedTableOids, tableOid];
    }
  }

  function toggleSelectAll() {
    if (!consumer.tables_included_in_source || !form.selectedTableOids) return;

    if (
      form.selectedTableOids.length ===
      consumer.tables_included_in_source.length
    ) {
      form.selectedTableOids = [];
    } else {
      form.selectedTableOids = consumer.tables_included_in_source.map(
        (t) => t.oid,
      );
    }
  }

  $: allTablesSelected =
    form.selectedTableOids && consumer.tables_included_in_source
      ? form.selectedTableOids.length ===
        consumer.tables_included_in_source.length
      : false;
</script>

<div class="space-y-4">
  <div class="space-y-4">
    <div>
      <p class="text-sm text-muted-foreground mt-1">
        Select one or more tables to backfill. All selected tables will be
        backfilled from the beginning.
      </p>
    </div>

    <div class="border rounded-lg overflow-hidden">
      <div class="flex items-center justify-between p-4 border-b bg-gray-50">
        <h3 class="font-medium">Tables to backfill</h3>
        <Button variant="outline" size="sm" on:click={toggleSelectAll}>
          {allTablesSelected ? "Deselect all" : "Select all"}
        </Button>
      </div>

      <div class="max-h-60 overflow-y-auto">
        <TableComponent>
          <TableBody>
            {#each consumer.tables_included_in_source as table (table.oid)}
              <TableRow
                on:click={() => toggleTableSelection(table.oid)}
                class="cursor-pointer {form.selectedTableOids?.includes(
                  table.oid,
                )
                  ? 'bg-blue-50 hover:bg-blue-100'
                  : 'hover:bg-gray-100'}"
              >
                <TableCell class="flex items-center space-x-2">
                  {#if form.selectedTableOids?.includes(table.oid)}
                    <CheckIcon class="h-4 w-4 text-green-500" />
                  {:else}
                    <TableIcon class="h-4 w-4 text-gray-400" />
                  {/if}
                  <span>{table.schema}.{table.name}</span>
                </TableCell>
              </TableRow>
            {/each}
          </TableBody>
        </TableComponent>
      </div>
    </div>
  </div>
</div>
