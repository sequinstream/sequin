<script lang="ts">
  import { Card, CardContent } from "$lib/components/ui/card";
  import { Button } from "$lib/components/ui/button";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
  } from "$lib/components/ui/table";
  import { Database, ArrowUpRight, HelpCircle } from "lucide-svelte";
  import { getColorFromName } from "../utils";
  import LinkPushNavigate from "$lib/components/LinkPushNavigate.svelte";

  export let consumer;
</script>

<Card>
  <CardContent class="p-6 space-y-6">
    <div class="flex justify-between items-center">
      <h2 class="text-lg font-semibold">Sequence</h2>
      <LinkPushNavigate href="/databases/{consumer.postgres_database.id}">
        <Button variant="outline" size="sm">
          View Database
          <ArrowUpRight class="h-4 w-4 ml-2" />
        </Button>
      </LinkPushNavigate>
    </div>
    <div class="flex items-center space-x-2">
      <Database class="h-5 w-5 text-gray-400" />
      <pre class="font-medium">{consumer.postgres_database.name}</pre>
    </div>
    <div class="mb-4 flex items-center space-x-2">
      <icon
        class="hero-table-cells w-6 h-6 rounded {getColorFromName(
          `${consumer.sequence.table_schema}.${consumer.sequence.table_name}`
        )}"
      ></icon>

      <pre class="font-medium">{consumer.sequence.table_schema}.{consumer
          .sequence.table_name}</pre>
    </div>
    <div class="">
      <div class="flex items-center space-x-2">
        <h3 class="text-md font-semibold">Group Columns</h3>
        <Tooltip.Root openDelay={200}>
          <Tooltip.Trigger>
            <HelpCircle
              class="inline-block h-2.5 w-2.5 text-gray-400 -mt-2 cursor-help"
            />
          </Tooltip.Trigger>
          <Tooltip.Content class="max-w-xs space-y-2">
            <p class="text-xs text-gray-500">
              The columns of the sequence table used to group messages. Messages
              in a group are processed in FIFO order.
              <br />
              <br />
              By default, the primary key columns of the sequence table are used.
              <br />
              <br />
              See the
              <a
                class="text-blue-500 hover:underline"
                href="https://sequinstream.com/docs/how-sequin-works#grouping"
                >docs</a
              >
              for more information.
            </p>
          </Tooltip.Content>
        </Tooltip.Root>
      </div>
      <p class="font-medium mt-2">
        {consumer.group_column_names.join(", ")}
      </p>
    </div>
    <div class="mb-4">
      <h3 class="text-md font-semibold mb-2">Filters</h3>
      {#if consumer.sequence.column_filters.length > 0}
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>Column</TableHead>
              <TableHead>Operator</TableHead>
              <TableHead>Value</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {#each consumer.sequence.column_filters as filter}
              <TableRow>
                <TableCell
                  ><code>
                    {filter.column}
                    {#if filter.is_jsonb && filter.jsonb_path}
                      -> {filter.jsonb_path}
                    {/if}</code
                  ></TableCell
                >
                <TableCell><code>{filter.operator}</code></TableCell>
                <TableCell><code>{filter.value}</code></TableCell>
              </TableRow>
            {/each}
          </TableBody>
        </Table>
      {:else}
        <div
          class="bg-gray-50 border border-gray-200 rounded-lg p-6 text-center"
        >
          <h4 class="text-sm font-medium text-gray-900 mb-1">
            No filters applied
          </h4>
          <p class="text-sm text-gray-500 mb-4">
            This consumer will process all data from the source table.
          </p>
        </div>
      {/if}
    </div>
  </CardContent>
</Card>
