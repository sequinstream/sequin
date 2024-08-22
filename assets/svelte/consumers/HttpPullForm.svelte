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
  import {
    PlusCircle,
    InfoIcon,
    RefreshCwIcon,
    CheckIcon,
  } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import TableSelector from "../components/TableSelector.svelte";
  import TableFilters from "../components/TableFilters.svelte";
  import * as Drawer from "$lib/components/ui/drawer";

  export let live;
  export let parent;
  export let http_pull_consumer;
  export let databases;
  export let errors: any = {};
  export let submitError;

  let form = {
    messageKind: http_pull_consumer.message_kind || "record",
    postgresDatabaseId: http_pull_consumer.postgres_database_id,
    tableOid: http_pull_consumer.table_oid,
    sourceTableFilters: http_pull_consumer.source_table_filters || [],
    name: http_pull_consumer.name || "",
    ackWaitMs: http_pull_consumer.ack_wait_ms || 30000,
    maxAckPending: http_pull_consumer.max_ack_pending || 10000,
    maxWaiting: http_pull_consumer.max_waiting || 20,
  };

  const pushEvent = (event, payload = {}, cb = () => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

  $: pushEvent("form_updated", { form });

  const messageKinds = [
    { id: "record", title: "Record" },
    { id: "event", title: "Event" },
  ];

  let selectedDatabase: any;
  let selectedTable: any;

  $: {
    if (form.postgresDatabaseId && form.tableOid) {
      selectedDatabase = databases.find(
        (db) => db.id === form.postgresDatabaseId
      );
      if (selectedDatabase) {
        selectedTable = selectedDatabase.tables.find(
          (table) => table.oid === form.tableOid
        );
      }
    }
  }

  let tableRefreshState: "idle" | "refreshing" | "done" = "idle";

  function refreshTables(databaseId: string) {
    if (databaseId) {
      tableRefreshState = "refreshing";
      pushEvent("refresh_tables", { database_id: databaseId }, () => {
        tableRefreshState = "done";
        setTimeout(() => {
          tableRefreshState = "idle";
        }, 2000);
      });
    }
  }

  function handleConsumerSubmit() {
    pushEvent("form_submitted", { form });
  }

  let activeInfo = "";

  const infoContent = {
    name: [
      "Choose a unique name for your consumer.",
      "You'll use this name in HTTP requests to Sequin.",
    ],
    ackWaitMs: [
      "The visibility timeout is the duration that a message is hidden from other consumers after it's been received.",
      "This prevents multiple consumers from processing the same message simultaneously.",
      "If the message isn't processed within this time, it becomes visible to other consumers again.",
    ],
    maxAckPending: [
      "Max ack pending sets the maximum number of messages that can be pending acknowledgment at any time.",
      "This helps control the flow of messages and prevents overwhelming the consumer.",
    ],
    maxWaiting: [
      "Max waiting determines the maximum number of waiting pull requests.",
      "This setting helps manage resource utilization and prevents excessive polling.",
    ],
  };

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
    pushEvent("form_updated", { consumer: form });
  }
</script>

<Drawer.Root open={true} onClose={() => pushEvent("form_closed")}>
  <Drawer.Content>
    <div class="p-8 max-w-5xl mx-auto">
      <div class="flex gap-6">
        <div class="w-full max-w-3xl">
          <h2 class="text-2xl font-bold mb-2">Create an HTTP pull consumer</h2>
          <p class="text-muted-foreground mb-6">
            Configure your consumer settings
          </p>
          <form
            on:submit|preventDefault={handleConsumerSubmit}
            class="space-y-6"
          >
            <div class="space-y-2">
              <Label
                for="name"
                class="flex items-center gap-2"
                data-1p-ignore
                data-lpignore="true"
                data-form-type="other"
              >
                Name
                <button
                  type="button"
                  on:click={() => (activeInfo = "name")}
                  class="text-muted-foreground"
                >
                  <InfoIcon class="h-4 w-4" />
                </button>
              </Label>
              <Input
                id="name"
                bind:value={form.name}
                placeholder="Enter consumer name"
                on:focus={() => (activeInfo = "name")}
              />
              {#if errors.name}
                <p class="text-red-500 text-sm">{errors.name}</p>
              {/if}
            </div>

            <div class="space-y-2">
              <Label for="message-kind">Message Kind</Label>
              <Select onSelectedChange={(value) => (form.messageKind = value)}>
                <SelectTrigger class="w-[180px]">
                  <SelectValue placeholder="Select message kind" />
                </SelectTrigger>
                <SelectContent>
                  {#each messageKinds as kind}
                    <SelectItem value={kind.id}>{kind.title}</SelectItem>
                  {/each}
                </SelectContent>
              </Select>
              {#if errors.message_kind}
                <p class="text-red-500 text-sm">{errors.message_kind}</p>
              {/if}
            </div>

            <div class="space-y-2">
              <Label>Source Table</Label>
              <TableSelector
                {pushEvent}
                {databases}
                selectedDatabaseId={form.postgresDatabaseId}
                selectedTableOid={form.tableOid}
                onSelect={handleTableSelect}
              />
              {#if errors.postgres_database_id || errors.table_oid}
                <p class="text-red-500 text-sm">
                  {errors.postgres_database_id || errors.table_oid}
                </p>
              {/if}
            </div>

            {#if form.postgresDatabaseId && form.tableOid}
              {#if selectedDatabase && selectedTable}
                <TableFilters
                  filters={form.sourceTableFilters}
                  columns={selectedTable.columns}
                  onFilterChange={handleFilterChange}
                />
              {:else}
                <p>Loading table information...</p>
              {/if}
            {:else}
              <p>Please select a database and table first.</p>
            {/if}

            <div class="space-y-2">
              <Label for="visibility-timeout" class="flex items-center gap-2">
                Visibility timeout
                <button
                  type="button"
                  on:click={() => (activeInfo = "ackWaitMs")}
                  class="text-muted-foreground"
                >
                  <InfoIcon class="h-4 w-4" />
                </button>
              </Label>
              <div class="flex items-center gap-2">
                <Input
                  id="visibility-timeout"
                  type="number"
                  bind:value={form.ackWaitMs}
                  class="w-24"
                  on:focus={() => (activeInfo = "ackWaitMs")}
                />
                <Select>
                  <SelectTrigger class="w-[180px]">
                    <SelectValue placeholder="Select unit" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="seconds">Seconds</SelectItem>
                    <SelectItem value="minutes">Minutes</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              {#if errors.ack_wait_ms}
                <p class="text-red-500 text-sm">{errors.ack_wait_ms}</p>
              {/if}
            </div>

            <Accordion class="w-full">
              <AccordionItem value="advanced">
                <AccordionTrigger>Advanced configuration</AccordionTrigger>
                <AccordionContent>
                  <div class="space-y-4 pt-4">
                    <div class="space-y-2">
                      <Label
                        for="max-ack-pending"
                        class="flex items-center gap-2"
                      >
                        Max ack pending
                        <button
                          type="button"
                          on:click={() => (activeInfo = "maxAckPending")}
                          class="text-muted-foreground"
                        >
                          <InfoIcon class="h-4 w-4" />
                        </button>
                      </Label>
                      <Input
                        id="max-ack-pending"
                        type="number"
                        bind:value={form.maxAckPending}
                        on:focus={() => (activeInfo = "maxAckPending")}
                      />
                      {#if errors.max_ack_pending}
                        <p class="text-red-500 text-sm">
                          {errors.max_ack_pending}
                        </p>
                      {/if}
                    </div>
                    <div class="space-y-2">
                      <Label for="max-waiting" class="flex items-center gap-2">
                        Max waiting
                        <button
                          type="button"
                          on:click={() => (activeInfo = "maxWaiting")}
                          class="text-muted-foreground"
                        >
                          <InfoIcon class="h-4 w-4" />
                        </button>
                      </Label>
                      <Input
                        id="max-waiting"
                        type="number"
                        bind:value={form.maxWaiting}
                        on:focus={() => (activeInfo = "maxWaiting")}
                      />
                      {#if errors.max_waiting}
                        <p class="text-red-500 text-sm">{errors.max_waiting}</p>
                      {/if}
                    </div>
                  </div>
                </AccordionContent>
              </AccordionItem>
            </Accordion>

            <Button type="submit">Create Consumer</Button>
            {#if submitError}
              <p class="text-red-500 text-sm mt-2">{submitError}</p>
            {/if}
          </form>
        </div>

        <Card class="w-80 h-fit sticky top-4">
          <CardHeader>
            <CardTitle>Information</CardTitle>
          </CardHeader>
          <CardContent>
            {#if activeInfo}
              {#each infoContent[activeInfo] as infoParagraph}
                <p class="text-sm text-muted-foreground mb-2">
                  {infoParagraph}
                </p>
              {/each}
            {:else}
              <p class="text-sm text-muted-foreground">
                Select a field or click an info icon to see more details.
              </p>
            {/if}
          </CardContent>
        </Card>
      </div>
    </div>
  </Drawer.Content>
</Drawer.Root>
