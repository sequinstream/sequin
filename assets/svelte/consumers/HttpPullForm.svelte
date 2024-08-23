<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
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
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import TableSelector from "../components/TableSelector.svelte";
  import TableFilters from "../components/TableFilters.svelte";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";

  export let live;
  export let parent;
  export let http_pull_consumer;
  export let databases;
  export let errors: any = {};
  export let submitError;

  let initialForm = {
    messageKind: http_pull_consumer.message_kind || "record",
    postgresDatabaseId: http_pull_consumer.postgres_database_id,
    tableOid: http_pull_consumer.table_oid,
    sourceTableFilters: http_pull_consumer.source_table_filters || [],
    sourceTableActions: http_pull_consumer.source_table_actions || [],
    name: http_pull_consumer.name || "",
    ackWaitMs: http_pull_consumer.ack_wait_ms || 30000,
    maxAckPending: http_pull_consumer.max_ack_pending || 10000,
    maxWaiting: http_pull_consumer.max_waiting || 20,
  };

  let form = { ...initialForm };
  let isDirty = false;

  $: {
    isDirty = JSON.stringify(form) !== JSON.stringify(initialForm);
    pushEvent("form_updated", { form });
  }

  const pushEvent = (event, payload = {}, cb = () => {}) => {
    return live.pushEventTo("#" + parent, event, payload, cb);
  };

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

  let dialogOpen = true;
  let showConfirmDialog = false;

  function handleConsumerSubmit() {
    pushEvent("form_submitted", { form });
  }

  function handleTableSelect(event: { databaseId: string; tableOid: number }) {
    form.postgresDatabaseId = event.databaseId;
    form.tableOid = event.tableOid;
  }

  function handleFilterChange(newFilters) {
    form.sourceTableFilters = newFilters;
  }

  function handleClose() {
    pushEvent("form_closed");
  }

  $: isCreateConsumerDisabled = !form.postgresDatabaseId || !form.tableOid;
</script>

<FullPageModal
  title="Create an HTTP pull consumer"
  bind:open={dialogOpen}
  bind:showConfirmDialog
  showConfirmOnExit={isDirty}
  on:close={handleClose}
>
  <form
    on:submit|preventDefault={handleConsumerSubmit}
    class="space-y-6 max-w-3xl mx-auto"
  >
    <Card>
      <CardHeader>
        <CardTitle>Source</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="message-kind">Stream type</Label>
          <Select
            selected={{
              value: form.messageKind,
              label:
                form.messageKind === "event" ? "Change stream" : "Sync stream",
            }}
            onSelectedChange={(event) => (form.messageKind = event.value)}
          >
            <SelectTrigger class="w-full">
              <SelectValue placeholder="Select stream type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem class="cursor-pointer" value="event"
                >Change stream</SelectItem
              >
              <SelectItem class="cursor-pointer" value="record"
                >Sync stream</SelectItem
              >
            </SelectContent>
          </Select>
          <p class="text-sm text-muted-foreground">
            {#if form.messageKind === "event"}
              A change stream will send you every create, update, and delete
              that happens to rows and includes both new and old values.
            {:else if form.messageKind === "record"}
              A sync stream will send you the latest version of rows whenever
              they change and only includes new values, no deletes.
            {/if}
          </p>
          {#if errors.message_kind}
            <p class="text-destructive text-sm">
              {errors.message_kind}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label>Source table</Label>
          <TableSelector
            {pushEvent}
            {databases}
            selectedDatabaseId={form.postgresDatabaseId}
            selectedTableOid={form.tableOid}
            onSelect={handleTableSelect}
          />
          {#if errors.postgres_database_id || errors.table_oid}
            <p class="text-destructive text-sm">
              {errors.postgres_database_id || errors.table_oid}
            </p>
          {/if}
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Filters</CardTitle>
      </CardHeader>
      <CardContent>
        {#if form.messageKind === "event"}
          {@const actions = form.sourceTableActions || []}
          {@const switches = [
            { id: "insert", label: "Insert" },
            { id: "update", label: "Update" },
            { id: "delete", label: "Delete" },
          ]}
          <div class="space-y-2 mb-6">
            <Label>Operations to capture</Label>
            <div class="flex items-center space-x-4">
              {#each switches as { id, label }}
                <div class="flex items-center space-x-2">
                  <Label for={id} class="cursor-pointer">
                    {label}
                  </Label>
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
            {#if errors.source_table?.actions}
              <p class="text-destructive text-sm">
                {errors.source_table?.actions}
              </p>
            {/if}
          </div>
        {/if}
        <div class="my-3">
          <TableFilters
            filters={form.sourceTableFilters}
            columns={selectedTable ? selectedTable.columns : []}
            onFilterChange={handleFilterChange}
            disabled={!form.postgresDatabaseId && !form.tableOid}
            errors={errors.source_tables?.[0]?.column_filters}
          />
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Pull configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="visibility-timeout">Visibility timeout</Label>
          <div class="flex items-center space-x-2">
            <Input
              id="visibility-timeout"
              type="number"
              bind:value={form.ackWaitMs}
              class="w-24"
            />
            <span class="text-sm text-muted-foreground">ms</span>
          </div>
          <p class="text-sm text-muted-foreground">
            The duration that a message is hidden from other consumers after
            it's been received. This prevents multiple consumers from processing
            the same message simultaneously. If the message isn't processed
            within this time, it becomes visible to other consumers again.
          </p>
          {#if errors.ack_wait_ms}
            <p class="text-destructive text-sm">{errors.ack_wait_ms}</p>
          {/if}
        </div>

        <Accordion class="w-full">
          <AccordionItem value="advanced">
            <AccordionTrigger>Advanced configuration</AccordionTrigger>
            <AccordionContent>
              <div class="space-y-4 pt-4">
                <div class="space-y-2">
                  <Label for="max-ack-pending">Max ack pending</Label>
                  <Input
                    id="max-ack-pending"
                    type="number"
                    bind:value={form.maxAckPending}
                  />
                  <p class="text-sm text-muted-foreground">
                    Sets the maximum number of messages that can be pending
                    acknowledgment at any time. This helps control the flow of
                    messages and prevents overwhelming the consumer.
                  </p>
                  {#if errors.max_ack_pending}
                    <p class="text-destructive text-sm">
                      {errors.max_ack_pending}
                    </p>
                  {/if}
                </div>
                <div class="space-y-2">
                  <Label for="max-waiting">Max waiting</Label>
                  <Input
                    id="max-waiting"
                    type="number"
                    bind:value={form.maxWaiting}
                  />
                  <p class="text-sm text-muted-foreground">
                    Determines the maximum number of waiting pull requests. This
                    setting helps manage resource utilization and prevents
                    excessive polling.
                  </p>
                  {#if errors.max_waiting}
                    <p class="text-destructive text-sm">
                      {errors.max_waiting}
                    </p>
                  {/if}
                </div>
              </div>
            </AccordionContent>
          </AccordionItem>
        </Accordion>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Consumer</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="consumer-name">Name</Label>
          <Input
            id="consumer-name"
            bind:value={form.name}
            placeholder="Enter a unique name for your consumer"
            data-1p-ignore
            data-lpignore="true"
            data-form-type="other"
          />
          {#if errors.name}
            <p class="text-destructive text-sm">{errors.name}</p>
          {/if}
        </div>

        <Button type="submit" disabled={isCreateConsumerDisabled}
          >Create Consumer</Button
        >
        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {/if}
      </CardContent>
    </Card>
  </form>
</FullPageModal>
