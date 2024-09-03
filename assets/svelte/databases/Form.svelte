<script lang="ts">
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { LoaderCircle } from "lucide-svelte";

  export let database: {
    id?: string;
    name: string;
    database: string;
    hostname: string;
    port: number;
    username: string;
    password: string;
    ssl: boolean;
    publication_name: string;
    slot_name: string;
  };
  export let errors: Record<string, any> = {};
  export let submitError: string | null = null;
  export let parent: string;
  export let live;

  const isEdit = !!database.id;

  let databaseErrors: any = {};
  let replicationErrors: any = {};

  $: {
    databaseErrors = errors.database || {};
    replicationErrors = errors.replication || {};
  }

  function pushEvent(event: string, payload = {}, callback = () => {}) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  let userInput = false;
  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;

  function handleInput() {
    userInput = true;
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    validating = true;
    pushEvent("form_submitted", { form: database }, () => {
      validating = false;
    });
  }

  $: if (userInput) {
    pushEvent("form_updated", { form: database });
    userInput = false;
  }

  function handleClose() {
    pushEvent("form_closed");
  }
</script>

<FullPageModal
  title="Connect Database"
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-3xl mx-auto">
    <Card>
      <CardHeader>
        <CardTitle>Database Configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="database">Database</Label>
          <Input
            type="text"
            id="database"
            bind:value={database.database}
            on:input={handleInput}
          />
          {#if databaseErrors.database}
            <p class="text-destructive text-sm">{databaseErrors.database}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="hostname">Hostname</Label>
          <Input
            type="text"
            id="hostname"
            bind:value={database.hostname}
            on:input={handleInput}
          />
          {#if databaseErrors.hostname}
            <p class="text-destructive text-sm">{databaseErrors.hostname}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="port">Port</Label>
          <Input
            type="number"
            id="port"
            bind:value={database.port}
            on:input={handleInput}
            placeholder="5432"
          />
          {#if databaseErrors.port}
            <p class="text-destructive text-sm">{databaseErrors.port}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="username">Username</Label>
          <Input
            type="text"
            id="username"
            bind:value={database.username}
            on:input={handleInput}
          />
          {#if databaseErrors.username}
            <p class="text-destructive text-sm">{databaseErrors.username}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="password">Password</Label>
          <Input
            type="password"
            id="password"
            bind:value={database.password}
            on:input={handleInput}
          />
          {#if databaseErrors.password}
            <p class="text-destructive text-sm">{databaseErrors.password}</p>
          {/if}
        </div>

        <div class="flex items-center space-x-2">
          <Switch
            id="ssl"
            checked={database.ssl}
            onCheckedChange={(checked) => {
              database.ssl = checked;
              handleInput();
            }}
          />
          <Label for="ssl">SSL</Label>
        </div>
        {#if databaseErrors.ssl}
          <p class="text-destructive text-sm">{databaseErrors.ssl}</p>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Replication Configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <p class="text-sm text-muted-foreground">
          To set up replication, you need to create a replication slot. Run the
          following SQL command on your database:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
select pg_create_logical_replication_slot('{database.slot_name ||
            "my_slot"}', 'pgoutput');</pre>
        <p class="text-sm text-muted-foreground">
          Next, you need to create a publication. You have two options:
        </p>
        <p class="text-sm font-medium">
          1. Create a publication for all tables:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {database.publication_name || "my_pub"} for all tables;</pre>
        <p class="text-sm font-medium">
          2. Create a publication for specific tables:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {database.publication_name ||
            "my_pub"} for table table1, table2, table3;</pre>

        <div class="space-y-2">
          <Label for="slot_name">Slot Name</Label>
          <Input
            type="text"
            id="slot_name"
            bind:value={database.slot_name}
            on:input={handleInput}
          />
          {#if replicationErrors.slot_name}
            <p class="text-destructive text-sm">
              {replicationErrors.slot_name}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="publication_name">Publication Name</Label>
          <Input
            type="text"
            id="publication_name"
            bind:value={database.publication_name}
            on:input={handleInput}
          />
          {#if replicationErrors.publication_name}
            <p class="text-destructive text-sm">
              {replicationErrors.publication_name}
            </p>
          {/if}
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Database</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="name">Name</Label>
          <Input
            type="text"
            id="name"
            bind:value={database.name}
            on:input={handleInput}
            placeholder="Enter a unique name for your database"
            data-1p-ignore
            data-lpignore="true"
            data-form-type="other"
          />
          {#if errors.name}
            <p class="text-destructive text-sm">{errors.name}</p>
          {/if}
        </div>

        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {:else if Object.keys(errors).length > 0}
          <p class="text-destructive text-sm">Validation errors, see above</p>
        {/if}

        <Button type="submit" loading={validating} variant="default">
          {#if validating}
            Validating...
          {:else if isEdit}
            Update Database
          {:else}
            Connect Database
          {/if}
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
