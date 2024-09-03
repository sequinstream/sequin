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

  let form = { ...database };

  const isEdit = !!form.id;

  let databaseErrors: any = {};
  let replicationErrors: any = {};

  $: {
    databaseErrors = errors.database || {};
    replicationErrors = errors.replication || {};
  }

  function pushEvent(event: string, payload = {}, callback = () => {}) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
  }

  let dialogOpen = true;
  let showConfirmDialog = false;
  let validating = false;

  function handleSubmit(event: Event) {
    event.preventDefault();
    validating = true;
    pushEvent("form_submitted", { form }, () => {
      validating = false;
    });
  }

  $: pushEvent("form_updated", { form });

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
  <form on:submit={handleSubmit} class="space-y-6 max-w-3xl mx-auto mt-6">
    <Card>
      <CardHeader>
        <CardTitle>Database Configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="database">Database</Label>
          <Input type="text" id="database" bind:value={form.database} />
          {#if databaseErrors.database}
            <p class="text-destructive text-sm">{databaseErrors.database}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="hostname">Hostname</Label>
          <Input type="text" id="hostname" bind:value={form.hostname} />
          {#if databaseErrors.hostname}
            <p class="text-destructive text-sm">{databaseErrors.hostname}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="port">Port</Label>
          <Input
            type="number"
            id="port"
            bind:value={form.port}
            placeholder="5432"
          />
          {#if databaseErrors.port}
            <p class="text-destructive text-sm">{databaseErrors.port}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="username">Username</Label>
          <Input type="text" id="username" bind:value={form.username} />
          {#if databaseErrors.username}
            <p class="text-destructive text-sm">{databaseErrors.username}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="password">Password</Label>
          <Input type="password" id="password" bind:value={form.password} />
          {#if databaseErrors.password}
            <p class="text-destructive text-sm">{databaseErrors.password}</p>
          {/if}
        </div>

        <div class="flex items-center space-x-2">
          <Switch
            id="ssl"
            checked={form.ssl}
            onCheckedChange={(checked) => {
              form.ssl = checked;
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
        <h3 class="text-md font-semibold mb-2">
          Step 1: Create a Replication Slot
        </h3>
        <p class="text-sm text-muted-foreground">
          Run the following SQL command on your database to create a replication
          slot:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
select pg_create_logical_replication_slot('{form.slot_name ||
            "my_slot"}', 'pgoutput');</pre>

        <h3 class="text-md font-semibold mb-2">Step 2: Create a Publication</h3>
        <p class="text-sm text-muted-foreground">
          Choose from one of the following examples to create a publication:
        </p>
        <p class="text-sm font-medium">
          • Create a publication for all tables:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {form.publication_name || "my_pub"} for all tables;</pre>
        <p class="text-sm font-medium">
          • Create a publication for certain tables:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {form.publication_name ||
            "my_pub"} for table table1, table2, table3;</pre>
        <p class="text-sm font-medium">
          • Create a publication for all tables in a schema:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {form.publication_name ||
            "my_pub"} for tables in schema myschema;</pre>

        <h3 class="text-md font-semibold mb-2">
          Step 3: (Optional) Enable Full Replica Identity
        </h3>
        <p class="text-sm text-muted-foreground">
          If you want Sequin to capture `old` values for updates and deletes,
          run this command for each table in the publication:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
alter table {"{mytable}"} replica identity full;</pre>

        <div class="space-y-2">
          <Label for="slot_name">Slot Name</Label>
          <Input type="text" id="slot_name" bind:value={form.slot_name} />
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
            bind:value={form.publication_name}
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
            bind:value={form.name}
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
