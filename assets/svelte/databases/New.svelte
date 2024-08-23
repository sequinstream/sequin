<script lang="ts">
  import { createEventDispatcher } from "svelte";
  import FullPageModal from "../components/FullPageModal.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";

  export let formData: {
    database: {
      name: string;
      database: string;
      hostname: string;
      port: number;
      username: string;
      password: string;
      ssl: boolean;
    };
    replication: {
      publication_name: string;
      slot_name: string;
    };
  };
  export let formErrors: Record<string, any> = {};
  export let validating: boolean;
  export let parent: string;
  export let live;

  const dispatch = createEventDispatcher();

  let userInput = false;
  let dialogOpen = true;
  let showConfirmDialog = false;

  function handleInput() {
    userInput = true;
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    pushEvent("save", {
      postgres_database: formData.database,
      postgres_replication_slot: formData.replication,
    });
  }

  $: if (userInput) {
    pushEvent("validate", {
      postgres_database: formData.database,
      postgres_replication_slot: formData.replication,
    });
    userInput = false;
  }

  function getError(field: string): string | undefined {
    const [category, subfield] = field.split(".");
    if (formErrors[category] && formErrors[category][subfield]) {
      const error = formErrors[category][subfield];
      return Array.isArray(error) ? error.join(", ") : error;
    }
    return undefined;
  }

  function pushEvent(event: string, payload = {}) {
    live.pushEventTo(`#${parent}`, event, payload);
  }

  function handleClose() {
    pushEvent("form_closed");
  }
</script>

<FullPageModal
  title="Create Database"
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
            bind:value={formData.database.database}
            on:input={handleInput}
          />
          {#if getError("database.database")}
            <p class="text-destructive text-sm">
              {getError("database.database")}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="hostname">Hostname</Label>
          <Input
            type="text"
            id="hostname"
            bind:value={formData.database.hostname}
            on:input={handleInput}
          />
          {#if getError("database.hostname")}
            <p class="text-destructive text-sm">
              {getError("database.hostname")}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="port">Port</Label>
          <Input
            type="number"
            id="port"
            bind:value={formData.database.port}
            on:input={handleInput}
          />
          {#if getError("database.port")}
            <p class="text-destructive text-sm">{getError("database.port")}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="username">Username</Label>
          <Input
            type="text"
            id="username"
            bind:value={formData.database.username}
            on:input={handleInput}
          />
          {#if getError("database.username")}
            <p class="text-destructive text-sm">
              {getError("database.username")}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="password">Password</Label>
          <Input
            type="password"
            id="password"
            bind:value={formData.database.password}
            on:input={handleInput}
          />
          {#if getError("database.password")}
            <p class="text-destructive text-sm">
              {getError("database.password")}
            </p>
          {/if}
        </div>

        <div class="flex items-center space-x-2">
          <Switch
            id="ssl"
            checked={formData.database.ssl}
            onCheckedChange={(checked) => {
              formData.database.ssl = checked;
              handleInput();
            }}
          />
          <Label for="ssl">SSL</Label>
        </div>
        {#if getError("database.ssl")}
          <p class="text-destructive text-sm">{getError("database.ssl")}</p>
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
select pg_create_logical_replication_slot('{formData.replication.slot_name ||
            "my_slot"}', 'pgoutput');</pre>
        <p class="text-sm text-muted-foreground">
          Next, you need to create a publication. You have two options:
        </p>
        <p class="text-sm font-medium">
          1. Create a publication for all tables:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {formData.replication.publication_name ||
            "my_pub"} for all tables;</pre>
        <p class="text-sm font-medium">
          2. Create a publication for specific tables:
        </p>
        <pre class="bg-muted p-4 rounded-md mb-4 text-sm">
create publication {formData.replication.publication_name ||
            "my_pub"} for table table1, table2, table3;</pre>

        <div class="space-y-2">
          <Label for="slot_name">Slot Name</Label>
          <Input
            type="text"
            id="slot_name"
            bind:value={formData.replication.slot_name}
            on:input={handleInput}
          />
          {#if getError("replication.slot_name")}
            <p class="text-destructive text-sm">
              {getError("replication.slot_name")}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="publication_name">Publication Name</Label>
          <Input
            type="text"
            id="publication_name"
            bind:value={formData.replication.publication_name}
            on:input={handleInput}
          />
          {#if getError("replication.publication_name")}
            <p class="text-destructive text-sm">
              {getError("replication.publication_name")}
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
            bind:value={formData.database.name}
            on:input={handleInput}
            placeholder="Enter a unique name for your database"
            data-1p-ignore
            data-lpignore="true"
            data-form-type="other"
          />
          {#if getError("database.name")}
            <p class="text-destructive text-sm">{getError("database.name")}</p>
          {/if}
        </div>

        <Button type="submit" disabled={validating} variant="default">
          {#if validating}
            <span class="loading loading-spinner" />
            Validating...
          {:else}
            Create Database
          {/if}
        </Button>
      </CardContent>
    </Card>
  </form>
</FullPageModal>
