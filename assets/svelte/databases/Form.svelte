<script lang="ts">
  import { slide } from "svelte/transition";
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
  import CodeWithCopy from "../components/CodeWithCopy.svelte";
  import {
    Alert,
    AlertDescription,
    AlertTitle,
  } from "$lib/components/ui/alert";
  import { AlertCircle } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import { HelpCircle } from "lucide-svelte";
  import {
    Popover,
    PopoverContent,
    PopoverTrigger,
  } from "$lib/components/ui/popover";
  import { isValidPostgresURL, parsePostgresURL } from "./utils";
  import { Eye, EyeOff } from "lucide-svelte";
  import { Wand } from "lucide-svelte";
  import { ChevronDown, ChevronUp } from "lucide-svelte";

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
  export let showSupabasePoolerPrompt: boolean = false;

  let form = { ...database };

  const isEdit = !!form.id;

  let databaseErrors: any = {};
  let replicationErrors: any = {};

  $: {
    databaseErrors = errors.database || {};
    replicationErrors = errors.replication || {};

    // Automatically enable SSL for Supabase databases
    if (form.hostname && form.hostname.endsWith("supabase.co")) {
      form.ssl = true;
    }
  }

  function pushEvent(
    event: string,
    payload = {},
    callback: (reply?: any) => void = () => {}
  ) {
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

  function handleConvertSupabase() {
    pushEvent("convert_supabase_connection", { form }, (reply) => {
      if (reply && reply.converted) {
        form = { ...form, ...reply.converted, ssl: true };
        showSupabasePoolerPrompt = false;
      }
    });
  }

  let urlInput = "";
  let isValidURL = false;
  let popoverOpen = false;

  function handleURLInput() {
    isValidURL = isValidPostgresURL(urlInput);
  }

  function autofillFromURL() {
    if (isValidURL) {
      const { database, hostname, port, username, password, ssl } =
        parsePostgresURL(urlInput);
      form.database = database;
      form.hostname = hostname;
      form.port = port || 5432;
      form.username = username;
      form.password = password;
      form.ssl = ssl !== "disable";

      // Close the popover
      popoverOpen = false;
    }
  }

  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }

  let testDatabaseCardExpanded = false;

  function toggleTestDatabaseCard() {
    testDatabaseCardExpanded = !testDatabaseCardExpanded;
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
      <CardHeader class="flex flex-row justify-between items-center">
        <CardTitle>Database connection details</CardTitle>
        <Popover bind:open={popoverOpen}>
          <PopoverTrigger>
            <Button variant="outline">
              <Wand class="inline-block h-4 w-4 mr-2" /> Autofill with URL
            </Button>
          </PopoverTrigger>
          <PopoverContent class="min-w-96">
            <div class="space-y-2">
              <Label for="url-input">PostgreSQL URL</Label>
              <Input
                id="url-input"
                type="text"
                bind:value={urlInput}
                on:input={handleURLInput}
                placeholder="postgres://user:pass@host:port/db"
              />
              <Button on:click={autofillFromURL} disabled={!isValidURL}>
                Autofill
              </Button>
            </div>
          </PopoverContent>
        </Popover>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="database" class="flex items-center">
            Database
            <Tooltip.Root openDelay={300}>
              <Tooltip.Trigger>
                <HelpCircle
                  class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help"
                />
              </Tooltip.Trigger>
              <Tooltip.Content class="max-w-xs">
                <p class="text-xs text-gray-500">
                  <b>Database</b>
                  <br />
                  The name of the specific database within your PostgreSQL server
                  that you want to connect to. This is typically the logical database
                  name you created or were given access to.
                </p>
              </Tooltip.Content>
            </Tooltip.Root>
          </Label>
          <Input type="text" id="database" bind:value={form.database} />
          {#if databaseErrors.database}
            <p class="text-destructive text-sm">{databaseErrors.database}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="hostname">Host</Label>
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
          <div class="relative">
            <Input
              type={showPassword ? "text" : "password"}
              id="password"
              bind:value={form.password}
            />
            <button
              type="button"
              class="absolute inset-y-0 right-0 flex items-center pr-3"
              on:click={togglePasswordVisibility}
            >
              {#if showPassword}
                <EyeOff class="h-4 w-4 text-gray-400" />
              {:else}
                <Eye class="h-4 w-4 text-gray-400" />
              {/if}
            </button>
          </div>
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
        {#if showSupabasePoolerPrompt}
          <div transition:slide>
            <Alert variant="default">
              <AlertCircle class="h-4 w-4" />
              <AlertTitle>Supabase pooled connection detected</AlertTitle>
              <AlertDescription>
                We've detected a Supabase pooled connection. Sequin requires a
                direct connection. Click the button below to convert to a direct
                connection.
              </AlertDescription>
              <Button class="mt-2" on:click={handleConvertSupabase}>
                Convert to direct connection
              </Button>
            </Alert>
          </div>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <button
          type="button"
          class="flex justify-between items-center w-full"
          on:click={toggleTestDatabaseCard}
        >
          <CardTitle>Need a test database?</CardTitle>
          {#if testDatabaseCardExpanded}
            <ChevronUp class="h-4 w-4" />
          {:else}
            <ChevronDown class="h-4 w-4" />
          {/if}
        </button>
      </CardHeader>
      {#if testDatabaseCardExpanded}
        <CardContent class="space-y-4">
          <p class="text-sm text-muted-foreground">
            We recommend setting up a free database with Supabase to get
            started.
          </p>
          <Button
            variant="outline"
            href="https://supabase.com/dashboard"
            target="_blank"
            rel="noopener noreferrer"
          >
            Go to Supabase Dashboard
          </Button>
        </CardContent>
      {/if}
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Replication configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="slot_name">Slot name</Label>
          <Input type="text" id="slot_name" bind:value={form.slot_name} />
          {#if replicationErrors.slot_name}
            <p class="text-destructive text-sm">
              {replicationErrors.slot_name}
            </p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="publication_name">Publication name</Label>
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
        <h3 class="text-md font-semibold mb-2">
          Step 1: Create a replication slot
        </h3>
        <p class="text-sm text-muted-foreground">
          Run the following SQL command on your database to create a replication
          slot:
        </p>
        <CodeWithCopy
          language="sql"
          code={`select pg_create_logical_replication_slot('${form.slot_name || "my_slot"}', 'pgoutput');`}
        />

        <h3 class="text-md font-semibold mb-2">Step 2: Create a publication</h3>
        <p class="text-sm text-muted-foreground">
          Choose from one of the following examples to create a publication:
        </p>
        <p class="text-sm font-medium">
          • Create a publication for all tables:
        </p>
        <CodeWithCopy
          language="sql"
          code={`create publication ${form.publication_name || "my_pub"} for all tables;`}
        />

        <p class="text-sm font-medium">
          • Create a publication for certain tables:
        </p>
        <CodeWithCopy
          language="sql"
          code={`create publication ${form.publication_name || "my_pub"} for table table1, table2, table3;`}
        />

        <p class="text-sm font-medium">
          • Create a publication for all tables in a schema:
        </p>
        <CodeWithCopy
          language="sql"
          code={`create publication ${form.publication_name || "my_pub"} for tables in schema myschema;`}
        />
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Database configuration</CardTitle>
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
