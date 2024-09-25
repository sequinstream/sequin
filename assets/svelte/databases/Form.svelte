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
  import CopyIcon from "../components/CopyIcon.svelte";
  import {
    Alert,
    AlertDescription,
    AlertTitle,
  } from "$lib/components/ui/alert";
  import {
    AlertCircle,
    Eye,
    EyeOff,
    HelpCircle,
    Wand,
    Zap,
    ExternalLinkIcon,
  } from "lucide-svelte";
  import * as Tooltip from "$lib/components/ui/tooltip";
  import {
    Popover,
    PopoverContent,
    PopoverTrigger,
  } from "$lib/components/ui/popover";
  import { isValidPostgresURL, parsePostgresURL } from "./utils";
  import { Progress } from "$lib/components/ui/progress";
  import { tweened } from "svelte/motion";
  import { cubicOut } from "svelte/easing";
  import { Info } from "lucide-svelte";
  import CodeWithSecret from "$lib/components/CodeWithSecret.svelte";

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
    useLocalTunnel: boolean;
  };
  export let errors: Record<string, any> = {};
  export let submitError: string | null = null;
  export let parent: string;
  export let live;
  export let showSupabasePoolerPrompt: boolean = false;
  export let api_token: { name: string; token: string };

  let form = { ...database, ssl: true }; // Set default SSL to true

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

  const progress = tweened(0, { duration: 10000, easing: cubicOut });

  $: if (validating) {
    progress.set(100);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    validating = true;
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok !== true) {
        validating = false;
        progress.set(0);
      }
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
      // Do not autofill the password if Supabase placeholder value
      form.password = password === "[YOUR-PASSWORD]" ? "" : password;
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

  function toggleLocalTunnel() {
    form.useLocalTunnel = !form.useLocalTunnel;
    pushEvent("form_updated", { form });
  }

  $: setupTunnelCode = `# if it's your first time using the Sequin CLI
sequin context add default --api-token={{secret}} --set-default

# every time you want to boot the tunnel
# Replace [your-local-port] with the local port you want Sequin to connect to
sequin tunnel --ports=[your-local-port]:${form.name}`;
</script>

<FullPageModal
  title="Connect Database"
  bind:open={dialogOpen}
  bind:showConfirmDialog
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-3xl mx-auto mt-6">
    <Card>
      <CardHeader class="flex flex-row items-center">
        <CardTitle class="flex-grow">Database connection details</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="flex items-center space-x-2 mb-2">
          <Popover bind:open={popoverOpen}>
            <PopoverTrigger>
              <Button variant="magic">
                <Wand class="inline-block h-4 w-4 mr-2" /> Autofill with Connection
                String
              </Button>
            </PopoverTrigger>
            <PopoverContent class="min-w-96 -mt-4">
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
          <Popover>
            <PopoverTrigger>
              <Button variant="secondary">
                <Zap class="inline-block h-4 w-4 mr-2" /> Try with test database?
              </Button>
            </PopoverTrigger>
            <PopoverContent class="w-80">
              <div class="space-y-2">
                <h4 class="font-medium">Need a test database?</h4>
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
              </div>
            </PopoverContent>
          </Popover>
        </div>
        <div class="space-y-2">
          <div
            class="grid grid-cols-[1fr_auto] items-center align-middle gap-2"
          >
            <Label for="hostname">Host</Label>
            <div class="flex items-center space-x-2">
              <Switch
                id="use-localhost"
                checked={form.useLocalTunnel}
                onCheckedChange={toggleLocalTunnel}
                disabled={isEdit}
              />
              <Label for="use-localhost">Use localhost</Label>
              <Popover>
                <PopoverTrigger>
                  <Info class="w-4 h-4 text-muted-foreground" />
                </PopoverTrigger>
                <PopoverContent>
                  You can use the Sequin CLI to connect Sequin to a database
                  running on your local machine.
                </PopoverContent>
              </Popover>
            </div>
          </div>
          <Input
            type="text"
            id="hostname"
            bind:value={form.hostname}
            disabled={form.useLocalTunnel}
            placeholder={form.useLocalTunnel ? "Managed by Sequin" : ""}
          />
          {#if databaseErrors.hostname}
            <p class="text-destructive text-sm">{databaseErrors.hostname}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="port">Port</Label>
          {#if form.useLocalTunnel}
            <Input
              type="number"
              id="port"
              placeholder="Managed by Sequin"
              disabled
            />
          {:else}
            <Input
              type="number"
              id="port"
              bind:value={form.port}
              placeholder="5432"
            />
          {/if}
          {#if databaseErrors.port}
            <p class="text-destructive text-sm">{databaseErrors.port}</p>
          {/if}
        </div>

        <div class="space-y-2">
          <Label for="database" class="flex items-center">
            Database
            <Tooltip.Root openDelay={200}>
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
          <Input
            type="text"
            id="database"
            placeholder="postgres"
            bind:value={form.database}
          />
          {#if databaseErrors.database}
            <p class="text-destructive text-sm">{databaseErrors.database}</p>
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
          {#if form.useLocalTunnel}
            <Switch id="ssl" checked={false} disabled />
          {:else}
            <Switch
              id="ssl"
              checked={form.ssl}
              onCheckedChange={(checked) => {
                form.ssl = checked;
              }}
            />
          {/if}
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
        {#if form.useLocalTunnel}
          <div class="text-sm p-4 text-muted-foreground bg-muted rounded-md">
            <p class="mb-4">
              Install the Sequin CLI and boot up a tunnel now. See setup
              instructions <a
                href="https://sequinstream.com/docs/cli"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center text-link hover:underline"
              >
                in the Sequin docs
                <ExternalLinkIcon class="w-3 h-3 ml-1" />
              </a>
            </p>
            <div>
              <CodeWithSecret
                preClass="text-xs bg-muted"
                containerClass="bg-muted"
                tabs={[
                  {
                    name: "Connect",
                    value: setupTunnelCode,
                  },
                ]}
                secret={api_token.token}
              />
            </div>
          </div>
        {/if}
      </CardContent>
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
        <h3 class="text-md font-semibold mb-2">Step 1: Create a publication</h3>
        <div class="pl-6 space-y-2">
          <p class="text-sm text-muted-foreground">
            Choose from one of the following examples to create a publication:
          </p>
          <p class="text-sm font-medium">
            Create a publication for all tables:
          </p>
          <CodeWithCopy
            language="sql"
            code={`create publication ${form.publication_name || "my_pub"} for all tables;`}
          />

          <p class="text-sm font-medium">
            Create a publication for certain tables:
          </p>
          <CodeWithCopy
            language="sql"
            code={`create publication ${form.publication_name || "my_pub"} for table table1, table2, table3;`}
          />

          <p class="text-sm font-medium">
            Create a publication for all tables in a schema:
          </p>
          <CodeWithCopy
            language="sql"
            code={`create publication ${form.publication_name || "my_pub"} for tables in schema myschema;`}
          />
        </div>
        <h3 class="text-md font-semibold mb-2">
          Step 2: Create a replication slot
        </h3>
        <div class="pl-6 space-y-2">
          <p class="text-sm text-muted-foreground">
            Run the following command on your database to create a replication
            slot:
          </p>
          <CodeWithCopy
            language="sql"
            code={`select pg_create_logical_replication_slot('${form.slot_name || "my_slot"}', 'pgoutput');`}
          />
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>IP addresses</CardTitle>
      </CardHeader>
      <CardContent>
        <p class="text-sm text-muted-foreground mb-2">
          Ensure your database and/or firewall allows connections from the
          following IPs:
        </p>
        <ul class="list-disc list-inside space-y-2 text-sm">
          <li>
            <div class="inline-flex items-center">
              <span class="mr-2">3.221.69.77</span>
              <CopyIcon class="h-4 w-4" content="3.221.69.77" />
            </div>
          </li>
          <li>
            <div class="inline-flex items-center">
              <span class="mr-2">54.210.150.114</span>
              <CopyIcon class="h-4 w-4" content="54.210.150.114" />
            </div>
          </li>
        </ul>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Database configuration</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="space-y-2">
          <Label for="name">Name</Label>
          <div class="flex flex-col gap-1">
            <Input
              type="text"
              id="name"
              bind:value={form.name}
              placeholder="Enter a unique name for your database"
              data-1p-ignore
              data-lpignore="true"
              data-form-type="other"
            />
            <p class="text-xs font-light">
              Must contain only alphanumeric characters, underscores, hyphens,
              or dots.
            </p>
          </div>
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
          <span slot="loading"> Validating... </span>
          {#if isEdit}
            Update Database
          {:else}
            Connect Database
          {/if}
        </Button>

        {#if validating}
          <Progress class="mt-4" value={$progress} />
        {/if}
      </CardContent>
    </Card>
  </form>
</FullPageModal>
