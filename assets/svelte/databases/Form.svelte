<script lang="ts">
  import { onMount } from "svelte";
  import { slide, fade } from "svelte/transition";
  import FullPageForm from "../components/FullPageForm.svelte";
  import { Button } from "$lib/components/ui/button";
  import { Input } from "$lib/components/ui/input";
  import { Label } from "$lib/components/ui/label";
  import { Switch } from "$lib/components/ui/switch";
  import { CircleX } from "lucide-svelte";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import {
    Accordion,
    AccordionContent,
    AccordionItem,
    AccordionTrigger,
  } from "$lib/components/ui/accordion";
  import CodeWithCopy from "../components/CodeWithCopy.svelte";
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
    pool_size: number;
    publication_name: string;
    slot_name: string;
    useLocalTunnel: boolean;
    is_replica: boolean;
    primary: {
      ssl: boolean;
      database: string;
      hostname: string;
      port: number;
      username: string;
      password: string;
    };
  };
  export let errors: Record<string, any> = {};
  export let submitError: string | null = null;
  export let parent: string;
  export let live;
  export let poolerType: "supabase" | "neon" | null = null;
  export let api_tokens: any[];
  export let showLocalTunnelPrompt: boolean = true;
  export let showPgVersionWarning: boolean = false;
  export let selfHosted: boolean = false;

  let form = { ...database };

  let lastPushedFormJSON = null;
  let isDirty = false;

  $: {
    isDirty = JSON.stringify(form) !== JSON.stringify(database);

    // Only push the form if it has changed since the last push
    // Prevents infinite loop of pushing the same form over and over
    if (JSON.stringify(form) !== lastPushedFormJSON) {
      pushEvent("form_updated", { form });
      lastPushedFormJSON = JSON.stringify(form);
    }
  }

  const isEdit = !!form.id;

  let databaseErrors: any = {};
  let replicationErrors: any = {};

  $: {
    databaseErrors = errors.database || {};
    replicationErrors = errors.replication || {};
  }

  $: {
    // Automatically enable SSL for Supabase databases
    if (form.hostname && form.hostname.endsWith("supabase.co")) {
      form.ssl = true;
    }

    if (
      form.hostname == "sequin_postgres" &&
      form.database == "sequin_playground"
    ) {
      form.ssl = false;
      form.name = "playground";
    }

    if (
      form.hostname === "localhost" ||
      form.hostname === "127.0.0.1" ||
      form.hostname === "host.docker.internal" ||
      form.hostname === "sequin_postgres"
    ) {
      form.ssl = false;
    }
  }

  const STORAGE_KEY = "sequin_database_form";
  const STORAGE_EXPIRY = 2 * 60 * 60 * 1000; // 2 hours in milliseconds

  let formLoadedFromLocalStorage = false;

  onMount(() => {
    const storedForm = localStorage.getItem(STORAGE_KEY);
    if (storedForm) {
      try {
        const parsedForm = JSON.parse(storedForm);
        const storedTimestamp = parsedForm._timestamp;

        delete parsedForm._timestamp;

        const currentTime = new Date().getTime();
        const isExpired = currentTime - storedTimestamp > STORAGE_EXPIRY;

        if (!isExpired) {
          form = { ...form, ...parsedForm };
        } else {
          localStorage.removeItem(STORAGE_KEY);
        }
      } catch (error) {
        console.error("Error parsing stored form:", error);
        localStorage.removeItem(STORAGE_KEY);
      }
    }

    formLoadedFromLocalStorage = true;
  });

  function saveFormToStorage() {
    if (!formLoadedFromLocalStorage) {
      return;
    }

    const formToSave = {
      ...form,
      _timestamp: new Date().getTime(),
    };
    localStorage.setItem(STORAGE_KEY, JSON.stringify(formToSave));
  }

  function clearFormStorage() {
    localStorage.removeItem(STORAGE_KEY);
  }

  function pushEvent(
    event: string,
    payload = {},
    callback: (reply?: any) => void = () => {},
  ) {
    live.pushEventTo(`#${parent}`, event, payload, callback);
    if (event === "form_updated") {
      saveFormToStorage();
    }
  }

  let validating = false;

  const progress = tweened(0, { duration: 10000, easing: cubicOut });

  $: if (validating) {
    progress.set(100);
  }

  function handleSubmit(event: Event) {
    event.preventDefault();
    validating = true;
    clearFormStorage();
    pushEvent("form_submitted", { form }, (reply) => {
      if (reply?.ok === false) {
        validating = false;
        progress.set(0);
        saveFormToStorage();
      }
    });
  }

  $: pushEvent("form_updated", { form });

  function handleClose() {
    clearFormStorage();
    pushEvent("form_closed");
  }

  function handleConvertPool() {
    pushEvent("convert_pooled_connection", { form }, (reply) => {
      if (reply && reply.converted) {
        form = { ...form, ...reply.converted, ssl: true };
        poolerType = null;
      }
    });
  }

  type TestConnectionReply =
    | { ok: true; error: undefined }
    | { ok: false; error: string };

  type TestConnectionState =
    | { displayStatus?: boolean; status: "initial" }
    | { displayStatus?: boolean; status: "loading" }
    | { displayStatus?: boolean; status: "success" }
    | { displayStatus?: boolean; status: "error"; error: string };

  let testConnectionState: TestConnectionState = { status: "initial" };

  function onTestConnection() {
    testConnectionState = { status: "loading" };

    pushEvent("test_connection", { form }, (reply: TestConnectionReply) => {
      if (reply.ok) {
        testConnectionState = { displayStatus: true, status: "success" };
        setTimeout(() => {
          testConnectionState = { status: "initial" };
        }, 3000);
      } else {
        testConnectionState = {
          displayStatus: true,
          status: "error",
          error: reply.error,
        };
        setTimeout(() => {
          testConnectionState.displayStatus = false;
        }, 3000);
      }
    });
  }

  function resetTestConnection() {
    testConnectionState = { status: "initial" };
  }

  let urlInput = "";
  let urlInputError;
  let popoverOpen = false;

  function autofillFromURL() {
    if (isValidPostgresURL(urlInput)) {
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
    } else {
      urlInputError = "Invalid Postgres URL";
    }
  }

  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }

  function toggleLocalTunnel() {
    form.useLocalTunnel = !form.useLocalTunnel;
    pushEvent("form_updated", { form });
  }

  $: setupTunnelCode = `# if it's your first time using the Sequin CLI
sequin context add default --api-token={{secret}} --set-default

# every time you want to boot the tunnel
# Replace [your-local-port] with the local port you want Sequin to connect to
sequin tunnel --ports=[your-local-port]:${form.name}`;

  function clearForm() {
    form = {
      id: form.id, // Preserve the id if it exists
      name: "",
      database: "",
      hostname: "",
      port: 5432,
      username: "",
      password: "",
      ssl: true,
      pool_size: 10,
      publication_name: database.publication_name,
      slot_name: database.slot_name,
      useLocalTunnel: false,
    };
    clearFormStorage();
    pushEvent("form_updated", { form });
  }

  function getPoolerName(type: string): string {
    switch (type) {
      case "supabase":
        return "Supabase";
      case "neon":
        return "Neon";
    }
  }
</script>

<FullPageForm
  title={isEdit ? `Edit database ${form.name}` : "Connect Database"}
  showConfirmOnExit={isDirty}
  on:close={handleClose}
>
  <form on:submit={handleSubmit} class="space-y-6 max-w-3xl mx-auto">
    <Card>
      <CardHeader class="flex flex-row items-center">
        <CardTitle class="flex-grow">Database connection details</CardTitle>
      </CardHeader>
      <CardContent class="space-y-4">
        <div class="flex items-center space-x-2 mb-2">
          <Popover bind:open={popoverOpen}>
            <PopoverTrigger asChild let:builder>
              <Button variant="magic" builders={[builder]}>
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
                  on:input={() => (urlInputError = null)}
                  placeholder="postgres://user:pass@host:port/db"
                />
                <Button on:click={autofillFromURL} disabled={!urlInput}>
                  Autofill
                </Button>
                {#if urlInputError}
                  <p class="text-destructive text-sm">{urlInputError}</p>
                {/if}
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
              {#if showLocalTunnelPrompt}
                <Switch
                  id="use-localhost"
                  checked={form.useLocalTunnel}
                  onCheckedChange={toggleLocalTunnel}
                  disabled={isEdit}
                />
                <Label for="use-localhost">Use localhost</Label>

                <Tooltip.Root openDelay={200}>
                  <Tooltip.Trigger>
                    <Info class="h-4 w-4 text-gray-400 cursor-help" />
                  </Tooltip.Trigger>
                  <Tooltip.Content class="p-4 max-w-xs">
                    <p class="text-sm text-muted-foreground font-normal">
                      You can use the Sequin CLI to connect Sequin to a database
                      running on your local machine.
                    </p>
                  </Tooltip.Content>
                </Tooltip.Root>
              {/if}
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

        <div class="space-y-2">
          <Label for="pool_size" class="flex items-center">
            Pool Size
            <Tooltip.Root openDelay={200}>
              <Tooltip.Trigger>
                <HelpCircle
                  class="inline-block h-4 w-4 text-gray-400 ml-1 cursor-help"
                />
              </Tooltip.Trigger>
              <Tooltip.Content class="max-w-xs">
                <p class="text-xs text-gray-500">
                  <b>Pool Size</b>
                  <br />
                  The maximum number of concurrent connections that Sequin will maintain
                  to your PostgreSQL database, aside from the replication slot connection.
                  Higher values allow for better throughput but consume more database
                  resources.
                </p>
              </Tooltip.Content>
            </Tooltip.Root>
          </Label>
          <Input
            type="number"
            id="pool_size"
            bind:value={form.pool_size}
            placeholder="10"
          />
          {#if databaseErrors.pool_size}
            <p class="text-destructive text-sm">
              {databaseErrors.pool_size}
            </p>
          {/if}
        </div>

        <div class="flex items-center gap-2">
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

        <div class="flex items-center gap-2">
          <Switch id="is_replica" bind:checked={form.is_replica} />
          <Label for="is_replica" class="flex items-center">
            Replica
            <Tooltip.Root openDelay={200}>
              <Tooltip.Trigger>
                <HelpCircle class="inline-block h-4 w-4 text-gray-400 ml-1" />
              </Tooltip.Trigger>
              <Tooltip.Content class="max-w-xs">
                <p class="text-sm text-gray-500">
                  <b>Replica</b>
                  <br />
                  When connecting a replica to Sequin, Sequin also needs to connect
                  to the primary database.
                  <a
                    href="https://docs.sequinstream.com/reference/databases#using-sequin-with-a-replica"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="inline-flex items-center text-link hover:underline"
                  >
                    Learn more
                    <ExternalLinkIcon class="w-3 h-3 ml-1" />
                  </a>
                </p>
              </Tooltip.Content>
            </Tooltip.Root>
          </Label>
        </div>

        {#if form.is_replica}
          <div transition:slide class="space-y-4 mt-2 bg-muted p-4 rounded-md">
            <div class="space-y-4">
              <div class="space-y-2">
                <Label for="primary_hostname">Primary host</Label>
                <Input
                  type="text"
                  id="primary_hostname"
                  placeholder="example.com"
                  bind:value={form.primary.hostname}
                />
                {#if databaseErrors.primary?.hostname}
                  <p class="text-destructive text-sm">
                    {databaseErrors.primary?.hostname}
                  </p>
                {/if}
              </div>

              <div class="space-y-2">
                <Label for="primary_port">Primary port</Label>
                <Input
                  type="number"
                  id="primary_port"
                  placeholder="5432"
                  bind:value={form.primary.port}
                />
                {#if databaseErrors.primary?.port}
                  <p class="text-destructive text-sm">
                    {databaseErrors.primary?.port}
                  </p>
                {/if}
              </div>

              <div class="space-y-2">
                <Label for="primary_database">Primary database</Label>
                <Input
                  type="text"
                  id="primary_database"
                  placeholder="postgres"
                  bind:value={form.primary.database}
                />
                {#if databaseErrors.primary?.database}
                  <p class="text-destructive text-sm">
                    {databaseErrors.primary?.database}
                  </p>
                {/if}
              </div>

              <div class="space-y-2">
                <Label for="primary_username">Primary username</Label>
                <Input
                  type="text"
                  id="primary_username"
                  bind:value={form.primary.username}
                />
                {#if databaseErrors.primary?.username}
                  <p class="text-destructive text-sm">
                    {databaseErrors.primary?.username}
                  </p>
                {/if}
              </div>

              <div class="space-y-2">
                <Label for="primary_password">Primary password</Label>
                <div class="relative">
                  <Input
                    type={showPassword ? "text" : "password"}
                    id="primary_password"
                    bind:value={form.primary.password}
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
                {#if databaseErrors.primary?.password}
                  <p class="text-destructive text-sm">
                    {databaseErrors.primary?.password}
                  </p>
                {/if}
              </div>

              <div class="flex items-center gap-2">
                <Switch id="primary_ssl" bind:checked={form.primary.ssl} />
                <Label for="primary_ssl">Primary SSL</Label>
              </div>
            </div>
          </div>
        {/if}

        {#if poolerType}
          <div transition:slide>
            <Alert variant="default">
              <AlertCircle class="h-4 w-4" />
              <AlertTitle>Pooled connection detected</AlertTitle>
              <AlertDescription>
                We've detected a {getPoolerName(poolerType)} pooled connection. Sequin
                requires a direct connection. Click the button below to convert to
                a direct connection.
              </AlertDescription>
              <Button class="mt-2" on:click={handleConvertPool}>
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
                apiTokens={api_tokens}
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
      <CardContent>
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

        <div class="space-y-2">
          <div class="space-y-2">
            <Label for="slot_name">Slot name</Label>
            <Input type="text" id="slot_name" bind:value={form.slot_name} />
            {#if replicationErrors.slot_name}
              <p class="text-destructive text-sm">
                {replicationErrors.slot_name}
              </p>
            {/if}
          </div>
        </div>

        <div class="mt-8">
          <h3 class="text-md font-semibold">Step 1: Create a publication</h3>
          <p class="text-sm text-muted-foreground">
            Create a publication in your database using <strong>one</strong> of the
            following commands:
          </p>
        </div>
        <Accordion class="pl-6 space-y-2" value="all_tables">
          <AccordionItem value="all_tables">
            <AccordionTrigger class="text-sm font-medium">
              Create a publication for all tables
            </AccordionTrigger>
            <AccordionContent>
              <CodeWithCopy
                language="sql"
                code={`create publication ${form.publication_name || "my_pub"} for all tables with (publish_via_partition_root = true);`}
              />
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="certain_tables">
            <AccordionTrigger class="text-sm font-medium">
              OR create a publication for certain tables
            </AccordionTrigger>
            <AccordionContent>
              <CodeWithCopy
                language="sql"
                code={`create publication ${form.publication_name || "my_pub"} for table table1, table2, table3 with (publish_via_partition_root = true);`}
              />
            </AccordionContent>
          </AccordionItem>
          <AccordionItem value="all_tables_in_schema">
            <AccordionTrigger class="text-sm font-medium">
              OR create a publication for all tables in a schema
            </AccordionTrigger>
            <AccordionContent>
              <CodeWithCopy
                language="sql"
                code={`create publication ${form.publication_name || "my_pub"} for tables in schema myschema with (publish_via_partition_root = true);`}
              />
            </AccordionContent>
          </AccordionItem>
        </Accordion>
        <div class="mt-8 mb-6">
          <h3 class="text-md font-semibold">
            Step 2: Create a replication slot
          </h3>
          <p class="text-sm text-muted-foreground">
            Create a replication slot in your database using the following
            command:
          </p>
        </div>
        <div class="pl-6 space-y-2">
          <CodeWithCopy
            language="sql"
            code={`select pg_create_logical_replication_slot('${form.slot_name || "my_slot"}', 'pgoutput');`}
          />
        </div>
      </CardContent>
    </Card>

    {#if !selfHosted}
      <Card>
        <CardHeader>
          <CardTitle>IP addresses</CardTitle>
          <p class="text-sm text-muted-foreground">
            Ensure your database and/or firewall allows connections from the
            following IPs:
          </p>
        </CardHeader>
        <CardContent>
          <div class="pl-6 flex flex-row space-x-2">
            <CodeWithCopy
              maxWidth="200px"
              language="bash"
              code={`3.221.69.77`}
            />
            <CodeWithCopy
              maxWidth="200px"
              language="bash"
              code={`54.210.150.114`}
            />
          </div>
        </CardContent>
      </Card>
    {/if}

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
          {#if databaseErrors.name}
            <p class="text-destructive text-sm">{databaseErrors.name}</p>
          {/if}
        </div>

        {#if submitError}
          <p class="text-destructive text-sm">{submitError}</p>
        {:else if Object.keys(errors).length > 0}
          <p class="text-destructive text-sm">Validation errors, see above</p>
        {/if}

        <div class="flex justify-between items-center gap-2">
          <Button type="button" variant="outline" on:click={clearForm}>
            Clear Form
          </Button>
          <div class="flex gap-2">
            <Button
              loading={testConnectionState.status === "loading"}
              type="button"
              variant="outline"
              class="self-end"
              on:click={onTestConnection}
            >
              {#if testConnectionState.status === "success" && testConnectionState.displayStatus}
                <span
                  class="flex items-center p-1 gap-1 mr-2 bg-green-500 rounded-full"
                ></span>
                Connection succeeded
              {:else if testConnectionState.status === "error" && testConnectionState.displayStatus}
                <span
                  class="flex items-center p-1 gap-1 mr-2 bg-red-500 rounded-full"
                ></span>
                Connection failed
              {:else}
                Test Connection
              {/if}
            </Button>
            <Button type="submit" loading={validating} variant="default">
              <span slot="loading"> Validating... </span>
              {#if isEdit}
                Update Database
              {:else}
                Connect Database
              {/if}
            </Button>
          </div>
        </div>

        {#if validating}
          <Progress class="mt-4" value={$progress} />
        {/if}

        {#if showPgVersionWarning}
          <Alert variant="destructive">
            <AlertCircle class="h-4 w-4" />
            <AlertTitle>Unsupported PostgreSQL version</AlertTitle>
            <AlertDescription>
              Sequin fully supports PostgreSQL 14+. PostgreSQL 12 and 13 are
              supported with <a
                href="https://docs.sequinstream.com/reference/databases#postgresql-12-and-13"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center text-link hover:underline"
              >
                some limitations.
              </a>
            </AlertDescription>
          </Alert>
        {/if}

        {#if testConnectionState.status === "error"}
          <div
            class="flex items-center justify-end gap-2"
            transition:fade={{ duration: 200 }}
          >
            <p class="text-destructive text-sm">
              Test connection failed: {testConnectionState.error}
            </p>
            <Button variant="ghost" size="icon" on:click={resetTestConnection}>
              <CircleX class="h-4 w-4 text-gray-500" />
            </Button>
          </div>
        {/if}
      </CardContent>
    </Card>
  </form>
</FullPageForm>
