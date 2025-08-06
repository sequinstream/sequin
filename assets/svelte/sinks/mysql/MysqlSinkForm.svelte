<script lang="ts">
  import { Input } from "$lib/components/ui/input";
  import * as Alert from "$lib/components/ui/alert";
  import {
    Card,
    CardContent,
    CardHeader,
    CardTitle,
  } from "$lib/components/ui/card";
  import { Label } from "$lib/components/ui/label";
  import { Eye, EyeOff } from "lucide-svelte";
  import { Switch } from "$lib/components/ui/switch";
  import DynamicRoutingForm from "$lib/consumers/DynamicRoutingForm.svelte";
  import type { MysqlConsumer } from "../../consumers/types";

  export let form: MysqlConsumer;
  export let functions: Array<any> = [];
  export let refreshFunctions: () => void;
  export let functionRefreshState: "idle" | "refreshing" | "done" = "idle";
  let isDynamicRouting = form.sink.routing_mode === "dynamic";
  export let errors: any = {};
  let showPassword = false;

  function togglePasswordVisibility() {
    showPassword = !showPassword;
  }
</script>

<Card>
  <CardHeader>
    <CardTitle>MySQL configuration</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <Alert.Root variant="info">
      <Alert.Title>Transform requirements</Alert.Title>
      <Alert.Description>
        <p class="mb-2">
          Your <a
            class="underline font-medium"
            href="https://sequinstream.com/docs/reference/transforms"
            target="_blank">transform</a
          >
          must return data matching your MySQL table schema.
        </p>
        <p class="mb-2">
          Ensure your transform returns key-value pairs where keys match your
          table's column names and values are appropriately typed.
        </p>
      </Alert.Description>
    </Alert.Root>

    <div class="grid grid-cols-1 gap-4 md:grid-cols-2">
      <div class="space-y-2">
        <Label for="host">Host</Label>
        <Input id="host" bind:value={form.sink.host} placeholder="localhost" />
        {#if errors.sink?.host}
          <p class="text-destructive text-sm">{errors.sink.host}</p>
        {/if}
        <p class="text-sm text-muted-foreground">
          MySQL server hostname or IP address
        </p>
      </div>

      <div class="space-y-2">
        <Label for="port">Port</Label>
        <Input
          id="port"
          type="number"
          bind:value={form.sink.port}
          placeholder="3306"
          min="1"
          max="65535"
        />
        {#if errors.sink?.port}
          <p class="text-destructive text-sm">{errors.sink.port}</p>
        {/if}
        <p class="text-sm text-muted-foreground">
          MySQL server port (default: 3306)
        </p>
      </div>
    </div>

    <div class="space-y-2">
      <Label for="database">Database</Label>
      <Input
        id="database"
        bind:value={form.sink.database}
        placeholder="my_database"
      />
      {#if errors.sink?.database}
        <p class="text-destructive text-sm">{errors.sink.database}</p>
      {/if}
      <p class="text-sm text-muted-foreground">
        Name of the target MySQL database
      </p>
    </div>

    <div class="grid grid-cols-1 gap-4 md:grid-cols-2">
      <div class="space-y-2">
        <Label for="username">Username</Label>
        <Input
          id="username"
          bind:value={form.sink.username}
          placeholder="mysql_user"
          autocomplete="username"
        />
        {#if errors.sink?.username}
          <p class="text-destructive text-sm">{errors.sink.username}</p>
        {/if}
      </div>

      <div class="space-y-2">
        <Label for="password">Password</Label>
        <div class="relative">
          <Input
            id="password"
            type={showPassword ? "text" : "password"}
            bind:value={form.sink.password}
            placeholder="••••••••"
            data-1p-ignore
            autocomplete="off"
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
        {#if errors.sink?.password}
          <p class="text-destructive text-sm">{errors.sink.password}</p>
        {/if}
      </div>
    </div>

    <div class="space-y-2">
      <div class="flex items-center gap-2">
        <Switch
          id="ssl"
          checked={form.sink.ssl}
          onCheckedChange={(checked) => {
            form.sink.ssl = checked;
          }}
        />
        <Label for="ssl">SSL</Label>
      </div>
      {#if errors.sink?.ssl}
        <p class="text-destructive text-sm">{errors.sink.ssl}</p>
      {/if}
    </div>

    <div class="space-y-2">
      <div class="flex items-center gap-2">
        <Switch
          id="upsert_on_duplicate"
          checked={form.sink.upsert_on_duplicate}
          onCheckedChange={(checked) => {
            form.sink.upsert_on_duplicate = checked;
          }}
        />
        <Label for="upsert_on_duplicate">Use upsert on duplicate keys</Label>
      </div>
      {#if errors.sink?.upsert_on_duplicate}
        <p class="text-destructive text-sm">
          {errors.sink.upsert_on_duplicate}
        </p>
      {/if}
      <p class="text-sm text-muted-foreground">
        When enabled, uses MySQL's ON DUPLICATE KEY UPDATE to handle existing
        records. When disabled, attempts to insert records directly (may fail on
        duplicates).
      </p>
    </div>

    <div class="grid grid-cols-1 gap-4 md:grid-cols-2">
      <div class="space-y-2">
        <Label for="batch_size">Batch size</Label>
        <Input
          id="batch_size"
          type="number"
          bind:value={form.sink.batch_size}
          placeholder="100"
          min="1"
          max="10000"
        />
        {#if errors.sink?.batch_size}
          <p class="text-destructive text-sm">{errors.sink.batch_size}</p>
        {/if}
        <p class="text-sm text-muted-foreground">
          Number of records to process in each batch (1-10,000)
        </p>
      </div>

      <div class="space-y-2">
        <Label for="timeout_seconds">Timeout (seconds)</Label>
        <Input
          id="timeout_seconds"
          type="number"
          bind:value={form.sink.timeout_seconds}
          placeholder="30"
          min="1"
          max="300"
        />
        {#if errors.sink?.timeout_seconds}
          <p class="text-destructive text-sm">{errors.sink.timeout_seconds}</p>
        {/if}
        <p class="text-sm text-muted-foreground">
          Connection timeout in seconds (1-300)
        </p>
      </div>
    </div>
  </CardContent>
</Card>

<Card>
  <CardHeader>
    <CardTitle>Routing</CardTitle>
  </CardHeader>
  <CardContent class="space-y-4">
    <DynamicRoutingForm
      bind:form
      routedSinkType="mysql"
      {functions}
      {refreshFunctions}
      bind:functionRefreshState
      bind:selectedDynamic={isDynamicRouting}
      {errors}
    />

    {#if !isDynamicRouting}
      <div class="space-y-2">
        <Label for="table_name">Table name</Label>
        <Input
          id="table_name"
          name="sink[table_name]"
          bind:value={form.sink.table_name}
          placeholder="my_table"
        />
        {#if errors.sink?.table_name}
          <p class="text-destructive text-sm">{errors.sink.table_name}</p>
        {/if}
        <p class="text-sm text-muted-foreground">
          Name of the target MySQL table. Must be a valid MySQL identifier.
        </p>
      </div>
    {/if}
  </CardContent>
</Card>
